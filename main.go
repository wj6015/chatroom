package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed static/index.html
var indexHTML []byte

type Message struct {
	Type      string `json:"type"`
	From      string `json:"from"`
	To        string `json:"to"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
	Password  string `json:"password,omitempty"` // 仅登录时使用
}

type Client struct {
	conn   *websocket.Conn
	nick   string
	send   chan Message
	cancel context.CancelFunc
}

type Hub struct {
	clients    map[*Client]bool
	nickMap    map[string]*Client
	broadcast  chan Message
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

var (
	hub            *Hub
	db             *sql.DB
	accessPassword string
	serverPort     string
)

func initHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		nickMap:    make(map[string]*Client),
		broadcast:  make(chan Message, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

func hashPwd(pwd string) string {
	h := sha256.New()
	h.Write([]byte(pwd))
	return hex.EncodeToString(h.Sum(nil))
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				if client.nick != "" {
					delete(h.nickMap, client.nick)
				}
				close(client.send)
				client.cancel()
			}
			h.mu.Unlock()
			broadcastOnline()
		case msg := <-h.broadcast:
			if msg.Type == "public" {
				saveMessage(msg)
			}
			h.mu.RLock()
			for client := range h.clients {
				select {
				case client.send <- msg:
				default:
				}
			}
			h.mu.RUnlock()
		}
	}
}

func broadcastOnline() {
	hub.mu.RLock()
	var nicks []string
	for nick := range hub.nickMap {
		nicks = append(nicks, nick)
	}
	hub.mu.RUnlock()
	msg := Message{Type: "online", Content: strings.Join(nicks, ","), Timestamp: time.Now().Unix()}
	hub.mu.RLock()
	for client := range hub.clients {
		select {
		case client.send <- msg:
		default:
		}
	}
	hub.mu.RUnlock()
}

func saveMessage(msg Message) {
	if msg.Type == "system" || msg.Type == "online" { return }
	db.Exec("INSERT INTO messages (type, sender, receiver, content, timestamp) VALUES (?, ?, ?, ?, ?)",
		msg.Type, msg.From, msg.To, msg.Content, msg.Timestamp)
}

func (c *Client) readPump(ctx context.Context) {
	defer func() {
		hub.unregister <- c
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil { return }
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil { continue }
		msg.Timestamp = time.Now().Unix()

		switch msg.Type {
		case "nick":
			nick := strings.TrimSpace(msg.From)
			pwd := hashPwd(msg.Password)
			if len(nick) < 2 { continue }

			hub.mu.Lock()
			// 1. 检查是否有人正在使用该昵称
			if _, online := hub.nickMap[nick]; online {
				hub.mu.Unlock()
				c.send <- Message{Type: "system", Content: "昵称已被占用，请重新更换昵称"}
				continue
			}

			// 2. 数据库校验/注册
			var storedPwd string
			err := db.QueryRow("SELECT password FROM users WHERE username = ?", nick).Scan(&storedPwd)
			if err == sql.ErrNoRows {
				// 新用户注册
				db.Exec("INSERT INTO users (username, password) VALUES (?, ?)", nick, pwd)
			} else if err == nil && storedPwd != pwd {
				// 密码错误，按要求提示“被占用”
				hub.mu.Unlock()
				c.send <- Message{Type: "system", Content: "昵称已被占用，请重新更换昵称"}
				continue
			}

			c.nick = nick
			hub.nickMap[nick] = c
			hub.mu.Unlock()

			// 发送历史记录
			rows, _ := db.Query(`SELECT type, sender, receiver, content, timestamp FROM messages 
				WHERE type='public' OR (type='private' AND (sender=? OR receiver=?)) 
				ORDER BY timestamp DESC LIMIT 60`, nick, nick)
			var msgs []Message
			for rows.Next() {
				var m Message
				rows.Scan(&m.Type, &m.From, &m.To, &m.Content, &m.Timestamp)
				msgs = append([]Message{m}, msgs...)
			}
			rows.Close()
			for _, m := range msgs { c.send <- m }
			broadcastOnline()

		case "public", "private":
			if c.nick == "" { continue }
			msg.From = c.nick
			if msg.Type == "public" {
				hub.broadcast <- msg
			} else {
				hub.mu.RLock()
				target, exists := hub.nickMap[msg.To]
				hub.mu.RUnlock()
				if exists {
					target.send <- msg
					if target != c { c.send <- msg }
					saveMessage(msg)
				}
			}
		}
	}
}

func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			if !ok { return }
			data, _ := json.Marshal(msg)
			c.conn.Write(ctx, websocket.MessageText, data)
		case <-ticker.C:
			if c.conn.Ping(ctx) != nil { return }
		case <-ctx.Done():
			return
		}
	}
}

func main() {
	accessPassword = os.Getenv("CHAT_PASSWORD")
	if accessPassword == "" { accessPassword = "changeme" }
	serverPort = os.Getenv("PORT")
	if serverPort == "" { serverPort = "10699" }

	db, _ = sql.Open("sqlite3", "./chat.db?_journal_mode=WAL")
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.Exec(`CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT)`)
	db.Exec(`CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, sender TEXT, receiver TEXT, content TEXT, timestamp INTEGER)`)
	
	hub = initHub()
	go hub.run()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("auth")
		if err != nil || cookie.Value != accessPassword {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			if r.FormValue("password") == accessPassword {
				http.SetCookie(w, &http.Cookie{Name: "auth", Value: accessPassword, Path: "/", MaxAge: 86400*7, HttpOnly: true})
				http.Redirect(w, r, "/", http.StatusSeeOther)
				return
			}
			http.Redirect(w, r, "/login?error=1", http.StatusSeeOther)
			return
		}
		w.Write([]byte(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Login</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#1a1d24;color:#fff}form{background:#2a2d36;padding:30px;border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,0.5)}input{display:block;width:100%;margin:15px 0;padding:12px;border-radius:8px;border:none}button{width:100%;padding:12px;background:#6c63ff;color:#fff;border:none;border-radius:8px;cursor:pointer}</style></head><body><form method="POST"><h2>Chatroom Login</h2><input type="password" name="password" placeholder="Password" required autofocus><button type="submit">Enter</button></form></body></html>`))
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		cookie, _ := r.Cookie("auth")
		if cookie == nil || cookie.Value != accessPassword { return }
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		ctx, cancel := context.WithCancel(r.Context())
		client := &Client{conn: conn, send: make(chan Message, 32), cancel: cancel}
		hub.register <- client
		go client.writePump(ctx)
		client.readPump(ctx)
	})

	log.Printf("Running on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}
