package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sort"
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
	Password  string `json:"password,omitempty"`
	UserList  []UserStatus `json:"user_list,omitempty"`
}

type UserStatus struct {
	Name   string `json:"name"`
	Online bool   `json:"online"`
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
			broadcastUserList()
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

// 核心改动：广播全量用户列表及其状态
func broadcastUserList() {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	rows, err := db.Query("SELECT username FROM users")
	if err != nil { return }
	defer rows.Close()

	var list []UserStatus
	for rows.Next() {
		var name string
		rows.Scan(&name)
		_, isOnline := hub.nickMap[name]
		list = append(list, UserStatus{Name: name, Online: isOnline})
	}

	// 按在线状态和字母排序
	sort.Slice(list, func(i, j int) bool {
		if list[i].Online != list[j].Online {
			return list[i].Online
		}
		return list[i].Name < list[j].Name
	})

	msg := Message{Type: "online", UserList: list, Timestamp: time.Now().Unix()}
	for client := range hub.clients {
		select {
		case client.send <- msg:
		default:
		}
	}
}

func saveMessage(msg Message) {
	if msg.Type == "system" || msg.Type == "online" { return }
	_, err := db.Exec("INSERT INTO messages (type, sender, receiver, content, timestamp) VALUES (?, ?, ?, ?, ?)",
		msg.Type, msg.From, msg.To, msg.Content, msg.Timestamp)
	if err != nil { log.Println("DB Error:", err) }
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
			if _, online := hub.nickMap[nick]; online {
				hub.mu.Unlock()
				c.send <- Message{Type: "system", Content: "此账号已在别处登录"}
				continue
			}

			var storedPwd string
			err := db.QueryRow("SELECT password FROM users WHERE username = ?", nick).Scan(&storedPwd)
			if err == sql.ErrNoRows {
				db.Exec("INSERT INTO users (username, password) VALUES (?, ?)", nick, pwd)
			} else if err == nil && storedPwd != pwd {
				hub.mu.Unlock()
				c.send <- Message{Type: "system", Content: "密码错误或昵称被占用"}
				continue
			}

			c.nick = nick
			hub.nickMap[nick] = c
			hub.mu.Unlock()

			// 拉取历史记录
			rows, _ := db.Query(`SELECT type, sender, receiver, content, timestamp FROM messages 
				WHERE type='public' OR (type='private' AND (sender=? OR receiver=?)) 
				ORDER BY timestamp DESC LIMIT 80`, nick, nick)
			var msgs []Message
			for rows.Next() {
				var m Message
				rows.Scan(&m.Type, &m.From, &m.To, &m.Content, &m.Timestamp)
				msgs = append([]Message{m}, msgs...)
			}
			rows.Close()
			for _, m := range msgs { c.send <- m }
			broadcastUserList()

		case "public":
			if c.nick == "" { continue }
			msg.From = c.nick
			hub.broadcast <- msg

		case "private":
			if c.nick == "" { continue }
			msg.From = c.nick
			saveMessage(msg)
			c.send <- msg
			hub.mu.RLock()
			target, exists := hub.nickMap[msg.To]
			hub.mu.RUnlock()
			if exists && target != c {
				target.send <- msg
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
		w.Write([]byte(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Login</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#1a1d24;color:#fff}form{background:#2a2d36;padding:30px;border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,0.5)}input{display:block;width:100%;margin:15px 0;padding:12px;border-radius:8px;border:none}button{width:100%;padding:12px;background:#6c63ff;color:#fff;border:none;border-radius:8px;cursor:pointer}</style></head><body><form method="POST"><h2>宇宙公司聊天室</h2><button type="submit">Enter</button><input type="password" name="password" placeholder="请输入聊天室密码" required autofocus></form></body></html>`))
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		cookie, _ := r.Cookie("auth")
		if cookie == nil || cookie.Value != accessPassword { return }
		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		ctx, cancel := context.WithCancel(r.Context())
		client := &Client{conn: conn, send: make(chan Message, 64), cancel: cancel}
		hub.register <- client
		go client.writePump(ctx)
		client.readPump(ctx)
	})

	log.Printf("Running on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}
