package main

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
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
	nicks := make([]string, 0, len(hub.nickMap))
	for nick := range hub.nickMap {
		nicks = append(nicks, nick)
	}
	hub.mu.RUnlock()

	msg := Message{
		Type:      "online",
		Content:   strings.Join(nicks, ","),
		Timestamp: time.Now().Unix(),
	}

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
	if msg.Type == "system" || msg.Type == "online" {
		return
	}
	_, err := db.Exec(
		"INSERT INTO messages (type, sender, receiver, content, timestamp) VALUES (?, ?, ?, ?, ?)",
		msg.Type, msg.From, msg.To, msg.Content, msg.Timestamp,
	)
	if err != nil {
		log.Printf("Save error: %v", err)
	}
}

func getHistory(nick string, limit int) []Message {
	rows, err := db.Query(
		`SELECT type, sender, receiver, content, timestamp FROM messages 
		 WHERE type='public' 
		 OR (type='private' AND (sender=? OR receiver=?)) 
		 ORDER BY timestamp DESC LIMIT ?`,
		nick, nick, limit,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var msg Message
		rows.Scan(&msg.Type, &msg.From, &msg.To, &msg.Content, &msg.Timestamp)
		msgs = append([]Message{msg}, msgs...)
	}
	return msgs
}

func (c *Client) readPump(ctx context.Context) {
	defer func() {
		hub.unregister <- c
		c.conn.Close(websocket.StatusNormalClosure, "")
	}()

	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		msg.Timestamp = time.Now().Unix()
		msg.From = c.nick

		switch msg.Type {
		case "nick":
			newNick := strings.TrimSpace(msg.Content)
			if newNick == "" {
				continue
			}
			oldNick := c.nick

			hub.mu.Lock()
			existing, taken := hub.nickMap[newNick]
			if taken && existing != c {
				hub.mu.Unlock()
				errMsg := Message{
					Type:      "system",
					Content:   "昵称 [" + newNick + "] 已被占用",
					Timestamp: time.Now().Unix(),
				}
				select {
				case c.send <- errMsg:
				default:
				}
				continue
			}
			if oldNick != "" {
				delete(hub.nickMap, oldNick)
			}
			c.nick = newNick
			hub.nickMap[c.nick] = c
			hub.mu.Unlock()

			go func(nick string) {
				history := getHistory(nick, 60)
				for _, m := range history {
					select {
					case c.send <- m:
					default:
					}
				}
			}(c.nick)

			broadcastOnline()

			if oldNick != "" {
				hub.broadcast <- Message{
					Type:    "system",
					Content: oldNick + " 改名为 " + newNick,
					Timestamp: time.Now().Unix(),
				}
			}

		case "public":
			if c.nick != "" && msg.Content != "" {
				hub.broadcast <- msg
			}

		case "private":
			if c.nick == "" || msg.Content == "" || msg.To == "" {
				continue
			}
			hub.mu.RLock()
			target, exists := hub.nickMap[msg.To]
			hub.mu.RUnlock()

			if exists {
				select {
				case target.send <- msg:
				default:
				}
				if target != c {
					select {
					case c.send <- msg:
					default:
					}
				}
				saveMessage(msg)
			} else {
				select {
				case c.send <- Message{Type: "system", Content: "用户 " + msg.To + " 不在线"}:
				default:
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
			if !ok {
				return
			}
			data, _ := json.Marshal(msg)
			if err := c.conn.Write(ctx, websocket.MessageText, data); err != nil {
				return
			}
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func serveWs(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie("auth")
	if err != nil || cookie.Value != accessPassword {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	client := &Client{conn: conn, send: make(chan Message, 32), cancel: cancel}
	hub.register <- client
	go client.writePump(ctx)
	client.readPump(ctx)
}

func main() {
	accessPassword = os.Getenv("CHAT_PASSWORD")
	if accessPassword == "" { accessPassword = "changeme" }
	serverPort = os.Getenv("PORT")
	if serverPort == "" { serverPort = "10699" }

	var err error
	db, err = sql.Open("sqlite3", "./chat.db?_cache=shared&_journal_mode=WAL")
	if err != nil { log.Fatal(err) }
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.Exec(`CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, sender TEXT, receiver TEXT, content TEXT, timestamp INTEGER)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_time ON messages(timestamp)`)

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
				http.SetCookie(w, &http.Cookie{Name: "auth", Value: accessPassword, Path: "/", MaxAge: 86400 * 7, HttpOnly: true})
				http.Redirect(w, r, "/", http.StatusSeeOther)
				return
			}
			http.Redirect(w, r, "/login?error=1", http.StatusSeeOther)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(loginHTML))
	})

	http.HandleFunc("/ws", serveWs)
	log.Printf("Starting on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}

const loginHTML = `<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Login</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#667eea}form{background:#fff;padding:30px;border-radius:10px}input{display:block;width:100%;margin:10px 0;padding:10px}button{width:100%;padding:10px;background:#667eea;color:#fff;border:none;border-radius:5px}</style></head><body><form method="POST"><input type="password" name="password" placeholder="Password" required><button type="submit">Enter</button></form></body></html>`
