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
		broadcast:  make(chan Message, 100),
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
			if client.nick != "" {
				h.nickMap[client.nick] = client
			}
			h.mu.Unlock()
			broadcastOnline()

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
			saveMessage(msg)
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

func getHistory(limit int) []Message {
	rows, err := db.Query(
		"SELECT type, sender, receiver, content, timestamp FROM messages ORDER BY timestamp DESC LIMIT ?",
		limit,
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
			oldNick := c.nick
			hub.mu.Lock()
			if oldNick != "" {
				delete(hub.nickMap, oldNick)
			}
			c.nick = msg.Content
			hub.nickMap[c.nick] = c
			hub.mu.Unlock()
			broadcastOnline()

			sysMsg := Message{
				Type:      "system",
				Content:   oldNick + " 改名为 " + msg.Content,
				Timestamp: time.Now().Unix(),
			}
			hub.broadcast <- sysMsg

		case "public":
			if msg.Content != "" {
				hub.broadcast <- msg
			}

		case "private":
			hub.mu.RLock()
			target, exists := hub.nickMap[msg.To]
			hub.mu.RUnlock()
			if exists {
				select {
				case target.send <- msg:
				case c.send <- msg:
				}
				saveMessage(msg)
			} else {
				errMsg := Message{
					Type:      "system",
					Content:   "用户 " + msg.To + " 不在线",
					Timestamp: time.Now().Unix(),
				}
				c.send <- errMsg
			}
		}
	}
}

func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			data, _ := json.Marshal(msg)
			c.conn.Write(ctx, websocket.MessageText, data)

		case <-ticker.C:
			ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
			c.conn.Ping(ctx2)
			cancel()

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

	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true,
	}
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	client := &Client{
		conn:   conn,
		nick:   "",
		send:   make(chan Message, 16),
		cancel: cancel,
	}

	hub.register <- client

	go func() {
		history := getHistory(30)
		for _, msg := range history {
			select {
			case client.send <- msg:
			default:
			}
		}
	}()

	go client.writePump(ctx)
	client.readPump(ctx)
}

func main() {
	accessPassword = os.Getenv("CHAT_PASSWORD")
	if accessPassword == "" {
		accessPassword = "changeme"
		log.Println("Warning: Using default password 'changeme'")
	}

	serverPort = os.Getenv("PORT")
	if serverPort == "" {
		serverPort = "10699"  // 默认使用 10699 端口
	}

	var err error
	db, err = sql.Open("sqlite3", "./chat.db?_journal_mode=WAL&_synchronous=NORMAL&cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT,
		sender TEXT,
		receiver TEXT,
		content TEXT,
		timestamp INTEGER
	)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_time ON messages(timestamp)`)

	hub = initHub()
	go hub.run()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			cookie, err := r.Cookie("auth")
			if err != nil || cookie.Value != accessPassword {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(indexHTML)
			return
		}
		http.NotFound(w, r)
	})

	http.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			r.ParseForm()
			if r.FormValue("password") == accessPassword {
				http.SetCookie(w, &http.Cookie{
					Name:     "auth",
					Value:    accessPassword,
					Path:     "/",
					MaxAge:   86400,
					HttpOnly: true,
				})
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

	log.Printf("Server starting on :%s (password: %s)", serverPort, accessPassword)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}

const loginHTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Chat Login</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,-apple-system,sans-serif;background:linear-gradient(135deg,#667eea,#764ba2);min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:white;padding:30px;border-radius:12px;box-shadow:0 10px 40px rgba(0,0,0,0.2);width:90%%;max-width:350px}
h2{margin-bottom:20px;color:#333;text-align:center;font-size:20px}
input{width:100%%;padding:12px;margin:10px 0;border:2px solid #ddd;border-radius:6px;font-size:14px}
button{width:100%%;padding:12px;background:#667eea;color:white;border:none;border-radius:6px;font-size:14px;cursor:pointer;margin-top:10px}
button:hover{background:#5568d3}
.error{color:#e74c3c;text-align:center;margin-top:10px;font-size:13px}
</style>
</head>
<body>
<div class="box">
<h2>🔒 聊天室</h2>
<form method="POST" action="/login">
<input type="password" name="password" placeholder="输入访问密码" required autofocus>
<button type="submit">进入聊天室</button>
</form>
<div class="error" id="err"></div>
</div>
<script>
if(location.search.includes('error'))document.getElementById('err').textContent='密码错误';
</script>
</body>
</html>`
