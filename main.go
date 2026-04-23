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
				nick := client.nick
				delete(h.clients, client)
				if nick != "" {
					delete(h.nickMap, nick)
				}
				close(client.send)
				client.cancel()
				h.mu.Unlock()

				if nick != "" {
					broadcastOnline()
					h.broadcast <- Message{
						Type:      "system",
						Content:   nick + " 离开了聊天室",
						Timestamp: time.Now().Unix(),
					}
				}
			} else {
				h.mu.Unlock()
			}

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
			if len([]rune(newNick)) > 20 {
				select {
				case c.send <- Message{Type: "system", Content: "昵称长度不能超过20个字符", Timestamp: time.Now().Unix()}:
				default:
				}
				continue
			}

			oldNick := c.nick
			hub.mu.Lock()
			if existing, taken := hub.nickMap[newNick]; taken && existing != c {
				hub.mu.Unlock()
				select {
				case c.send <- Message{Type: "system", Content: "昵称 \"" + newNick + "\" 已被占用，请换一个", Timestamp: time.Now().Unix()}:
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

			// 昵称设置后发送历史（同步，确保顺序）
			history := getHistory(c.nick, 80)
			for _, m := range history {
				select {
				case c.send <- m:
				default:
				}
			}

			broadcastOnline()

			if oldNick == "" {
				hub.broadcast <- Message{Type: "system", Content: newNick + " 加入了聊天室", Timestamp: time.Now().Unix()}
			} else {
				hub.broadcast <- Message{Type: "system", Content: oldNick + " 改名为 " + newNick, Timestamp: time.Now().Unix()}
			}

		case "public":
			if c.nick == "" || strings.TrimSpace(msg.Content) == "" {
				continue
			}
			if len(msg.Content) > 2000 {
				msg.Content = msg.Content[:2000]
			}
			hub.broadcast <- msg

		case "private":
			if c.nick == "" || strings.TrimSpace(msg.Content) == "" || msg.To == "" {
				continue
			}
			if len(msg.Content) > 2000 {
				msg.Content = msg.Content[:2000]
			}
			msg.From = c.nick

			hub.mu.RLock()
			target, exists := hub.nickMap[msg.To]
			hub.mu.RUnlock()

			if exists {
				select {
				case target.send <- msg:
				default:
					log.Printf("Private msg drop: target %s channel full", msg.To)
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
				case c.send <- Message{Type: "system", Content: "用户 " + msg.To + " 不在线", Timestamp: time.Now().Unix()}:
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

	opts := &websocket.AcceptOptions{InsecureSkipVerify: true}
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	client := &Client{
		conn:   conn,
		nick:   "",
		send:   make(chan Message, 64),
		cancel: cancel,
	}

	hub.register <- client
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
		serverPort = "10699"
	}

	var err error
	db, err = sql.Open("sqlite3", "./chat.db?_journal_mode=WAL&_synchronous=NORMAL&cache=shared")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT,
		sender TEXT,
		receiver TEXT,
		content TEXT,
		timestamp INTEGER
	)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_time ON messages(timestamp)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_private_sender ON messages(type, sender)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_private_receiver ON messages(type, receiver)`)

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
			w.Header().Set("Cache-Control", "no-cache")
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
					MaxAge:   86400 * 7,
					HttpOnly: true,
					SameSite: http.SameSiteLaxMode,
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

	log.Printf("Server starting on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}

const loginHTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>聊天室登录</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:system-ui,-apple-system,sans-serif;background:linear-gradient(135deg,#5b6abf,#764ba2);min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:white;padding:32px 28px;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,0.2);width:90%;max-width:360px}
h2{margin-bottom:22px;color:#333;text-align:center;font-size:22px}
input{width:100%;padding:13px 14px;margin:8px 0;border:2px solid #e0e0e0;border-radius:8px;font-size:15px;outline:none;transition:border-color .2s}
input:focus{border-color:#5b6abf}
button{width:100%;padding:13px;background:#5b6abf;color:white;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer;margin-top:10px;transition:background .2s}
button:hover{background:#4a59a8}
.error{color:#e74c3c;text-align:center;margin-top:12px;font-size:13px;min-height:18px}
</style>
</head>
<body>
<div class="box">
<h2>🔒 进入聊天室</h2>
<form method="POST" action="/login">
<input type="password" name="password" placeholder="输入访问密码" required autofocus>
<button type="submit">进入</button>
</form>
<div class="error" id="err"></div>
</div>
<script>if(location.search.includes('error'))document.getElementById('err').textContent='密码错误，请重试';</script>
</body>
</html>`
