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
			// 不在此处广播，等设置昵称后再广播

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
			// 只保存公共消息（私聊在 readPump 单独处理和保存）
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

// getHistory 获取历史消息：公共消息 + 当前用户参与的私聊消息
// 私聊消息严格隔离，只返回与 nick 有关的记录
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
			// 检查昵称是否被占用（不允许重复昵称）
			if existing, taken := hub.nickMap[newNick]; taken && existing != c {
				hub.mu.Unlock()
				errMsg := Message{
					Type:      "system",
					Content:   "昵称 "" + newNick + "" 已被占用，请换一个",
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

			// 昵称设置完毕后，立即发送历史记录（含私聊隔离）
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

			// 广播改名通知
			if oldNick != "" {
				sysMsg := Message{
					Type:      "system",
					Content:   oldNick + " 改名为 " + newNick,
					Timestamp: time.Now().Unix(),
				}
				hub.broadcast <- sysMsg
			}

		case "public":
			if c.nick == "" || msg.Content == "" {
				continue
			}
			hub.broadcast <- msg

		case "private":
			if c.nick == "" || msg.Content == "" || msg.To == "" {
				continue
			}
			msg.From = c.nick

			hub.mu.RLock()
			target, exists := hub.nickMap[msg.To]
			hub.mu.RUnlock()

			if exists {
				// ✅ 修复：同时发给接收者和发送者（两个独立 select，不再互斥）
				select {
				case target.send <- msg:
				default:
					log.Printf("Private msg drop: target %s channel full", msg.To)
				}
				// 如果是发给自己以外的人，发送者也要收到自己的消息（回显）
				if target != c {
					select {
					case c.send <- msg:
					default:
					}
				}
				// 保存私聊记录
				saveMessage(msg)
			} else {
				errMsg := Message{
					Type:      "system",
					Content:   "用户 " + msg.To + " 不在线",
					Timestamp: time.Now().Unix(),
				}
				select {
				case c.send <- errMsg:
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
		send:   make(chan Message, 32),
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
	db.SetMaxOpenConns(1) // SQLite WAL 模式下单写连接足够，节省内存
	db.SetMaxIdleConns(1)

	db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		type TEXT,
		sender TEXT,
		receiver TEXT,
		content TEXT,
		timestamp INTEGER
	)`)
	// 复合索引：加速私聊历史查询
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_time ON messages(timestamp)`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_private ON messages(type, sender, receiver)`)

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
body{font-family:system-ui,-apple-system,sans-serif;background:linear-gradient(135deg,#667eea,#764ba2);min-height:100vh;display:flex;align-items:center;justify-content:center}
.box{background:white;padding:32px 28px;border-radius:16px;box-shadow:0 10px 40px rgba(0,0,0,0.2);width:90%;max-width:360px}
h2{margin-bottom:22px;color:#333;text-align:center;font-size:22px}
input{width:100%;padding:13px 14px;margin:8px 0;border:2px solid #e0e0e0;border-radius:8px;font-size:15px;outline:none;transition:border-color .2s}
input:focus{border-color:#667eea}
button{width:100%;padding:13px;background:#667eea;color:white;border:none;border-radius:8px;font-size:15px;font-weight:600;cursor:pointer;margin-top:10px;transition:background .2s}
button:hover{background:#5568d3}
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
