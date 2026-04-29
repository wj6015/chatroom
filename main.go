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
	"math/rand"
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
	ID        string       `json:"id,omitempty"`
	Type      string       `json:"type"`
	From      string       `json:"from"`
	To        string       `json:"to"`
	Content   string       `json:"content"`
	Timestamp int64        `json:"timestamp"`
	Password  string       `json:"password,omitempty"`
	UserList  []UserStatus `json:"user_list,omitempty"`
	Mentions  []string     `json:"mentions,omitempty"`
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

func generateMessageID() string {
	return fmt.Sprintf("msg_%d_%04d", time.Now().UnixMilli(), rand.Intn(10000))
}

func getAllUsernames() ([]string, error) {
	rows, err := db.Query("SELECT username FROM users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			names = append(names, name)
		}
	}
	return names, nil
}

func parseMentions(content string, usernames []string, sender string) []string {
	content = strings.TrimSpace(content)
	if content == "" {
		return nil
	}

	candidates := make([]string, 0, len(usernames))
	for _, name := range usernames {
		name = strings.TrimSpace(name)
		if name == "" || name == sender {
			continue
		}
		candidates = append(candidates, name)
	}

	sort.Slice(candidates, func(i, j int) bool {
		return len([]rune(candidates[i])) > len([]rune(candidates[j]))
	})

	seen := make(map[string]bool)
	var result []string
	runes := []rune(content)

	for i := 0; i < len(runes); i++ {
		if runes[i] != '@' {
			continue
		}

		if i > 0 && !isWhitespaceRune(runes[i-1]) {
			continue
		}

		rest := string(runes[i+1:])
		for _, name := range candidates {
			if strings.HasPrefix(rest, name) {
				nextPos := i + 1 + len([]rune(name))
				var nextRune rune
				if nextPos < len(runes) {
					nextRune = runes[nextPos]
				}
				if nextPos == len(runes) || isMentionBoundary(nextRune) {
					if !seen[name] {
						seen[name] = true
						result = append(result, name)
					}
					break
				}
			}
		}
	}

	return result
}

func isWhitespaceRune(r rune) bool {
	return r == ' ' || r == '\n' || r == '\t' || r == '\r'
}

func isMentionBoundary(r rune) bool {
	if r == 0 {
		return true
	}
	switch r {
	case ' ', '\n', '\t', '\r', ',', '，', '.', '。', '!', '！', '?', '？', ':', '：', ';', '；':
		return true
	default:
		return false
	}
}

func mentionsToJSON(mentions []string) string {
	if len(mentions) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(mentions)
	return string(b)
}

func parseMentionsJSON(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var mentions []string
	_ = json.Unmarshal([]byte(raw), &mentions)
	return mentions
}

func parseMentionReadJSON(raw string) map[string]bool {
	if strings.TrimSpace(raw) == "" {
		return map[string]bool{}
	}
	var m map[string]bool
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m == nil {
		return map[string]bool{}
	}
	return m
}

func mentionReadToJSON(m map[string]bool) string {
	if m == nil {
		m = map[string]bool{}
	}
	b, _ := json.Marshal(m)
	return string(b)
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

func broadcastUserList() {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	rows, err := db.Query("SELECT username FROM users")
	if err != nil {
		return
	}
	defer rows.Close()

	var list []UserStatus
	for rows.Next() {
		var name string
		rows.Scan(&name)
		_, isOnline := hub.nickMap[name]
		list = append(list, UserStatus{Name: name, Online: isOnline})
	}

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
	if msg.Type == "system" || msg.Type == "online" || msg.Type == "read_sync" || msg.Type == "mention_read_sync" {
		return
	}
	_, err := db.Exec(
		"INSERT INTO messages (msg_id, type, sender, receiver, content, timestamp, mentions) VALUES (?, ?, ?, ?, ?, ?, ?)",
		msg.ID, msg.Type, msg.From, msg.To, msg.Content, msg.Timestamp, mentionsToJSON(msg.Mentions),
	)
	if err != nil {
		log.Println("DB Error:", err)
	}
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

		switch msg.Type {
		case "nick":
			nick := strings.TrimSpace(msg.From)
			pwd := hashPwd(msg.Password)
			if len([]rune(nick)) < 2 {
				continue
			}

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

			var lastReadJSON string
			db.QueryRow("SELECT last_read_at FROM users WHERE username = ?", nick).Scan(&lastReadJSON)
			c.send <- Message{Type: "read_sync", Content: lastReadJSON}

			var mentionReadJSON string
			db.QueryRow("SELECT mention_read_at FROM users WHERE username = ?", nick).Scan(&mentionReadJSON)
			c.send <- Message{Type: "mention_read_sync", Content: mentionReadJSON}

			rows, _ := db.Query(`
				SELECT msg_id, type, sender, receiver, content, timestamp, mentions
				FROM messages
				WHERE type='public' OR (type='private' AND (sender=? OR receiver=?))
				ORDER BY timestamp DESC, id DESC
				LIMIT 80
			`, nick, nick)

			var msgs []Message
			for rows.Next() {
				var m Message
				var mentionsRaw string
				rows.Scan(&m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.Timestamp, &mentionsRaw)
				m.Mentions = parseMentionsJSON(mentionsRaw)
				msgs = append([]Message{m}, msgs...)
			}
			rows.Close()

			for _, m := range msgs {
				c.send <- m
			}

			broadcastUserList()

		case "read_ack":
			if c.nick == "" {
				continue
			}

			var lastReadMap map[string]int64
			var rawJSON string
			db.QueryRow("SELECT last_read_at FROM users WHERE username = ?", c.nick).Scan(&rawJSON)
			if rawJSON == "" {
				rawJSON = "{}"
			}
			json.Unmarshal([]byte(rawJSON), &lastReadMap)
			if lastReadMap == nil {
				lastReadMap = make(map[string]int64)
			}

			target := msg.To
			if target == "" {
				target = "public"
			}
			lastReadMap[target] = time.Now().Unix()

			newJSON, _ := json.Marshal(lastReadMap)
			db.Exec("UPDATE users SET last_read_at = ? WHERE username = ?", string(newJSON), c.nick)

		case "mention_read_ack":
			if c.nick == "" {
				continue
			}

			mentionMsgID := strings.TrimSpace(msg.Content)
			if mentionMsgID == "" {
				continue
			}

			var rawJSON string
			db.QueryRow("SELECT mention_read_at FROM users WHERE username = ?", c.nick).Scan(&rawJSON)
			readMap := parseMentionReadJSON(rawJSON)
			readMap[mentionMsgID] = true

			db.Exec("UPDATE users SET mention_read_at = ? WHERE username = ?", mentionReadToJSON(readMap), c.nick)

		case "public":
			if c.nick == "" {
				continue
			}
			msg.ID = generateMessageID()
			msg.From = c.nick

			usernames, err := getAllUsernames()
			if err == nil {
				msg.Mentions = parseMentions(msg.Content, usernames, c.nick)
			} else {
				msg.Mentions = nil
			}

			hub.broadcast <- msg

		case "private":
			if c.nick == "" {
				continue
			}
			msg.ID = generateMessageID()
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
			if !ok {
				return
			}
			data, _ := json.Marshal(msg)
			c.conn.Write(ctx, websocket.MessageText, data)

		case <-ticker.C:
			if c.conn.Ping(ctx) != nil {
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	accessPassword = os.Getenv("CHAT_PASSWORD")
	if accessPassword == "" {
		accessPassword = "changeme"
	}
	serverPort = os.Getenv("PORT")
	if serverPort == "" {
		serverPort = "10699"
	}

	db, _ = sql.Open("sqlite3", "./chat.db?_journal_mode=WAL")
	defer db.Close()
	db.SetMaxOpenConns(1)

	db.Exec(`CREATE TABLE IF NOT EXISTS users (
		username TEXT PRIMARY KEY,
		password TEXT,
		last_read_at TEXT DEFAULT '{}',
		mention_read_at TEXT DEFAULT '{}'
	)`)

	db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		msg_id TEXT,
		type TEXT,
		sender TEXT,
		receiver TEXT,
		content TEXT,
		timestamp INTEGER,
		mentions TEXT DEFAULT '[]'
	)`)

	db.Exec(`ALTER TABLE users ADD COLUMN last_read_at TEXT DEFAULT '{}'`)
	db.Exec(`ALTER TABLE users ADD COLUMN mention_read_at TEXT DEFAULT '{}'`)
	db.Exec(`ALTER TABLE messages ADD COLUMN msg_id TEXT`)
	db.Exec(`ALTER TABLE messages ADD COLUMN mentions TEXT DEFAULT '[]'`)

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
		w.Write([]byte(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Login</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#1a1d24;color:#fff}form{background:#2a2d36;padding:30px;border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,0.5)}input{display:block;width:100%;margin:15px 0;padding:12px;border-radius:8px;border:none}button{width:100%;padding:12px;background:#6c63ff;color:#fff;border:none;border-radius:8px;cursor:pointer}</style></head><body><form method="POST"><h2>宇宙公司聊天室</h2><button type="submit">登陆</button><input type="password" name="password" placeholder="请输入聊天室密码" required autofocus></form></body></html>`))
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		cookie, _ := r.Cookie("auth")
		if cookie == nil || cookie.Value != accessPassword {
			return
		}

		conn, _ := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		ctx, cancel := context.WithCancel(r.Context())
		client := &Client{
			conn:   conn,
			send:   make(chan Message, 64),
			cancel: cancel,
		}
		hub.register <- client
		go client.writePump(ctx)
		client.readPump(ctx)
	})

	log.Printf("Running on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}
