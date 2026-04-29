package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed static/index.html
var indexHTML []byte

const (
	sendQueueSize         = 128
	broadcastQueueSize    = 256
	saveQueueSize         = 512
	loginHistoryLimit     = 40
	loadMoreLimit         = 40
	writeWait             = 10 * time.Second
	pingInterval          = 25 * time.Second
	readAckFlushInterval  = 60 * time.Second
	maxMentionReadEntries = 2000
	maxMessageChars       = 1000
)

var nickRegexp = regexp.MustCompile(`^[\p{Han}A-Za-z0-9_-]{2,15}$`)

type Message struct {
	ID                string           `json:"id,omitempty"`
	Type              string           `json:"type"`
	From              string           `json:"from"`
	To                string           `json:"to"`
	Content           string           `json:"content"`
	Timestamp         int64            `json:"timestamp"`
	Password          string           `json:"password,omitempty"`
	UserList          []UserStatus     `json:"user_list,omitempty"`
	Mentions          []string         `json:"mentions,omitempty"`
	Messages          []Message        `json:"messages,omitempty"`
	HasMore           bool             `json:"has_more,omitempty"`
	UnreadMap         map[string]int   `json:"unread_map,omitempty"`
	LastReadMap       map[string]int64 `json:"last_read_map,omitempty"`
	PrivateHasMoreMap map[string]bool  `json:"private_has_more_map,omitempty"`
	PublicHasMore     bool             `json:"public_has_more,omitempty"`
	Seq               int64            `json:"seq,omitempty"`
	DBID              int64            `json:"db_id,omitempty"`
}

type UserStatus struct {
	Name   string `json:"name"`
	Online bool   `json:"online"`
}

type Client struct {
	conn             *websocket.Conn
	nick             string
	send             chan Message
	cancel           context.CancelFunc
	mu               sync.Mutex
	lastReadMap      map[string]int64
	mentionReadMap   map[string]bool
	dirtyLastRead    bool
	dirtyMentionRead bool
	closed           bool
}

type Hub struct {
	clients       map[*Client]bool
	nickMap       map[string]*Client
	allUsers      map[string]struct{}
	broadcast     chan Message
	register      chan *Client
	unregister    chan *Client
	saveChan      chan Message
	userListDirty chan struct{}
	mu            sync.RWMutex
}

var (
	hub            *Hub
	db             *sql.DB
	accessPassword string
	serverPort     string
)

func initHub() *Hub {
	return &Hub{
		clients:       make(map[*Client]bool),
		nickMap:       make(map[string]*Client),
		allUsers:      make(map[string]struct{}),
		broadcast:     make(chan Message, broadcastQueueSize),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		saveChan:      make(chan Message, saveQueueSize),
		userListDirty: make(chan struct{}, 1),
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

func parseLastReadJSON(raw string) map[string]int64 {
	if strings.TrimSpace(raw) == "" {
		return map[string]int64{}
	}
	var m map[string]int64
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m == nil {
		return map[string]int64{}
	}
	return m
}

func lastReadToJSON(m map[string]int64) string {
	if m == nil {
		m = map[string]int64{}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func parseMentionReadJSON(raw string) map[string]bool {
	if strings.TrimSpace(raw) == "" {
		return map[string]bool{}
	}
	var m map[string]bool
	if err := json.Unmarshal([]byte(raw), &m); err != nil || m == nil {
		return map[string]bool{}
	}
	return trimMentionReadMap(m, maxMentionReadEntries)
}

func mentionReadToJSON(m map[string]bool) string {
	if m == nil {
		m = map[string]bool{}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func trimMentionReadMap(m map[string]bool, max int) map[string]bool {
	if len(m) <= max {
		return m
	}
	n := make(map[string]bool, max)
	i := 0
	for k, v := range m {
		n[k] = v
		i++
		if i >= max {
			break
		}
	}
	return n
}

func validateNick(nick string) bool {
	return nickRegexp.MatchString(nick)
}

func normalizeContent(content string) (string, bool) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", false
	}
	if len([]rune(content)) > maxMessageChars {
		return "", false
	}
	return content, true
}

func safeSend(c *Client, msg Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return false
	}

	select {
	case c.send <- msg:
		return true
	default:
		return false
	}
}

func (h *Hub) markUserListDirty() {
	select {
	case h.userListDirty <- struct{}{}:
	default:
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
			removedNick := ""
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				if client.nick != "" {
					if existing, ok := h.nickMap[client.nick]; ok && existing == client {
						delete(h.nickMap, client.nick)
						removedNick = client.nick
					}
				}
				client.mu.Lock()
				if !client.closed {
					client.closed = true
					close(client.send)
				}
				client.mu.Unlock()
				client.cancel()
			}
			h.mu.Unlock()

			if removedNick != "" {
				go flushClientReadState(client)
				h.markUserListDirty()
			}

		case msg := <-h.broadcast:
			h.mu.RLock()
			snapshot := make([]*Client, 0, len(h.clients))
			for client := range h.clients {
				snapshot = append(snapshot, client)
			}
			h.mu.RUnlock()

			for _, client := range snapshot {
				if !safeSend(client, msg) {
					go func(cl *Client) {
						h.unregister <- cl
					}(client)
				}
			}
		}
	}
}

func buildUserListSnapshot() []UserStatus {
	hub.mu.RLock()
	names := make([]string, 0, len(hub.allUsers))
	for name := range hub.allUsers {
		names = append(names, name)
	}
	sort.Strings(names)

	nickMap := make(map[string]*Client, len(hub.nickMap))
	for k, v := range hub.nickMap {
		nickMap[k] = v
	}
	hub.mu.RUnlock()

	list := make([]UserStatus, 0, len(names))
	for _, name := range names {
		_, isOnline := nickMap[name]
		list = append(list, UserStatus{Name: name, Online: isOnline})
	}

	sort.Slice(list, func(i, j int) bool {
		if list[i].Online != list[j].Online {
			return list[i].Online
		}
		return list[i].Name < list[j].Name
	})

	return list
}

func broadcastUserList() {
	list := buildUserListSnapshot()
	hub.broadcast <- Message{
		Type:      "online",
		UserList:  list,
		Timestamp: time.Now().Unix(),
	}
}

func saveMessage(msg Message) {
	switch msg.Type {
	case "system", "online", "read_sync", "mention_read_sync", "history_done", "history_page", "pong", "ping", "unread_sync":
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

func startSaveWorker() {
	go func() {
		for msg := range hub.saveChan {
			saveMessage(msg)
		}
	}()
}

func enqueueSave(msg Message) {
	select {
	case hub.saveChan <- msg:
	default:
		go func(m Message) {
			hub.saveChan <- m
		}(msg)
	}
}

func getAllUsernamesFromMemory() []string {
	hub.mu.RLock()
	defer hub.mu.RUnlock()

	names := make([]string, 0, len(hub.allUsers))
	for name := range hub.allUsers {
		names = append(names, name)
	}
	return names
}

func userExists(username string) bool {
	hub.mu.RLock()
	defer hub.mu.RUnlock()
	_, ok := hub.allUsers[username]
	return ok
}

func computeUnreadMap(lastRead map[string]int64, me string) map[string]int {
	result := make(map[string]int)

	publicLast := lastRead["public"]
	var publicCount int
	_ = db.QueryRow("SELECT COUNT(*) FROM messages WHERE type='public' AND timestamp > ?", publicLast).Scan(&publicCount)
	if publicCount > 0 {
		result["public"] = publicCount
	}

	hub.mu.RLock()
	users := make([]string, 0, len(hub.allUsers))
	for name := range hub.allUsers {
		if name != me {
			users = append(users, name)
		}
	}
	hub.mu.RUnlock()

	for _, peer := range users {
		last := lastRead[peer]
		var count int
		_ = db.QueryRow(
			"SELECT COUNT(*) FROM messages WHERE type='private' AND sender=? AND receiver=? AND timestamp > ?",
			peer, me, last,
		).Scan(&count)
		if count > 0 {
			result[peer] = count
		}
	}

	return result
}

func loadInitialHistory(nick string) ([]Message, bool, map[string]bool, error) {
	rows, err := db.Query(`
		SELECT id, msg_id, type, sender, receiver, content, timestamp, mentions
		FROM messages
		WHERE type='public' OR (type='private' AND (sender=? OR receiver=?))
		ORDER BY timestamp DESC, id DESC
		LIMIT ?
	`, nick, nick, loginHistoryLimit)
	if err != nil {
		return nil, false, nil, err
	}
	defer rows.Close()

	msgs := make([]Message, 0, loginHistoryLimit)
	for rows.Next() {
		var m Message
		var mentionsRaw string
		if err := rows.Scan(&m.DBID, &m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.Timestamp, &mentionsRaw); err != nil {
			continue
		}
		m.Mentions = parseMentionsJSON(mentionsRaw)
		m.Seq = m.DBID
		msgs = append(msgs, m)
	}

	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	publicHasMore := false
	privateHasMore := make(map[string]bool)

	if len(msgs) == 0 {
		return msgs, false, privateHasMore, nil
	}

	var publicMinID int64
	privateMinIDMap := make(map[string]int64)

	for _, m := range msgs {
		if m.Type == "public" {
			if publicMinID == 0 || m.DBID < publicMinID {
				publicMinID = m.DBID
			}
			continue
		}

		peer := m.From
		if peer == nick {
			peer = m.To
		}
		if old, ok := privateMinIDMap[peer]; !ok || m.DBID < old {
			privateMinIDMap[peer] = m.DBID
		}
	}

	if publicMinID > 0 {
		var count int
		_ = db.QueryRow("SELECT COUNT(*) FROM messages WHERE type='public' AND id < ?", publicMinID).Scan(&count)
		publicHasMore = count > 0
	}

	for peer, minID := range privateMinIDMap {
		var count int
		_ = db.QueryRow(`
			SELECT COUNT(*) FROM messages
			WHERE type='private'
			  AND ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
			  AND id < ?
		`, nick, peer, peer, nick, minID).Scan(&count)
		privateHasMore[peer] = count > 0
	}

	return msgs, publicHasMore, privateHasMore, nil
}

func flushClientReadState(c *Client) {
	if c == nil || c.nick == "" {
		return
	}

	c.mu.Lock()
	if !c.dirtyLastRead && !c.dirtyMentionRead {
		c.mu.Unlock()
		return
	}

	lastReadJSON := lastReadToJSON(c.lastReadMap)
	mentionReadJSON := mentionReadToJSON(trimMentionReadMap(c.mentionReadMap, maxMentionReadEntries))
	dirtyLast := c.dirtyLastRead
	dirtyMention := c.dirtyMentionRead
	c.dirtyLastRead = false
	c.dirtyMentionRead = false
	nick := c.nick
	c.mu.Unlock()

	switch {
	case dirtyLast && dirtyMention:
		_, err := db.Exec("UPDATE users SET last_read_at = ?, mention_read_at = ? WHERE username = ?", lastReadJSON, mentionReadJSON, nick)
		if err != nil {
			log.Println("flush read state error:", err)
		}
	case dirtyLast:
		_, err := db.Exec("UPDATE users SET last_read_at = ? WHERE username = ?", lastReadJSON, nick)
		if err != nil {
			log.Println("flush last_read error:", err)
		}
	case dirtyMention:
		_, err := db.Exec("UPDATE users SET mention_read_at = ? WHERE username = ?", mentionReadJSON, nick)
		if err != nil {
			log.Println("flush mention_read error:", err)
		}
	}
}

func startReadStateFlusher() {
	go func() {
		ticker := time.NewTicker(readAckFlushInterval)
		defer ticker.Stop()

		for range ticker.C {
			hub.mu.RLock()
			clients := make([]*Client, 0, len(hub.clients))
			for c := range hub.clients {
				clients = append(clients, c)
			}
			hub.mu.RUnlock()

			for _, c := range clients {
				flushClientReadState(c)
			}
		}
	}()
}

func startUserListBroadcaster() {
	go func() {
		for range hub.userListDirty {
			broadcastUserList()
			time.Sleep(50 * time.Millisecond)
		}
	}()
}

func (c *Client) handleNick(msg Message) {
	nick := strings.TrimSpace(msg.From)
	pwd := hashPwd(msg.Password)

	if !validateNick(nick) {
		_ = safeSend(c, Message{Type: "system", Content: "昵称仅支持2-15位中文、字母、数字、下划线或中划线"})
		return
	}

	var storedPwd string
	var lastReadJSON, mentionReadJSON string

	err := db.QueryRow("SELECT password, last_read_at, mention_read_at FROM users WHERE username = ?", nick).
		Scan(&storedPwd, &lastReadJSON, &mentionReadJSON)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, insErr := db.Exec(
			"INSERT INTO users (username, password, last_read_at, mention_read_at) VALUES (?, ?, '{}', '{}')",
			nick, pwd,
		)
		if insErr != nil {
			log.Println("insert user error:", insErr)
			err = db.QueryRow("SELECT password, last_read_at, mention_read_at FROM users WHERE username = ?", nick).
				Scan(&storedPwd, &lastReadJSON, &mentionReadJSON)
			if err != nil {
				_ = safeSend(c, Message{Type: "system", Content: "登录失败，请稍后重试"})
				return
			}
			if storedPwd != pwd {
				_ = safeSend(c, Message{Type: "system", Content: "密码错误或昵称被占用"})
				return
			}
		} else {
			storedPwd = pwd
			lastReadJSON = "{}"
			mentionReadJSON = "{}"
		}
	case err != nil:
		log.Println("query user error:", err)
		_ = safeSend(c, Message{Type: "system", Content: "登录失败，请稍后重试"})
		return
	case storedPwd != pwd:
		_ = safeSend(c, Message{Type: "system", Content: "密码错误或昵称被占用"})
		return
	}

	hub.mu.Lock()
	if existing, online := hub.nickMap[nick]; online && existing != c {
		hub.mu.Unlock()
		_ = safeSend(c, Message{Type: "system", Content: "此账号已在别处登录"})
		return
	}
	c.nick = nick
	hub.nickMap[nick] = c
	hub.allUsers[nick] = struct{}{}
	hub.mu.Unlock()

	c.mu.Lock()
	c.lastReadMap = parseLastReadJSON(lastReadJSON)
	c.mentionReadMap = parseMentionReadJSON(mentionReadJSON)
	c.mu.Unlock()

	_ = safeSend(c, Message{
		Type:        "read_sync",
		Content:     lastReadToJSON(c.lastReadMap),
		LastReadMap: c.lastReadMap,
	})
	_ = safeSend(c, Message{
		Type:    "mention_read_sync",
		Content: mentionReadToJSON(c.mentionReadMap),
	})

	unreadMap := computeUnreadMap(c.lastReadMap, c.nick)
	_ = safeSend(c, Message{
		Type:      "unread_sync",
		UnreadMap: unreadMap,
	})

	go c.loadHistoryAndSync()
	hub.markUserListDirty()
}

func (c *Client) loadHistoryAndSync() {
	if c.nick == "" {
		return
	}

	msgs, publicHasMore, privateHasMore, err := loadInitialHistory(c.nick)
	if err != nil {
		log.Println("load history error:", err)
		_ = safeSend(c, Message{Type: "history_done"})
		return
	}

	_ = safeSend(c, Message{
		Type:              "history_done",
		Messages:          msgs,
		PublicHasMore:     publicHasMore,
		PrivateHasMoreMap: privateHasMore,
	})
}

func (c *Client) handleReadAck(msg Message) {
	if c.nick == "" {
		return
	}
	target := strings.TrimSpace(msg.To)
	if target == "" {
		target = "public"
	}
	c.mu.Lock()
	if c.lastReadMap == nil {
		c.lastReadMap = make(map[string]int64)
	}
	c.lastReadMap[target] = time.Now().Unix()
	c.dirtyLastRead = true
	c.mu.Unlock()
}

func (c *Client) handleMentionReadAck(msg Message) {
	if c.nick == "" {
		return
	}
	mentionMsgID := strings.TrimSpace(msg.Content)
	if mentionMsgID == "" {
		return
	}
	c.mu.Lock()
	if c.mentionReadMap == nil {
		c.mentionReadMap = make(map[string]bool)
	}
	c.mentionReadMap[mentionMsgID] = true
	c.mentionReadMap = trimMentionReadMap(c.mentionReadMap, maxMentionReadEntries)
	c.dirtyMentionRead = true
	c.mu.Unlock()
}

func (c *Client) handlePublic(msg Message) {
	if c.nick == "" {
		return
	}

	content, ok := normalizeContent(msg.Content)
	if !ok {
		_ = safeSend(c, Message{Type: "system", Content: fmt.Sprintf("消息不能为空且不能超过%d个字符", maxMessageChars)})
		return
	}

	msg.ID = generateMessageID()
	msg.From = c.nick
	msg.Timestamp = time.Now().Unix()
	msg.Content = content

	usernames := getAllUsernamesFromMemory()
	msg.Mentions = parseMentions(msg.Content, usernames, c.nick)

	hub.broadcast <- msg
	enqueueSave(msg)
}

func (c *Client) handlePrivate(msg Message) {
	if c.nick == "" {
		return
	}

	targetUser := strings.TrimSpace(msg.To)
	if targetUser == "" || targetUser == c.nick {
		_ = safeSend(c, Message{Type: "system", Content: "私聊目标无效"})
		return
	}
	if !userExists(targetUser) {
		_ = safeSend(c, Message{Type: "system", Content: "目标用户不存在"})
		return
	}

	content, ok := normalizeContent(msg.Content)
	if !ok {
		_ = safeSend(c, Message{Type: "system", Content: fmt.Sprintf("消息不能为空且不能超过%d个字符", maxMessageChars)})
		return
	}

	msg.ID = generateMessageID()
	msg.From = c.nick
	msg.To = targetUser
	msg.Timestamp = time.Now().Unix()
	msg.Content = content

	_ = safeSend(c, msg)

	hub.mu.RLock()
	target, exists := hub.nickMap[msg.To]
	hub.mu.RUnlock()

	if exists && target != c {
		_ = safeSend(target, msg)
	}

	enqueueSave(msg)
}

func (c *Client) handleLoadMore(msg Message) {
	if c.nick == "" {
		return
	}

	chat := "public"
	if strings.TrimSpace(msg.To) != "" {
		chat = strings.TrimSpace(msg.To)
	}

	beforeTs := msg.Timestamp
	beforeSeq := msg.Seq
	if beforeTs <= 0 {
		beforeTs = time.Now().Unix() + 1
	}
	if beforeSeq <= 0 {
		beforeSeq = 1 << 62
	}

	var rows *sql.Rows
	var err error

	if chat == "public" {
		rows, err = db.Query(`
			SELECT id, msg_id, type, sender, receiver, content, timestamp, mentions
			FROM messages
			WHERE type='public'
			  AND (timestamp < ? OR (timestamp = ? AND id < ?))
			ORDER BY timestamp DESC, id DESC
			LIMIT ?
		`, beforeTs, beforeTs, beforeSeq, loadMoreLimit)
	} else {
		if !userExists(chat) {
			_ = safeSend(c, Message{Type: "system", Content: "目标用户不存在"})
			return
		}

		rows, err = db.Query(`
			SELECT id, msg_id, type, sender, receiver, content, timestamp, mentions
			FROM messages
			WHERE type='private'
			  AND ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
			  AND (timestamp < ? OR (timestamp = ? AND id < ?))
			ORDER BY timestamp DESC, id DESC
			LIMIT ?
		`, c.nick, chat, chat, c.nick, beforeTs, beforeTs, beforeSeq, loadMoreLimit)
	}

	if err != nil {
		log.Println("load more error:", err)
		_ = safeSend(c, Message{Type: "history_page", To: chat, HasMore: false})
		return
	}
	defer rows.Close()

	msgs := make([]Message, 0, loadMoreLimit)
	for rows.Next() {
		var m Message
		var mentionsRaw string
		if err := rows.Scan(&m.DBID, &m.ID, &m.Type, &m.From, &m.To, &m.Content, &m.Timestamp, &mentionsRaw); err != nil {
			continue
		}
		m.Mentions = parseMentionsJSON(mentionsRaw)
		m.Seq = m.DBID
		msgs = append(msgs, m)
	}

	hasMore := false
	if len(msgs) > 0 {
		last := msgs[len(msgs)-1]
		if chat == "public" {
			var count int
			_ = db.QueryRow(`
				SELECT COUNT(*) FROM messages
				WHERE type='public'
				  AND (timestamp < ? OR (timestamp = ? AND id < ?))
			`, last.Timestamp, last.Timestamp, last.DBID).Scan(&count)
			hasMore = count > 0
		} else {
			var count int
			_ = db.QueryRow(`
				SELECT COUNT(*) FROM messages
				WHERE type='private'
				  AND ((sender=? AND receiver=?) OR (sender=? AND receiver=?))
				  AND (timestamp < ? OR (timestamp = ? AND id < ?))
			`, c.nick, chat, chat, c.nick, last.Timestamp, last.Timestamp, last.DBID).Scan(&count)
			hasMore = count > 0
		}
	}

	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}

	_ = safeSend(c, Message{
		Type:     "history_page",
		To:       chat,
		Messages: msgs,
		HasMore:  hasMore,
	})
}

func (c *Client) readPump(ctx context.Context) {
	defer func() {
		hub.unregister <- c
		_ = c.conn.Close(websocket.StatusNormalClosure, "")
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

		switch msg.Type {
		case "ping":
			_ = safeSend(c, Message{Type: "pong", Timestamp: time.Now().Unix()})
		case "nick":
			c.handleNick(msg)
		case "read_ack":
			c.handleReadAck(msg)
		case "mention_read_ack":
			c.handleMentionReadAck(msg)
		case "public":
			c.handlePublic(msg)
		case "private":
			c.handlePrivate(msg)
		case "load_more":
			c.handleLoadMore(msg)
		}
	}
}

func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-c.send:
			if !ok {
				return
			}

			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}

			writeCtx, cancel := context.WithTimeout(ctx, writeWait)
			err = c.conn.Write(writeCtx, websocket.MessageText, data)
			cancel()
			if err != nil {
				return
			}

		case <-ticker.C:
			writeCtx, cancel := context.WithTimeout(ctx, writeWait)
			err := c.conn.Ping(writeCtx)
			cancel()
			if err != nil {
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func loadAllUsersToMemory() error {
	rows, err := db.Query("SELECT username FROM users")
	if err != nil {
		return err
	}
	defer rows.Close()

	users := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil && strings.TrimSpace(name) != "" {
			users[name] = struct{}{}
		}
	}

	hub.mu.Lock()
	hub.allUsers = users
	hub.mu.Unlock()
	return nil
}

func setupDatabase() error {
	var err error
	db, err = sql.Open("sqlite3", "./chat.db?_journal_mode=WAL&_busy_timeout=10000")
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA busy_timeout=10000;",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			log.Println("pragma warn:", err)
		}
	}

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS users (
			username TEXT PRIMARY KEY,
			password TEXT,
			last_read_at TEXT DEFAULT '{}',
			mention_read_at TEXT DEFAULT '{}'
		)`,
		`CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			msg_id TEXT,
			type TEXT,
			sender TEXT,
			receiver TEXT,
			content TEXT,
			timestamp INTEGER,
			mentions TEXT DEFAULT '[]'
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(timestamp DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_private ON messages(type, sender, receiver, timestamp DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_msgid ON messages(msg_id)`,
	}

	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}

	_, _ = db.Exec(`ALTER TABLE users ADD COLUMN last_read_at TEXT DEFAULT '{}'`)
	_, _ = db.Exec(`ALTER TABLE users ADD COLUMN mention_read_at TEXT DEFAULT '{}'`)
	_, _ = db.Exec(`ALTER TABLE messages ADD COLUMN msg_id TEXT`)
	_, _ = db.Exec(`ALTER TABLE messages ADD COLUMN mentions TEXT DEFAULT '[]'`)

	return nil
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

	hub = initHub()

	if err := setupDatabase(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := loadAllUsersToMemory(); err != nil {
		log.Println("load users warn:", err)
	}

	startSaveWorker()
	startReadStateFlusher()
	startUserListBroadcaster()
	go hub.run()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie("auth")
		if err != nil || cookie.Value != accessPassword {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
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
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Login</title><style>body{font-family:sans-serif;display:flex;align-items:center;justify-content:center;height:100vh;background:#1a1d24;color:#fff}form{background:#2a2d36;padding:30px;border-radius:15px;box-shadow:0 10px 30px rgba(0,0,0,0.5)}input{display:block;width:100%;margin:15px 0;padding:12px;border-radius:8px;border:none}button{width:100%;padding:12px;background:#6c63ff;color:#fff;border:none;border-radius:8px;cursor:pointer}</style></head><body><form method="POST"><h2>宇宙公司聊天室</h2><button type="submit">登陆</button><input type="password" name="password" placeholder="请输入聊天室密码" required autofocus></form></body></html>`))
	})

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		cookie, _ := r.Cookie("auth")
		if cookie == nil || cookie.Value != accessPassword {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}

		ctx, cancel := context.WithCancel(r.Context())
		client := &Client{
			conn:           conn,
			send:           make(chan Message, sendQueueSize),
			cancel:         cancel,
			lastReadMap:    make(map[string]int64),
			mentionReadMap: make(map[string]bool),
		}

		hub.register <- client
		go client.writePump(ctx)
		client.readPump(ctx)
	})

	log.Printf("Running on :%s", serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, nil))
}
