package amp

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	defaultNeoRuntimeHost               = "127.0.0.1"
	defaultNeoRuntimePort               = 6420
	defaultAmpProxyPort                 = 8317
	defaultNeoExecutorConnectTimeout    = 90 * time.Second
	defaultNeoRemoteControlPollInterval = 5 * time.Second
	defaultNeoRemoteControlMaxThreads   = 20
	defaultNeoTitleModel                = "claude-haiku-4-5-20251001"
	neoCloudGzipBytes                   = 10 * 1024 * 1024
	neoReplayEventLimit                 = 512
	neoActorIdleTTL                     = 30 * time.Minute
	neoActorPruneInterval               = 5 * time.Minute
)

var (
	neoThreadIDPattern      = regexp.MustCompile(`T-[0-9A-Za-z][0-9A-Za-z-]*`)
	neoThreadIDExactPattern = regexp.MustCompile(`^T-[0-9A-Za-z][0-9A-Za-z-]*$`)
	neoCloudThreadIDPattern = regexp.MustCompile(`^T-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	neoAmpThreadStoreDir    = defaultNeoAmpThreadStoreDir
	neoModeToolAllowlist    = map[string]map[string]bool{
		"smart": toolSet("Read", "finder", "Bash", "create_file", "edit_file", "web_search", "read_web_page", "read_thread", "find_thread", "skill", "oracle", "librarian", "Task", "look_at", "handoff", "painter", "read_mcp_resource"),
		"rush":  toolSet("Read", "Grep", "glob", "finder", "Bash", "create_file", "edit_file", "get_diagnostics", "web_search", "read_web_page", "read_mcp_resource", "chart", "read_thread", "find_thread", "skill", "oracle", "handoff", "librarian", "Task", "task_list", "look_at", "painter"),
		"deep":  toolSet("shell_command", "apply_patch", "web_search", "read_web_page", "chart", "skill", "read_thread", "find_thread", "librarian", "oracle", "finder", "look_at", "painter", "handoff", "send_message_to_aggman"),
	}
	neoKnownModeTools = toolSet("Read", "Grep", "glob", "finder", "Bash", "create_file", "edit_file", "get_diagnostics", "web_search", "read_web_page", "read_mcp_resource", "chart", "read_thread", "find_thread", "skill", "oracle", "handoff", "librarian", "Task", "task_list", "look_at", "painter", "shell_command", "apply_patch", "send_message_to_aggman")
)

type neoRuntime struct {
	mu      sync.RWMutex
	cfg     *config.Config
	host    string
	port    int
	server  *http.Server
	store   *neoActorStore
	started bool
	remote  context.CancelFunc
	cleanup context.CancelFunc
}

func newNeoRuntime(cfg *config.Config) *neoRuntime {
	host, port := neoRuntimeAddress(cfg)
	rt := &neoRuntime{
		cfg:   cfg,
		host:  host,
		port:  port,
		store: newNeoActorStore(),
	}
	rt.store.runtime = rt
	return rt
}

func neoRuntimeEnabled(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	if cfg.AmpCode.NeoLocalRuntime.Enabled == nil {
		return false
	}
	return *cfg.AmpCode.NeoLocalRuntime.Enabled
}

func neoRuntimeAddress(cfg *config.Config) (string, int) {
	host := strings.TrimSpace(cfg.AmpCode.NeoLocalRuntime.Host)
	if host == "" {
		host = defaultNeoRuntimeHost
	}
	port := cfg.AmpCode.NeoLocalRuntime.Port
	if port <= 0 {
		port = defaultNeoRuntimePort
	}
	return host, port
}

func (rt *neoRuntime) start() error {
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.started {
		return nil
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", rt.handleHTTP)
	rt.server = &http.Server{
		Addr:              net.JoinHostPort(rt.host, fmt.Sprintf("%d", rt.port)),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ctx, cleanup := context.WithCancel(context.Background())
	rt.cleanup = cleanup
	rt.started = true
	rt.ensureRemoteControlLoopLocked()

	go func() {
		log.Infof("amp neo local runtime listening on http://%s", rt.server.Addr)
		if err := rt.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Warnf("amp neo local runtime stopped: %v", err)
		}
	}()
	go rt.actorPruneLoop(ctx)

	return nil
}

func (rt *neoRuntime) stop(ctx context.Context) error {
	if rt == nil {
		return nil
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.started || rt.server == nil {
		return nil
	}
	rt.started = false
	if rt.remote != nil {
		rt.remote()
		rt.remote = nil
	}
	if rt.cleanup != nil {
		rt.cleanup()
		rt.cleanup = nil
	}
	return rt.server.Shutdown(ctx)
}

func (rt *neoRuntime) updateConfig(cfg *config.Config) error {
	if rt == nil || cfg == nil {
		return nil
	}
	rt.mu.Lock()
	rt.cfg = cfg
	if rt.started {
		rt.ensureRemoteControlLoopLocked()
	}
	rt.mu.Unlock()
	return nil
}

func (rt *neoRuntime) configSnapshot() *config.Config {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.cfg
}

func (rt *neoRuntime) ensureRemoteControlLoopLocked() {
	if rt == nil {
		return
	}
	if !neoRemoteControlEnabled(rt.cfg) {
		if rt.remote != nil {
			rt.remote()
			rt.remote = nil
		}
		return
	}
	if rt.remote != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt.remote = cancel
	go rt.remoteControlLoop(ctx)
}

func (rt *neoRuntime) actorPruneLoop(ctx context.Context) {
	ticker := time.NewTicker(neoActorPruneInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pruned := rt.store.pruneIdle(time.Now(), neoActorIdleTTL)
			if pruned > 0 {
				log.Debugf("amp neo local runtime pruned %d idle actor(s)", pruned)
			}
		}
	}
}

func (rt *neoRuntime) handleHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		rt.handleWebSocket(w, r)
		return
	}

	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/metadata":
		writeNeoJSON(w, http.StatusOK, map[string]any{
			"runtime":         "engine",
			"version":         "2.3.0-rc.4",
			"git_sha":         "local-cliproxyapi",
			"build_timestamp": "2026-05-07T00:00:00Z",
			"rustc_version":   "local",
			"rustc_host":      runtimeHost(),
			"cargo_target":    runtimeArch(),
			"cargo_profile":   "release",
		})
	case r.Method == http.MethodGet && rt.serveLocalThreadSearchHTTP(w, r):
		return
	case r.Method == http.MethodGet && rt.serveLocalThreadHTTP(w, r):
		return
	case r.URL.Path == "/actors" && r.Method == http.MethodGet:
		writeNeoJSON(w, http.StatusOK, map[string]any{"actors": rt.store.findActors(r.URL.Query())})
	case r.URL.Path == "/actors" && (r.Method == http.MethodPut || r.Method == http.MethodPost):
		body := readNeoJSON(r.Body)
		stored, created := rt.store.upsert(body, r.Method == http.MethodPut)
		writeNeoJSON(w, http.StatusOK, map[string]any{"actor": stored.record, "created": created})
	case r.URL.Path == "/request/import" && r.Method == http.MethodPost:
		rt.handleThreadImport(w, r)
	case isNeoSkillsPath(r.URL.Path):
		log.Debugf("amp neo local runtime HTTP skills method=%s path=%s rawQuery=%s", r.Method, r.URL.Path, r.URL.RawQuery)
		writeNeoJSON(w, http.StatusOK, rt.store.skillsResponse(neoActorIDFromSkillsPath(r.URL.Path), r.URL.Query()))
	case strings.HasPrefix(r.URL.Path, "/actors/") && r.Method == http.MethodDelete:
		rt.store.delete(strings.TrimPrefix(r.URL.Path, "/actors/"))
		w.WriteHeader(http.StatusNoContent)
	default:
		log.Debugf("amp neo local runtime HTTP not_found method=%s path=%s rawQuery=%s", r.Method, r.URL.Path, r.URL.RawQuery)
		writeNeoJSON(w, http.StatusNotFound, map[string]any{"error": "not_found"})
	}
}

func (rt *neoRuntime) serveLocalThreadSearchHTTP(w http.ResponseWriter, r *http.Request) bool {
	if rt == nil || r == nil {
		return false
	}
	if !neoThreadSearchPath(r.URL.Path) {
		return false
	}
	result, ok := neoThreadSearchResponse(r.Context(), rt.configSnapshot(), r.URL.Query())
	if !ok {
		return false
	}
	writeNeoJSON(w, http.StatusOK, result)
	return true
}

func (rt *neoRuntime) serveLocalThreadHTTP(w http.ResponseWriter, r *http.Request) bool {
	if rt == nil || r == nil {
		return false
	}
	threadID, markdown := neoThreadRequestPath(r.URL.Path)
	if threadID == "" {
		return false
	}
	thread, ok := loadNeoThread(r.Context(), rt.configSnapshot(), threadID)
	if !ok {
		return false
	}
	if markdown {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(neoThreadMarkdown(thread)))
		return true
	}
	writeNeoJSON(w, http.StatusOK, thread)
	return true
}

func (rt *neoRuntime) handleThreadImport(w http.ResponseWriter, r *http.Request) {
	body := readNeoJSON(r.Body)
	thread := mapValue(body["thread"])
	if len(thread) == 0 {
		writeNeoJSON(w, http.StatusBadRequest, map[string]any{"error": "missing_thread"})
		return
	}
	threadID := firstNonEmptyString(thread["id"], findThreadID(thread))
	if !neoThreadIDExactPattern.MatchString(threadID) {
		writeNeoJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid_thread_id"})
		return
	}

	actor := rt.store.ensureThreadActor(threadID)
	actor.touch()
	if err := actor.importThread(thread); err != nil {
		log.Warnf("amp neo local runtime thread import failed thread=%s: %v", threadID, err)
		writeNeoJSON(w, http.StatusBadRequest, map[string]any{"error": "import_failed", "message": err.Error()})
		return
	}
	writeNeoJSON(w, http.StatusOK, map[string]any{"ok": true, "threadId": threadID})
}

func runtimeHost() string {
	return runtime.GOOS
}

func runtimeArch() string {
	return runtime.GOARCH
}

func isNeoSkillsPath(path string) bool {
	path = strings.TrimSuffix(path, "/")
	if path == "/skills" || (strings.HasPrefix(path, "/actors/") && strings.HasSuffix(path, "/skills")) {
		return true
	}
	if strings.Contains(strings.ToLower(path), "skills") {
		return true
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	return len(parts) >= 3 && parts[len(parts)-2] == "request" && parts[len(parts)-1] == "skills"
}

func neoActorIDFromSkillsPath(path string) string {
	path = strings.Trim(path, "/")
	parts := strings.Split(path, "/")
	for i, part := range parts {
		if (part == "actors" || part == "gateway") && i+1 < len(parts) {
			return neoActorPathID(parts[i+1])
		}
	}
	for i := len(parts) - 1; i >= 0; i-- {
		if parts[i] != "" && parts[i] != "actors" && parts[i] != "gateway" && parts[i] != "request" && parts[i] != "skills" {
			return neoActorPathID(parts[i])
		}
	}
	return ""
}

func neoActorPathID(segment string) string {
	segment, _, _ = strings.Cut(segment, "@")
	if unescaped, err := url.PathUnescape(segment); err == nil {
		return unescaped
	}
	return segment
}

func (rt *neoRuntime) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	protocols := parseWebSocketProtocols(r.Header.Get("Sec-WebSocket-Protocol"))
	actorID := extractNeoActorID(protocols)
	if actorID == "" {
		actorID = firstNonEmptyString(r.URL.Query().Get("actorId"), r.Header.Get("x-rivet-actor"))
	}
	actor := rt.store.get(actorID)
	if actor == nil {
		actor = rt.store.actorForGatewayRequest(r)
		if actor == nil {
			http.Error(w, "Unknown local Neo actor", http.StatusNotFound)
			return
		}
	}

	responseHeader := http.Header{}
	if len(protocols) > 0 {
		responseHeader.Set("Sec-WebSocket-Protocol", protocols[0])
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	conn, err := upgrader.Upgrade(w, r, responseHeader)
	if err != nil {
		log.Warnf("amp neo websocket upgrade failed: %v", err)
		return
	}
	socket := &neoSocket{conn: conn}
	actor.open(socket)
	defer actor.close(socket)
	defer conn.Close()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if string(payload) == "ping" {
			socket.sendText("pong")
			continue
		}
		messages, err := decodeNeoClientFrame(payload)
		if err != nil {
			socket.send(map[string]any{"type": "error", "message": "Invalid Neo protocol message", "code": "PARSE_ERROR"})
			continue
		}
		for _, msg := range messages {
			actor.handleForSocket(socket, msg)
		}
	}
}

func decodeNeoClientFrame(payload []byte) ([]map[string]any, error) {
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, err
	}
	if msg, ok := decoded.(map[string]any); ok {
		return []map[string]any{msg}, nil
	}
	items, ok := decoded.([]any)
	if !ok {
		return nil, fmt.Errorf("invalid Neo protocol frame")
	}
	messages := make([]map[string]any, 0, len(items))
	for _, item := range items {
		msg, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("invalid Neo protocol batch item")
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

type neoActorStore struct {
	mu        sync.RWMutex
	runtime   *neoRuntime
	actors    map[string]*neoActor
	byNameKey map[string]string
}

func newNeoActorStore() *neoActorStore {
	return &neoActorStore{
		actors:    make(map[string]*neoActor),
		byNameKey: make(map[string]string),
	}
}

func (s *neoActorStore) get(id string) *neoActor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.actors[id]
}

func (s *neoActorStore) actorForGatewayRequest(r *http.Request) *neoActor {
	if s == nil || r == nil || r.URL == nil {
		return nil
	}
	q := r.URL.Query()
	target := neoGatewayTargetFromPath(r.URL.Path)
	key := strings.TrimSpace(q.Get("rvt-key"))

	s.mu.RLock()
	if target != "" {
		if actor := s.actors[target]; actor != nil {
			s.mu.RUnlock()
			return actor
		}
	}
	if key != "" {
		if id := s.byNameKey[target+"\x00"+key]; id != "" {
			if actor := s.actors[id]; actor != nil {
				s.mu.RUnlock()
				return actor
			}
		}
		for _, actor := range s.actors {
			if actor.key == key || actor.threadID == key {
				s.mu.RUnlock()
				return actor
			}
		}
	}
	s.mu.RUnlock()

	if !strings.EqualFold(q.Get("rvt-method"), "getOrCreate") || target == "" || key == "" {
		return nil
	}
	body := map[string]any{"name": target, "key": key}
	if input := decodeNeoGatewayInput(q.Get("rvt-input")); input != nil {
		body["input"] = input
	} else if threadID := neoThreadIDPattern.FindString(key); threadID != "" {
		body["input"] = map[string]any{"threadId": threadID}
	}
	actor, _ := s.upsert(body, true)
	return actor
}

func neoGatewayTargetFromPath(path string) string {
	if !strings.HasPrefix(path, "/gateway/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "/gateway/")
	segment, _, _ := strings.Cut(rest, "/")
	return neoActorPathID(segment)
}

func decodeNeoGatewayInput(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		data, err = base64.URLEncoding.DecodeString(value)
	}
	if err != nil {
		return nil
	}
	var decoded any
	if err := json.Unmarshal(data, &decoded); err != nil {
		return nil
	}
	return decoded
}

func (s *neoActorStore) threadActors(limit int) []*neoActor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	actors := make([]*neoActor, 0, len(s.actors))
	for _, actor := range s.actors {
		if actor != nil && neoThreadIDExactPattern.MatchString(actor.threadID) {
			actors = append(actors, actor)
		}
	}
	sort.Slice(actors, func(i, j int) bool {
		left := actors[i].lastUsedAt()
		right := actors[j].lastUsedAt()
		if !left.Equal(right) {
			return left.After(right)
		}
		return actors[i].threadID < actors[j].threadID
	})
	if limit > 0 && len(actors) > limit {
		actors = actors[:limit]
	}
	return actors
}

func (s *neoActorStore) skillsResponse(actorID string, q url.Values) map[string]any {
	actor := s.findActorForRequest(actorID, q)
	if actor == nil {
		return map[string]any{"ok": true, "skills": []any{}, "errors": []any{}}
	}
	return actor.skillsResponse()
}

func (s *neoActorStore) findActorForRequest(actorID string, q url.Values) *neoActor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if actorID != "" {
		if actor := s.actors[actorID]; actor != nil {
			return actor
		}
		for _, actor := range s.actors {
			if actor.threadID == actorID || actor.key == actorID {
				return actor
			}
		}
	}
	match := func(value string) *neoActor {
		for _, candidate := range strings.Split(value, ",") {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			for _, actor := range s.actors {
				if actor.threadID == candidate || actor.key == candidate || actor.id == candidate {
					return actor
				}
			}
		}
		return nil
	}
	for _, key := range []string{"threadID", "threadId", "thread_id", "key", "rvt-key", "actorID", "actorId", "actor_id"} {
		if value := strings.TrimSpace(q.Get(key)); value != "" {
			if actor := match(value); actor != nil {
				return actor
			}
		}
	}
	for _, values := range q {
		for _, value := range values {
			if actor := match(value); actor != nil {
				return actor
			}
		}
	}
	var latest *neoActor
	var latestCreated string
	for _, actor := range s.actors {
		created := stringValue(actor.record["create_ts"])
		if latest == nil || created > latestCreated {
			latest = actor
			latestCreated = created
		}
	}
	return latest
}

func (s *neoActorStore) findActors(q url.Values) []map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if ids := strings.TrimSpace(q.Get("actor_ids")); ids != "" {
		out := make([]map[string]any, 0)
		for _, id := range strings.Split(ids, ",") {
			if actor := s.actors[strings.TrimSpace(id)]; actor != nil {
				out = append(out, actor.record)
			}
		}
		return out
	}

	name := q.Get("name")
	key, hasKey := q["key"]
	if name != "" && hasKey {
		id := s.byNameKey[name+"\x00"+firstString(key)]
		if actor := s.actors[id]; actor != nil {
			return []map[string]any{actor.record}
		}
		return nil
	}

	out := make([]map[string]any, 0, len(s.actors))
	for _, actor := range s.actors {
		if name == "" || actor.name == name {
			out = append(out, actor.record)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["create_ts"]) < fmt.Sprint(out[j]["create_ts"])
	})
	return out
}

func (s *neoActorStore) upsert(body map[string]any, reuse bool) (*neoActor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	name := stringValue(body["name"])
	if name == "" {
		name = "thread-actor"
	}
	key := stringValue(body["key"])
	if reuse && key != "" {
		if id := s.byNameKey[name+"\x00"+key]; id != "" {
			if existing := s.actors[id]; existing != nil {
				existing.touch()
				return existing, false
			}
		}
	}

	id := "actor-" + randomBase62(22)
	threadID := extractThreadIDFromActorBody(body, key)
	record := neoActorRecord(id, name, key)
	actor := newNeoActor(s.runtime, id, name, key, threadID, record, body)
	s.actors[id] = actor
	if key != "" {
		s.byNameKey[name+"\x00"+key] = id
	}
	return actor, true
}

func (s *neoActorStore) ensureThreadActor(threadID string) *neoActor {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, actor := range s.actors {
		if actor.threadID == threadID || actor.key == threadID {
			return actor
		}
	}

	id := "actor-" + randomBase62(22)
	name := "thread-actor"
	record := neoActorRecord(id, name, threadID)
	actor := newNeoActor(s.runtime, id, name, threadID, threadID, record, map[string]any{"input": map[string]any{"threadId": threadID}})
	s.actors[id] = actor
	s.byNameKey[name+"\x00"+threadID] = id
	return actor
}

func (s *neoActorStore) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if actor := s.actors[id]; actor != nil {
		actor.dispose()
		if actor.key != "" {
			delete(s.byNameKey, actor.name+"\x00"+actor.key)
		}
	}
	delete(s.actors, id)
}

func (s *neoActorStore) pruneIdle(now time.Time, ttl time.Duration) int {
	if s == nil || ttl <= 0 {
		return 0
	}
	s.mu.Lock()
	stale := make([]*neoActor, 0)
	for id, actor := range s.actors {
		if actor == nil || !actor.prunable(now, ttl) {
			continue
		}
		stale = append(stale, actor)
		delete(s.actors, id)
		if actor.key != "" {
			delete(s.byNameKey, actor.name+"\x00"+actor.key)
		}
	}
	s.mu.Unlock()

	for _, actor := range stale {
		actor.dispose()
	}
	return len(stale)
}

type neoActor struct {
	mu                        sync.Mutex
	runtime                   *neoRuntime
	id                        string
	name                      string
	key                       string
	threadID                  string
	record                    map[string]any
	settings                  map[string]any
	environment               map[string]any
	capabilities              map[string]any
	guidanceSnapshot          map[string]any
	tools                     map[string]neoToolSpec
	skillSnapshot             map[string]any
	messages                  []neoMessage
	history                   []neoHistoryMessage
	queue                     []neoQueuedMessage
	pendingTools              map[string]neoPendingTool
	approvalQueue             []map[string]any
	sockets                   map[*neoSocket]struct{}
	spawnedExecutors          map[string]*neoSpawnedExecutor
	artifacts                 map[string]any
	notificationSubs          map[string]map[string]any
	remoteSeenMessages        map[string]bool
	remoteControlStarted      bool
	lastUsed                  time.Time
	syncRunning               bool
	syncPending               bool
	title                     string
	titleSource               string
	titleGenerationStarted    bool
	threadStatus              string
	compacting                bool
	compactionRecords         []map[string]any
	retryScheduled            bool
	replayEvents              []neoReplayEvent
	activeError               map[string]any
	activeErrorSeq            int
	seq                       int
	agentState                string
	executorID                string
	executorReady             bool
	executorBootstrapComplete bool
	executorResumeBootstrap   bool
	currentAgentMode          string
	currentReasoningEffort    string
	generation                int
}

type neoReplayEvent struct {
	Seq     int
	Payload map[string]any
}

type neoSpawnedExecutor struct {
	spawnID   string
	threadID  string
	command   string
	logPath   string
	cmd       *exec.Cmd
	startedAt time.Time
}

func (e *neoSpawnedExecutor) pid() int {
	if e == nil || e.cmd == nil || e.cmd.Process == nil {
		return 0
	}
	return e.cmd.Process.Pid
}

func (e *neoSpawnedExecutor) stop() {
	if e == nil || e.cmd == nil || e.cmd.Process == nil {
		return
	}
	_ = e.cmd.Process.Kill()
}

func newNeoActor(rt *neoRuntime, id, name, key, threadID string, record map[string]any, input map[string]any) *neoActor {
	settings := map[string]any{}
	if nested, ok := asMap(input["input"]); ok {
		if mode := stringValue(nested["agentMode"]); mode != "" {
			settings["agentMode"] = mode
		}
	}
	agentMode := stringValue(settings["agentMode"])
	if agentMode == "" {
		agentMode = "smart"
	}
	return &neoActor{
		runtime:                rt,
		id:                     id,
		name:                   name,
		key:                    key,
		threadID:               threadID,
		record:                 record,
		settings:               settings,
		environment:            map[string]any{},
		capabilities:           map[string]any{},
		guidanceSnapshot:       map[string]any{},
		tools:                  map[string]neoToolSpec{},
		skillSnapshot:          map[string]any{},
		pendingTools:           map[string]neoPendingTool{},
		sockets:                map[*neoSocket]struct{}{},
		spawnedExecutors:       map[string]*neoSpawnedExecutor{},
		artifacts:              map[string]any{},
		notificationSubs:       map[string]map[string]any{},
		remoteSeenMessages:     map[string]bool{},
		lastUsed:               time.Now(),
		seq:                    1,
		agentState:             "idle",
		currentAgentMode:       agentMode,
		currentReasoningEffort: defaultNeoReasoningEffort(agentMode),
	}
}

func (a *neoActor) open(socket *neoSocket) {
	a.mu.Lock()
	a.touchLocked()
	a.sockets[socket] = struct{}{}
	a.mu.Unlock()
	a.sendSnapshot(socket, 0)
	a.broadcastObservers()
}

func (a *neoActor) close(socket *neoSocket) {
	a.mu.Lock()
	delete(a.sockets, socket)
	a.mu.Unlock()
	a.broadcastObservers()
}

func (a *neoActor) dispose() {
	a.mu.Lock()
	sockets := a.socketListLocked()
	a.sockets = map[*neoSocket]struct{}{}
	executors := a.spawnedExecutorListLocked()
	a.spawnedExecutors = map[string]*neoSpawnedExecutor{}
	a.mu.Unlock()
	for _, socket := range sockets {
		_ = socket.conn.Close()
	}
	for _, executor := range executors {
		executor.stop()
	}
}

func (a *neoActor) touch() {
	a.mu.Lock()
	a.touchLocked()
	a.mu.Unlock()
}

func (a *neoActor) touchLocked() {
	a.lastUsed = time.Now()
}

func (a *neoActor) lastUsedAt() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastUsed
}

func (a *neoActor) prunable(now time.Time, ttl time.Duration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastUsed.IsZero() || now.Sub(a.lastUsed) < ttl {
		return false
	}
	if len(a.sockets) > 0 || a.executorID != "" || a.agentState != "idle" || a.syncRunning || a.syncPending {
		return false
	}
	if len(a.pendingTools) > 0 || len(a.approvalQueue) > 0 || len(a.queue) > 0 || len(a.spawnedExecutors) > 0 {
		return false
	}
	return true
}

func (a *neoActor) handle(msg map[string]any) {
	a.handleForSocket(nil, msg)
}

func (a *neoActor) handleForSocket(socket *neoSocket, msg map[string]any) {
	a.touch()
	msgType := stringValue(msg["type"])
	log.Debugf("amp neo local runtime WS recv %s", msgType)

	switch msgType {
	case "client_resume":
		a.sendSnapshot(socket, intValue(msg["version"]))
	case "client_update_thread_settings":
		a.updateSettings(mapValue(msg["settings"]))
	case "executor_connect":
		a.executorConnect(msg)
	case "executor_environment_snapshot", "executor_environment_update":
		a.updateEnvironment(mapValue(msg["environment"]))
	case "executor_guidance_snapshot", "executor_guidance_update", "executor_guidance_discovery":
		a.updateGuidanceSnapshot(msg)
	case "executor_skill_snapshot":
		a.updateSkillSnapshot(msg)
	case "executor_tools_register":
		a.registerTools(msg["tools"])
	case "executor_tools_unregister":
		a.unregisterTools(msg["toolNames"])
	case "executor_tools_bootstrap_complete":
		a.executorToolsBootstrapComplete(msg)
	case "executor_tool_lease_ack":
		return
	case "executor_connected":
		a.executorConnected(msg)
	case "executor_disconnected":
		a.executorDisconnected(msg)
	case "executor_connect_rejected":
		a.executorConnectRejected(msg)
	case "executor_status":
		a.broadcast(normalizeNeoExecutorStatus(msg))
	case "executor_error":
		a.broadcast(normalizeNeoExecutorError(msg))
	case "client_append_user_msg":
		a.receiveUserMessage(msg)
	case "client_edit_message":
		a.editMessage(msg)
	case "executor_tool_result":
		a.receiveToolResult(msg)
	case "executor_tool_result_ack":
		a.broadcast(normalizeNeoToolResultAck(msg))
	case "executor_tool_lease_revoked":
		a.revokeToolLease(msg)
	case "tool_progress":
		a.handleToolProgress(msg)
	case "executor_tool_approval_request":
		a.handleToolApprovalRequest(msg)
	case "client_tool_approval_response":
		a.handleToolApprovalResponse(msg)
	case "executor_tool_approval_response":
		a.handleToolApprovalResponse(msg)
	case "client_filesystem_read_directory":
		a.broadcast(map[string]any{"type": "executor_filesystem_read_directory", "requestId": msg["requestId"], "uri": msg["uri"]})
	case "client_filesystem_read_file":
		a.broadcast(map[string]any{"type": "executor_filesystem_read_file", "requestId": msg["requestId"], "uri": msg["uri"]})
	case "executor_filesystem_read_directory":
		a.broadcast(map[string]any{"type": "executor_filesystem_read_directory", "requestId": msg["requestId"], "uri": msg["uri"]})
	case "executor_filesystem_read_file":
		a.broadcast(map[string]any{"type": "executor_filesystem_read_file", "requestId": msg["requestId"], "uri": msg["uri"]})
	case "executor_filesystem_read_directory_result":
		msg["type"] = "client_filesystem_read_directory_result"
		a.broadcast(msg)
	case "executor_filesystem_read_file_result":
		msg["type"] = "client_filesystem_read_file_result"
		a.broadcast(msg)
	case "client_filesystem_read_directory_result", "client_filesystem_read_file_result":
		a.broadcast(msg)
	case "executor_plugin_message":
		a.broadcast(map[string]any{"type": "plugin_message", "message": msg["message"]})
	case "executor_artifact_upsert":
		a.upsertArtifact(msg["artifact"])
	case "executor_artifact_delete":
		a.deleteArtifact(stringValue(msg["key"]))
	case "thread_status":
		a.updateThreadStatus(msg)
	case "compaction_started", "compaction_complete", "compaction_records":
		a.handleCompactionEvent(msg)
	case "retry_scheduled", "retry_started", "retry_cancelled":
		a.handleRetryEvent(msg)
	case "client_cancel":
		a.cancel()
	case "client_remove_queued_msg":
		a.removeQueuedMessage(stringValue(msg["queuedMessageId"]))
	case "client_steer_queued_msg":
		a.steerQueuedMessage(stringValue(msg["queuedMessageId"]))
	case "client_set_thread_title":
		a.setTitle(stringValue(msg["title"]))
	case "client_dismiss_active_error":
		a.clearActiveError(msg)
	case "client_mark_message_read":
		a.markMessageRead(stringValue(msg["messageId"]), true)
	case "client_mark_message_unread":
		a.markMessageRead(stringValue(msg["messageId"]), false)
	case "client_upsert_notification_subscription":
		a.upsertNotificationSubscription(msg)
	case "client_spawn_executor":
		a.spawnExecutor(msg)
	case "client_append_manual_bash_invocation":
		message := a.storeMessage(neoMessage{ThreadID: a.threadID, MessageID: newNeoMessageID(), Role: "info", Content: []any{map[string]any{"type": "manual_bash_invocation", "args": mapValue(msg["args"]), "run": mapValue(msg["run"]), "hidden": boolValue(msg["hidden"])}}})
		a.broadcast(neoMessageAddedPayload(message))
	case "client_retry":
		a.retry()
	default:
		log.Debugf("amp neo local runtime ignored message %s", msgType)
	}
}

func (a *neoActor) updateSettings(settings map[string]any) {
	a.mu.Lock()
	if a.settings == nil {
		a.settings = map[string]any{}
	}
	for key, value := range settings {
		a.settings[key] = value
	}
	if mode := stringValue(settings["agentMode"]); mode != "" {
		a.currentAgentMode = mode
		if effort := stringValue(settings["reasoning.effort"]); neoReasoningEffortAllowedForMode(mode, effort) {
			a.currentReasoningEffort = effort
		} else {
			a.currentReasoningEffort = defaultNeoReasoningEffort(mode)
			if a.currentReasoningEffort == "" {
				delete(a.settings, "reasoning.effort")
			} else {
				a.settings["reasoning.effort"] = a.currentReasoningEffort
			}
		}
	} else if a.currentAgentMode == "" {
		a.currentAgentMode = a.agentModeLocked()
	} else if effort := stringValue(settings["reasoning.effort"]); effort != "" {
		a.currentReasoningEffort = effort
	}
	merged := cloneMap(a.settings)
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "thread_settings", "settings": merged})
}

func (a *neoActor) toolProgressPayload(msg map[string]any) map[string]any {
	out := cloneMap(msg)
	if stringValue(out["parentToolCallId"]) != "" {
		return out
	}
	toolCallID := stringValue(out["toolCallId"])
	if toolCallID == "" {
		return out
	}
	a.mu.Lock()
	pending := a.pendingTools[toolCallID]
	a.mu.Unlock()
	return withNeoParentToolCallID(out, pending.ParentToolCallID)
}

func (a *neoActor) handleToolProgress(msg map[string]any) {
	payload := a.toolProgressPayload(msg)
	toolCallID := stringValue(payload["toolCallId"])
	if toolCallID == "" {
		a.broadcast(payload)
		return
	}

	a.mu.Lock()
	pending := a.pendingTools[toolCallID]
	parentToolCallID := firstNonEmptyString(payload["parentToolCallId"], pending.ParentToolCallID)
	if parentToolCallID != "" {
		payload["parentToolCallId"] = parentToolCallID
	}
	existingRun, userInput := a.toolResultRunLocked(toolCallID)
	if neoToolRunTerminal(existingRun) {
		a.mu.Unlock()
		a.broadcast(payload)
		return
	}
	run, ok := neoToolProgressRun(payload["progress"], existingRun)
	if !ok {
		a.mu.Unlock()
		a.broadcast(payload)
		return
	}
	block := map[string]any{"type": "tool_result", "toolUseID": toolCallID, "run": run}
	if userInput != nil {
		block["userInput"] = userInput
	}
	_, event := a.storeMessageEventLocked(neoMessage{
		ThreadID:         a.threadID,
		Role:             "user",
		MessageID:        toolResultMessageID(toolCallID),
		Content:          []any{block},
		CreatedAt:        time.Now().UTC().Format(time.RFC3339Nano),
		ParentToolUseID:  parentToolCallID,
		CompletionStatus: "tool_progress",
	})
	a.mu.Unlock()

	a.broadcast(payload)
	a.broadcast(event)
	a.syncCloudAsync()
}

func (a *neoActor) handleToolApprovalRequest(msg map[string]any) {
	a.mu.Lock()
	approval, ok := a.normalizeToolApprovalRequestLocked(msg)
	if !ok {
		a.mu.Unlock()
		a.broadcast(toolApprovalQueuePayload([]any{cloneMap(mapValue(msg["approval"]))}))
		return
	}
	a.upsertApprovalLocked(approval)
	approvals := a.approvalQueueListLocked()
	stateChanged := a.agentState != "awaiting_approval"
	a.agentState = "awaiting_approval"
	agentMode := a.currentAgentMode
	reasoningEffort := a.currentReasoningEffort
	a.mu.Unlock()

	a.broadcast(toolApprovalQueuePayload(approvals))
	if stateChanged {
		a.broadcast(map[string]any{"type": "agent_state", "state": "awaiting_approval", "agentMode": agentMode, "reasoningEffort": omitEmpty(reasoningEffort)})
	}
}

func (a *neoActor) handleToolApprovalResponse(msg map[string]any) {
	payload := normalizeNeoToolApprovalResponse(msg)
	toolCallID := firstNonEmptyString(payload["toolCallId"], msg["toolUseId"], msg["id"])
	a.mu.Lock()
	removed := a.removeApprovalLocked(toolCallID)
	approvals := a.approvalQueueListLocked()
	stateChanged := false
	state := a.agentState
	if removed && len(approvals) == 0 && a.agentState == "awaiting_approval" {
		if len(a.pendingTools) > 0 {
			state = "running_tools"
		} else {
			state = "idle"
		}
		a.agentState = state
		stateChanged = true
	}
	agentMode := a.currentAgentMode
	reasoningEffort := a.currentReasoningEffort
	a.mu.Unlock()

	a.broadcast(payload)
	if removed {
		a.broadcast(toolApprovalQueuePayload(approvals))
	}
	if stateChanged {
		a.broadcast(map[string]any{"type": "agent_state", "state": state, "agentMode": agentMode, "reasoningEffort": omitEmpty(reasoningEffort)})
	}
}

func (a *neoActor) executorConnect(msg map[string]any) {
	a.mu.Lock()
	previousExecutorID := a.executorID
	clientID := fallbackString(msg["clientId"], a.executorID)
	resumeBootstrap := a.executorBootstrapComplete && (previousExecutorID == "" || clientID == "" || previousExecutorID == clientID)
	a.executorID = clientID
	a.executorReady = false
	a.executorResumeBootstrap = resumeBootstrap
	if !resumeBootstrap {
		a.executorBootstrapComplete = false
		a.tools = map[string]neoToolSpec{}
		a.guidanceSnapshot = map[string]any{}
		a.skillSnapshot = map[string]any{}
	}
	a.capabilities = mapValue(msg["capabilities"])
	if env, ok := asMap(mapValue(msg["capabilities"])["environment"]); ok {
		for k, v := range env {
			a.environment[k] = v
		}
	}
	a.mu.Unlock()
	a.sendExecutorConnected(nil, resumeBootstrap)
	a.broadcastObservers()
}

func (a *neoActor) executorToolsBootstrapComplete(msg map[string]any) {
	if ok, exists := msg["ok"].(bool); exists && !ok {
		a.fail(fmt.Errorf("%s", fallbackString(msg["error"], "Executor bootstrap failed")))
		return
	}
	a.mu.Lock()
	a.executorReady = true
	a.executorBootstrapComplete = true
	resumeBootstrap := a.executorResumeBootstrap
	a.executorResumeBootstrap = false
	a.mu.Unlock()
	a.sendExecutorConnected(nil, resumeBootstrap)
	a.processQueue()
}

func (a *neoActor) executorConnected(msg map[string]any) {
	a.mu.Lock()
	executorID := firstNonEmptyString(msg["executorId"], msg["clientId"], a.executorID)
	if executorID == "" {
		executorID = "local-executor"
	}
	a.executorID = executorID
	a.executorReady = true
	a.executorBootstrapComplete = true
	a.executorResumeBootstrap = false
	registeredToolCount := numberFrom(msg["registeredToolCount"], len(a.tools))
	guidanceInventory := neoGuidanceInventory(a.guidanceSnapshot)
	a.mu.Unlock()

	payload := cloneMap(msg)
	payload["type"] = "executor_connected"
	payload["executorId"] = executorID
	payload["registeredToolCount"] = registeredToolCount
	if _, exists := payload["guidanceInventory"]; !exists {
		payload["guidanceInventory"] = guidanceInventory
	}
	a.broadcast(payload)
	a.broadcastObservers()
	a.processQueue()
}

func (a *neoActor) executorDisconnected(msg map[string]any) {
	a.mu.Lock()
	a.executorReady = false
	a.executorID = ""
	pending := a.pendingToolIDsLocked()
	updateEvents := a.cancelToolResultMessagesLocked(pending, "system:disposed")
	a.pendingTools = map[string]neoPendingTool{}
	hadApprovals := len(a.approvalQueue) > 0
	a.approvalQueue = nil
	a.agentState = "idle"
	a.mu.Unlock()

	for _, toolCallID := range pending {
		a.broadcast(map[string]any{"type": "executor_tool_lease_revoked", "toolCallId": toolCallID, "reason": "executor_disconnected"})
	}
	if hadApprovals {
		a.broadcast(toolApprovalQueuePayload(nil))
	}
	for _, event := range updateEvents {
		a.broadcast(event)
	}
	a.broadcast(normalizeNeoExecutorStatus(map[string]any{
		"type":    "executor_status",
		"spawnId": firstNonEmptyString(msg["spawnId"], msg["requestId"]),
		"status":  "failed",
		"message": fallbackString(msg["message"], "Executor disconnected"),
		"details": map[string]any{"reasonCode": "executor_disconnected"},
	}))
	a.broadcastObservers()
	a.processQueue()
}

func (a *neoActor) executorConnectRejected(msg map[string]any) {
	a.mu.Lock()
	a.executorReady = false
	a.mu.Unlock()
	a.broadcast(normalizeNeoExecutorStatus(map[string]any{
		"type":    "executor_status",
		"spawnId": firstNonEmptyString(msg["spawnId"], msg["requestId"]),
		"status":  "failed",
		"message": fallbackString(msg["message"], "Executor connect rejected"),
		"details": map[string]any{"reasonCode": "executor_connect_rejected"},
	}))
	a.broadcastObservers()
}

func (a *neoActor) spawnExecutor(msg map[string]any) {
	spawnID := firstNonEmptyString(msg["spawnId"], msg["requestId"])
	if spawnID == "" {
		spawnID = "spawn-" + randomBase62(12)
	}

	a.mu.Lock()
	threadID := firstNonEmptyString(msg["threadID"], msg["threadId"], msg["thread_id"], a.threadID, a.key)
	environment := cloneMap(a.environment)
	if env := mapValue(msg["environment"]); len(env) > 0 {
		for key, value := range env {
			environment[key] = value
		}
	}
	ready := a.executorReady
	executorID := a.executorID
	var existing *neoSpawnedExecutor
	for _, spawned := range a.spawnedExecutors {
		if spawned != nil && spawned.threadID == threadID {
			existing = spawned
			break
		}
	}
	a.mu.Unlock()

	if !neoThreadIDExactPattern.MatchString(threadID) {
		a.broadcastExecutorStatus(spawnID, "failed", "Cannot spawn Amp headless executor without a valid thread ID.", map[string]any{"reasonCode": "environment_missing"})
		return
	}
	if ready {
		a.broadcastExecutorStatus(spawnID, "running", "Executor is already connected.", map[string]any{"reasonCode": "executor_connected", "executorId": executorID})
		return
	}
	if existing != nil {
		a.broadcastExecutorStatus(spawnID, "running", "Headless executor is already starting for this thread.", map[string]any{"reasonCode": "waiting_for_executor_connect", "pid": existing.pid(), "threadId": threadID})
		return
	}

	cfg := a.runtime.configSnapshot()
	command, err := neoAmpExecutorCommand(cfg)
	if err != nil {
		a.broadcastExecutorStatus(spawnID, "failed", err.Error(), map[string]any{"reasonCode": "spawn_failed"})
		return
	}

	workDir := neoHeadlessWorkingDirectory(msg, environment)
	logPath := neoHeadlessExecutorLogPath(threadID, spawnID)
	cmd := exec.Command(command, "--headless", threadID)
	if workDir != "" {
		cmd.Dir = workDir
	}
	cmd.Env = neoHeadlessExecutorEnv(os.Environ(), cfg, threadID, workDir, logPath)

	var logFile *os.File
	if logPath != "" {
		if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err == nil {
			logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
			if err != nil {
				log.Warnf("amp neo local runtime failed to open headless log file %s: %v", logPath, err)
			}
		}
	}
	if logFile != nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	} else {
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
	}

	a.broadcastExecutorStatus(spawnID, "starting", "Starting local Amp headless executor.", map[string]any{"reasonCode": "spawn_requested", "threadId": threadID, "command": command})
	if err := cmd.Start(); err != nil {
		if logFile != nil {
			_ = logFile.Close()
		}
		a.broadcastExecutorStatus(spawnID, "failed", "Failed to start local Amp headless executor: "+err.Error(), map[string]any{"reasonCode": "spawn_failed", "command": command})
		return
	}

	spawned := &neoSpawnedExecutor{
		spawnID:   spawnID,
		threadID:  threadID,
		command:   command,
		logPath:   logPath,
		cmd:       cmd,
		startedAt: time.Now(),
	}
	a.mu.Lock()
	if a.spawnedExecutors == nil {
		a.spawnedExecutors = map[string]*neoSpawnedExecutor{}
	}
	a.spawnedExecutors[spawnID] = spawned
	a.mu.Unlock()

	a.broadcastExecutorStatus(spawnID, "running", "Waiting for local Amp headless executor to connect.", map[string]any{"reasonCode": "waiting_for_executor_connect", "pid": spawned.pid(), "threadId": threadID, "logFile": omitEmpty(logPath)})
	go a.waitSpawnedExecutor(spawnID, spawned, logFile)
	go a.watchSpawnedExecutorConnectTimeout(spawnID, spawned, neoExecutorConnectTimeout(cfg))
}

func (a *neoActor) waitSpawnedExecutor(spawnID string, spawned *neoSpawnedExecutor, logFile *os.File) {
	err := spawned.cmd.Wait()
	if logFile != nil {
		_ = logFile.Close()
	}

	a.mu.Lock()
	current := a.spawnedExecutors[spawnID]
	if current == spawned {
		delete(a.spawnedExecutors, spawnID)
	}
	ready := a.executorReady
	a.mu.Unlock()

	if current != spawned {
		return
	}
	if !ready {
		message := "Local Amp headless executor exited before connecting."
		if err != nil {
			message = "Local Amp headless executor exited before connecting: " + err.Error()
		}
		a.broadcastExecutorStatus(spawnID, "failed", message, map[string]any{"reasonCode": "spawn_failed", "threadId": spawned.threadID, "pid": spawned.pid(), "logFile": omitEmpty(spawned.logPath)})
		return
	}
	if err != nil {
		log.Warnf("amp neo local runtime headless executor exited thread=%s pid=%d: %v", spawned.threadID, spawned.pid(), err)
	}
}

func (a *neoActor) watchSpawnedExecutorConnectTimeout(spawnID string, spawned *neoSpawnedExecutor, timeout time.Duration) {
	if timeout <= 0 || spawned == nil {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	<-timer.C

	a.mu.Lock()
	current := a.spawnedExecutors[spawnID]
	ready := a.executorReady
	if current != spawned || ready {
		a.mu.Unlock()
		return
	}
	delete(a.spawnedExecutors, spawnID)
	a.mu.Unlock()

	spawned.stop()
	a.broadcastExecutorStatus(spawnID, "failed", "Local Amp headless executor did not connect before the timeout.", map[string]any{"reasonCode": "connect_timeout", "threadId": spawned.threadID, "pid": spawned.pid(), "timeoutSeconds": int(timeout.Seconds()), "logFile": omitEmpty(spawned.logPath)})
}

func neoExecutorConnectTimeout(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.AmpCode.NeoLocalRuntime.ExecutorConnectTimeoutSeconds > 0 {
		return time.Duration(cfg.AmpCode.NeoLocalRuntime.ExecutorConnectTimeoutSeconds) * time.Second
	}
	return defaultNeoExecutorConnectTimeout
}

func (a *neoActor) broadcastExecutorStatus(spawnID, status, message string, details map[string]any) {
	a.broadcast(normalizeNeoExecutorStatus(map[string]any{
		"type":    "executor_status",
		"spawnId": spawnID,
		"status":  status,
		"message": message,
		"details": details,
	}))
}

func (a *neoActor) updateEnvironment(environment map[string]any) {
	a.mu.Lock()
	a.environment = environment
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "environment_update", "environment": environment})
}

func (a *neoActor) updateGuidanceSnapshot(msg map[string]any) {
	snapshot := cloneMap(msg)
	delete(snapshot, "type")
	if len(snapshot) == 0 {
		return
	}

	a.mu.Lock()
	for key, value := range snapshot {
		a.guidanceSnapshot[key] = value
	}
	textLen := len(neoGuidanceText(a.guidanceSnapshot))
	inventoryLen := len(neoGuidanceInventory(a.guidanceSnapshot))
	a.mu.Unlock()

	log.Debugf("amp neo local runtime guidance snapshot text_len=%d inventory=%d files=%d keys=%s", textLen, inventoryLen, len(neoGuidanceFiles(a.guidanceSnapshot)), strings.Join(sortedMapKeys(snapshot), ","))
}

func (a *neoActor) updateSkillSnapshot(msg map[string]any) {
	snapshot := cloneMap(msg)
	delete(snapshot, "type")
	snapshotID := stringValue(snapshot["snapshotId"])
	skills := firstArray(snapshot["skills"], snapshot["skillInventory"])
	errors := arrayValue(snapshot["errors"])

	a.mu.Lock()
	if snapshotID != "" && snapshotID != stringValue(a.skillSnapshot["snapshotId"]) {
		a.skillSnapshot = map[string]any{"snapshotId": snapshotID, "skills": []any{}, "errors": []any{}}
	}
	for key, value := range snapshot {
		if key != "skills" && key != "skillInventory" && key != "errors" {
			a.skillSnapshot[key] = value
		}
	}
	if skills != nil {
		prior := firstArray(a.skillSnapshot["skills"], a.skillSnapshot["skillInventory"])
		combined := make([]any, 0, len(prior)+len(skills))
		combined = append(combined, prior...)
		combined = append(combined, skills...)
		a.skillSnapshot["skills"] = combined
		a.capabilities["skills"] = combined
	}
	if errors != nil {
		a.skillSnapshot["errors"] = errors
	}
	if names := neoSkillNamesFromAny(a.skillSnapshot["skills"]); len(names) > 0 {
		a.capabilities["skillNames"] = names
	}
	totalSkills := len(firstArray(a.skillSnapshot["skills"], a.skillSnapshot["skillInventory"]))
	a.mu.Unlock()

	log.Debugf("amp neo local runtime skills snapshot skills=%d chunk=%d errors=%d", totalSkills, len(skills), len(errors))
}

func (a *neoActor) skillsResponse() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()

	skills := firstArray(a.skillSnapshot["skills"], a.skillSnapshot["skillInventory"])
	if skills == nil {
		skills = []any{}
	}
	errors := arrayValue(a.skillSnapshot["errors"])
	if errors == nil {
		errors = []any{}
	}
	return map[string]any{"ok": true, "skills": skills, "errors": errors}
}

func (a *neoActor) registerTools(raw any) {
	tools, _ := raw.([]any)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, item := range tools {
		m := mapValue(item)
		name := stringValue(m["name"])
		if name == "" {
			continue
		}
		a.tools[name] = neoToolSpec{
			Name:        name,
			Description: stringValue(m["description"]),
			InputSchema: firstMap(m["inputSchema"], m["input_schema"], m["parameters"]),
			Meta:        mapValue(m["meta"]),
		}
	}
}

func (a *neoActor) unregisterTools(raw any) {
	names, _ := raw.([]any)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, name := range names {
		delete(a.tools, stringValue(name))
	}
}

func (a *neoActor) receiveUserMessage(msg map[string]any) {
	user := neoQueuedMessage{
		MessageID:       fallbackString(msg["messageId"], newNeoMessageID()),
		Content:         arrayValue(msg["content"]),
		UserState:       msg["userState"],
		Meta:            mapValue(msg["meta"]),
		CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		AgentMode:       stringValue(msg["agentMode"]),
		ReasoningEffort: stringValue(msg["reasoningEffort"]),
		Steer:           boolValue(msg["steer"]),
	}
	if len(user.Content) == 0 {
		user.Content = []any{map[string]any{"type": "text", "text": fmt.Sprint(msg["content"])}}
	}

	a.mu.Lock()
	a.touchLocked()
	if a.agentState != "idle" || !a.executorReady {
		if user.Steer {
			a.queue = append([]neoQueuedMessage{user}, a.queue...)
		} else {
			a.queue = append(a.queue, user)
		}
		seq := a.nextSeqLocked()
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "queued_message_added", "message": map[string]any{"steer": user.Steer, "queuedMessage": user.protocol()}, "seq": seq})
		return
	}
	a.mu.Unlock()
	a.startUserMessage(user)
}

func (a *neoActor) editMessage(msg map[string]any) {
	messageID := stringValue(msg["messageId"])
	editID := stringValue(msg["editId"])
	content := arrayValue(msg["content"])
	if content == nil {
		content = []any{}
	}
	if messageID == "" {
		a.rejectEdit(editID, "Missing messageId")
		return
	}

	a.mu.Lock()
	index := -1
	for i, message := range a.messages {
		if message.MessageID == messageID {
			index = i
			break
		}
	}
	if index < 0 {
		a.mu.Unlock()
		a.rejectEdit(editID, "Message not found")
		return
	}
	if a.messages[index].Role != "user" {
		a.mu.Unlock()
		a.rejectEdit(editID, "Only user messages can be edited")
		return
	}

	a.generation++
	updated := a.messages[index]
	updated.Content = content
	if mode := stringValue(msg["agentMode"]); mode != "" {
		updated.AgentMode = mode
	}
	if effort := stringValue(msg["reasoningEffort"]); effort != "" {
		updated.ReasoningEffort = effort
	}
	if updated.CreatedAt == "" {
		updated.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	var truncateFromMessage string
	if index+1 < len(a.messages) {
		truncateFromMessage = a.messages[index+1].MessageID
	}
	a.messages = append(a.messages[:index], updated)
	a.rebuildHistoryLocked()
	a.pendingTools = map[string]neoPendingTool{}
	a.agentState = "idle"
	if updated.Seq == 0 {
		updated.Seq = a.nextSeqLocked()
		a.messages[len(a.messages)-1] = updated
	}
	updateSeq := a.nextSeqLocked()
	updateEvent := map[string]any{"type": "message_updated", "message": updated.protocol(), "seq": updateSeq}
	a.rememberReplayEventLocked(updateEvent)
	var truncateEvent map[string]any
	if truncateFromMessage != "" {
		truncateSeq := a.nextSeqLocked()
		truncateEvent = map[string]any{"type": "thread_truncated", "seq": truncateSeq, "truncateFromMessage": truncateFromMessage}
		a.rememberReplayEventLocked(truncateEvent)
	}
	ready := a.executorReady
	mode := updated.AgentMode
	if mode == "" {
		mode = a.agentModeLocked()
	}
	effort := updated.ReasoningEffort
	if !neoReasoningEffortAllowedForMode(mode, effort) {
		effort = a.reasoningEffortForModeLocked(mode)
	}
	a.currentAgentMode = mode
	a.currentReasoningEffort = effort
	a.mu.Unlock()

	a.broadcast(updateEvent)
	if truncateEvent != nil {
		a.broadcast(truncateEvent)
	}
	a.syncCloudAsync()
	if ready {
		go a.runInference(mode, effort)
		return
	}
	a.processQueue()
}

func (a *neoActor) rejectEdit(editID, message string) {
	if editID == "" {
		editID = "unknown"
	}
	a.broadcast(map[string]any{"type": "edit_rejected", "editId": editID, "message": message})
}

func (a *neoActor) startUserMessage(user neoQueuedMessage) {
	a.mu.Lock()
	mode := user.AgentMode
	if mode == "" {
		mode = a.agentModeLocked()
	}
	effort := user.ReasoningEffort
	if !neoReasoningEffortAllowedForMode(mode, effort) {
		effort = a.reasoningEffortForModeLocked(mode)
	}
	message := a.storeMessageLocked(neoMessage{
		ThreadID:         a.threadID,
		MessageID:        user.MessageID,
		Role:             "user",
		Content:          user.Content,
		AgentMode:        mode,
		ReasoningEffort:  effort,
		UserState:        user.UserState,
		Meta:             user.Meta,
		CreatedAt:        user.CreatedAt,
		CompletionStatus: "",
	})
	a.history = append(a.history, neoHistoryMessage{Role: "user", Text: textFromBlocks(user.Content), Content: cloneArray(user.Content)})
	a.mu.Unlock()

	a.broadcast(neoMessageAddedPayload(message))
	a.ensureThreadTitle(user.Content)
	a.syncCloudAsync()
	go a.runInference(mode, effort)
}

func (a *neoActor) runInference(agentMode, reasoningEffort string) {
	a.runInferenceForParent(agentMode, reasoningEffort, "")
}

func (a *neoActor) runInferenceForParent(agentMode, reasoningEffort, parentToolCallID string) {
	reasoningEffort = normalizeNeoReasoningEffortForMode(agentMode, reasoningEffort)
	a.mu.Lock()
	a.generation++
	generation := a.generation
	assistantID := newNeoMessageID()
	a.currentAgentMode = agentMode
	a.currentReasoningEffort = reasoningEffort
	a.agentState = "working"
	tools := a.toolNamesLocked(agentMode)
	request := a.inferenceRequestLocked(agentMode, reasoningEffort, parentToolCallID)
	a.mu.Unlock()
	streamBlockOffset := neoOpenAIThinkingBlockOffset(agentMode, selectNeoModelRoute(agentMode, request.Settings).Provider)

	a.broadcast(map[string]any{"type": "agent_state", "state": "working", "messageId": assistantID, "agentMode": agentMode, "reasoningEffort": omitEmpty(reasoningEffort)})
	a.broadcast(withNeoParentToolCallID(map[string]any{"type": "inference_tools", "messageId": assistantID, "agentMode": agentMode, "tools": tools}, parentToolCallID))
	a.broadcast(withNeoParentToolCallID(map[string]any{"type": "delta", "messageId": assistantID, "role": "assistant", "blocks": []any{}, "blockIndex": 0, "state": "start"}, parentToolCallID))

	streamed := false
	result, err := inferNeoLocalStream(a.runtime, request, func(delta neoInferenceDelta) {
		if delta.Text == "" {
			return
		}
		a.mu.Lock()
		alive := generation == a.generation
		a.mu.Unlock()
		if !alive {
			return
		}
		streamed = true
		a.broadcast(withNeoParentToolCallID(map[string]any{"type": "delta", "messageId": assistantID, "role": "assistant", "blocks": []any{map[string]any{"type": "text", "text": delta.Text}}, "blockIndex": streamBlockOffset, "state": "generating", "usage": delta.Usage}, parentToolCallID))
	})
	if err != nil {
		a.fail(err)
		a.setAgentState("idle", assistantID, agentMode, reasoningEffort)
		a.processQueue()
		return
	}

	a.mu.Lock()
	if generation != a.generation {
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()
	a.finishAssistantMessageWithOptions(assistantID, result, agentMode, reasoningEffort, streamed, parentToolCallID)
}

func (a *neoActor) finishAssistantMessage(messageID string, result neoInferenceResult, agentMode, reasoningEffort string) {
	a.finishAssistantMessageWithOptions(messageID, result, agentMode, reasoningEffort, false, "")
}

func (a *neoActor) finishAssistantMessageWithOptions(messageID string, result neoInferenceResult, agentMode, reasoningEffort string, streamed bool, parentToolCallID string) {
	normalizedCalls := normalizeNeoToolCalls(result.ToolCalls)
	streamBlockOffset := neoOpenAIThinkingBlockOffset(agentMode, result.Provider)
	blocks := make([]any, 0, 2+len(normalizedCalls))
	if streamBlockOffset > 0 {
		blocks = append(blocks, map[string]any{
			"type":      "thinking",
			"thinking":  "",
			"signature": "",
		})
	}
	if result.Text != "" {
		blocks = append(blocks, map[string]any{"type": "text", "text": result.Text})
	}
	for _, call := range normalizedCalls {
		blocks = append(blocks, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": call.Input, "complete": true})
	}
	usage := normalizeNeoUsage(result.Usage)

	a.setAgentState("streaming", messageID, agentMode, reasoningEffort)
	state := "generating"
	stopReason := "end_turn"
	if len(normalizedCalls) > 0 {
		state = "tool_use"
		stopReason = "tool_use"
	}
	if streamed {
		streamBlocks := make([]any, 0, len(normalizedCalls))
		for _, call := range normalizedCalls {
			streamBlocks = append(streamBlocks, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": call.Input, "complete": true})
		}
		if len(streamBlocks) > 0 {
			blockIndex := streamBlockOffset
			if result.Text != "" {
				blockIndex++
			}
			a.broadcast(withNeoParentToolCallID(map[string]any{"type": "delta", "messageId": messageID, "role": "assistant", "blocks": streamBlocks, "blockIndex": blockIndex, "state": state, "usage": usage}, parentToolCallID))
		}
	} else {
		a.broadcast(withNeoParentToolCallID(map[string]any{"type": "delta", "messageId": messageID, "role": "assistant", "blocks": blocks, "blockIndex": 0, "state": state, "usage": usage}, parentToolCallID))
	}
	if len(normalizedCalls) == 0 {
		a.broadcast(withNeoParentToolCallID(map[string]any{"type": "delta", "messageId": messageID, "role": "assistant", "blocks": []any{}, "state": "complete", "usage": usage}, parentToolCallID))
	}

	a.mu.Lock()
	stored := a.storeMessageLocked(neoMessage{
		ThreadID:        a.threadID,
		MessageID:       messageID,
		Role:            "assistant",
		Content:         blocks,
		State:           map[string]any{"type": "complete", "stopReason": stopReason},
		Usage:           usage,
		CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		AgentMode:       agentMode,
		ParentToolUseID: parentToolCallID,
	})
	toolCalls := make([]neoPendingTool, 0, len(normalizedCalls))
	for _, call := range normalizedCalls {
		pending := neoPendingTool{ID: call.ID, Name: call.Name, Input: call.Input, AgentMode: agentMode, ReasoningEffort: reasoningEffort, MessageID: messageID, ParentToolCallID: parentToolCallID}
		a.pendingTools[call.ID] = pending
		toolCalls = append(toolCalls, pending)
	}
	a.history = append(a.history, neoHistoryMessage{Role: "assistant", Text: result.Text, ToolCalls: normalizedCalls, ParentToolUseID: parentToolCallID})
	a.mu.Unlock()

	a.broadcast(neoMessageAddedPayload(stored))
	a.syncCloudAsync()

	if len(toolCalls) == 0 {
		a.setAgentState("idle", messageID, agentMode, reasoningEffort)
		a.processQueue()
		return
	}

	a.setAgentState("running_tools", messageID, agentMode, reasoningEffort)
	for _, call := range toolCalls {
		a.broadcast(withNeoParentToolCallID(map[string]any{"type": "tool_lease", "toolCallId": call.ID, "toolName": call.Name, "args": call.Input, "messageId": stored.MessageID}, call.ParentToolCallID))
	}
}

func shouldAddNeoOpenAIThinkingBlock(result neoInferenceResult, agentMode string) bool {
	return neoOpenAIThinkingBlockOffset(agentMode, result.Provider) > 0
}

func neoOpenAIThinkingBlockOffset(agentMode, provider string) int {
	if strings.EqualFold(agentMode, "deep") && strings.EqualFold(provider, "openai") {
		return 1
	}
	return 0
}

func (a *neoActor) receiveToolResult(msg map[string]any) {
	toolCallID := stringValue(msg["toolCallId"])
	run := mapValue(msg["run"])
	a.mu.Lock()
	pending, ok := a.pendingTools[toolCallID]
	if !ok {
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "executor_error", "message": "Unknown tool lease " + toolCallID, "toolCallId": toolCallID, "code": "LEASE_NOT_FOUND"})
		return
	}
	delete(a.pendingTools, toolCallID)
	approvalRemoved := a.removeApprovalLocked(toolCallID)
	approvals := a.approvalQueueListLocked()
	approvalStateChanged := false
	approvalState := a.agentState
	if approvalRemoved && len(approvals) == 0 && a.agentState == "awaiting_approval" {
		if len(a.pendingTools) > 0 {
			approvalState = "running_tools"
		} else {
			approvalState = "idle"
		}
		a.agentState = approvalState
		approvalStateChanged = true
	}
	a.mu.Unlock()

	run = normalizeNeoLocalThreadToolRun(context.Background(), a.configSnapshot(), pending, run, a.threadID)

	a.mu.Lock()
	_, event := a.storeMessageEventLocked(neoMessage{
		ThreadID:        a.threadID,
		Role:            "user",
		MessageID:       toolResultMessageID(toolCallID),
		Content:         []any{map[string]any{"type": "tool_result", "toolUseID": toolCallID, "run": run}},
		CreatedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		ParentToolUseID: pending.ParentToolCallID,
	})
	a.history = append(a.history, neoHistoryMessage{Role: "tool", ToolCallID: toolCallID, ToolName: pending.Name, Text: runToText(run), ParentToolUseID: pending.ParentToolCallID})
	remaining := len(a.pendingTools)
	a.mu.Unlock()

	a.broadcast(event)
	a.syncCloudAsync()
	a.broadcast(map[string]any{"type": "executor_tool_result_ack", "toolCallId": toolCallID})
	if approvalRemoved {
		a.broadcast(toolApprovalQueuePayload(approvals))
	}
	if approvalStateChanged {
		a.broadcast(map[string]any{"type": "agent_state", "state": approvalState, "agentMode": pending.AgentMode, "reasoningEffort": omitEmpty(pending.ReasoningEffort)})
	}
	if remaining == 0 {
		go a.runInferenceForParent(pending.AgentMode, pending.ReasoningEffort, pending.ParentToolCallID)
	}
}

func normalizeNeoLocalThreadToolRun(ctx context.Context, cfg *config.Config, pending neoPendingTool, run map[string]any, currentThreadID string) map[string]any {
	switch pending.Name {
	case "read_thread":
		return normalizeNeoReadThreadToolRun(ctx, cfg, pending, run, currentThreadID)
	case "find_thread", "thread_search", "search_threads":
		return normalizeNeoFindThreadToolRun(ctx, cfg, pending, run)
	default:
		return run
	}
}

func normalizeNeoReadThreadToolRun(ctx context.Context, cfg *config.Config, pending neoPendingTool, run map[string]any, currentThreadID string) map[string]any {
	threadID := neoToolInputThreadID(pending.Input)
	if threadID == "" {
		return run
	}
	thread, ok := loadNeoThread(ctx, cfg, threadID)
	if !ok {
		return run
	}
	text := strings.ToLower(runToText(run))
	wrongThread := currentThreadID != "" && strings.Contains(text, strings.ToLower(currentThreadID)) && !strings.Contains(text, strings.ToLower(threadID))
	failed := text == "" || stringValue(run["status"]) == "error" || strings.Contains(text, "not found") || strings.Contains(text, "unavailable") || strings.Contains(text, "no actionable content") || strings.Contains(text, "no useful content") || strings.Contains(text, "only metadata") || strings.Contains(text, "lookup returned no content") || strings.Contains(text, "contains no messages") || strings.Contains(text, "contains no message content")
	if !wrongThread && !failed {
		return run
	}

	rewritten := cloneMap(run)
	rewritten["status"] = "done"
	rewritten["result"] = neoThreadToolFallbackMarkdown(threadID, pending.Input, thread)
	delete(rewritten, "error")
	return rewritten
}

func normalizeNeoFindThreadToolRun(ctx context.Context, cfg *config.Config, pending neoPendingTool, run map[string]any) map[string]any {
	query := strings.TrimSpace(stringValue(pending.Input["query"]))
	if query == "" {
		return run
	}
	text := strings.ToLower(runToText(run))
	if !strings.Contains(text, "threads:[]") && !strings.Contains(text, `"threads":[]`) {
		return run
	}
	values := url.Values{"q": []string{query}}
	if limit := stringValue(pending.Input["limit"]); limit != "" {
		values.Set("limit", limit)
	}
	result, ok := neoThreadSearchResponse(ctx, cfg, values)
	if !ok {
		return run
	}
	rewritten := cloneMap(run)
	rewritten["status"] = "done"
	rewritten["result"] = result
	delete(rewritten, "error")
	return rewritten
}

func neoToolInputThreadID(input map[string]any) string {
	raw := firstNonEmptyString(input["threadID"], input["threadId"], input["thread_id"])
	raw = strings.TrimSpace(strings.TrimPrefix(raw, "@"))
	if raw == "" {
		return ""
	}
	if !neoThreadIDExactPattern.MatchString(raw) {
		raw = findThreadID(raw)
	}
	if !neoThreadIDExactPattern.MatchString(raw) {
		return ""
	}
	return raw
}

func neoThreadToolFallbackMarkdown(threadID string, input map[string]any, thread map[string]any) string {
	var out strings.Builder
	out.WriteString("Local thread fallback for ")
	out.WriteString(threadID)
	out.WriteString(". Amp's thread reader returned an unavailable or wrong-thread result, so CLIProxyAPI supplied the locally stored thread content.\n\n")
	if goal := strings.TrimSpace(stringValue(input["goal"])); goal != "" {
		out.WriteString("Goal: ")
		out.WriteString(goal)
		out.WriteString("\n\n")
	}
	out.WriteString(neoThreadMarkdown(thread))
	return out.String()
}

func (a *neoActor) revokeToolLease(msg map[string]any) {
	toolCallID := stringValue(msg["toolCallId"])
	if toolCallID == "" {
		return
	}
	revoked := normalizeNeoToolLeaseRevoked(msg)
	reason := stringValue(revoked["reason"])
	a.mu.Lock()
	pending, ok := a.pendingTools[toolCallID]
	if ok {
		delete(a.pendingTools, toolCallID)
	}
	updateEvents := a.cancelToolResultMessagesLocked([]string{toolCallID}, neoCancelledToolRunReason(reason))
	approvalRemoved := a.removeApprovalLocked(toolCallID)
	approvals := a.approvalQueueListLocked()
	remaining := len(a.pendingTools)
	if ok && remaining == 0 {
		a.agentState = "idle"
	}
	a.mu.Unlock()

	a.broadcast(revoked)
	if approvalRemoved {
		a.broadcast(toolApprovalQueuePayload(approvals))
	}
	for _, event := range updateEvents {
		a.broadcast(event)
	}
	if ok && remaining == 0 {
		a.broadcast(map[string]any{
			"type":            "agent_state",
			"state":           "idle",
			"messageId":       omitEmpty(pending.MessageID),
			"agentMode":       pending.AgentMode,
			"reasoningEffort": omitEmpty(pending.ReasoningEffort),
		})
		a.processQueue()
	}
}

type neoCloudThreadSnapshot struct {
	upstreamURL       string
	apiKey            string
	threadID          string
	seq               int
	createdMs         int64
	title             string
	threadStatus      string
	settings          map[string]any
	messages          []neoMessage
	environment       map[string]any
	artifacts         []any
	compactionRecords []any
}

func (a *neoActor) syncCloudAsync() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.threadID == "" {
		a.mu.Unlock()
		return
	}
	if a.syncRunning {
		a.syncPending = true
		a.mu.Unlock()
		return
	}
	a.syncRunning = true
	a.mu.Unlock()

	go a.syncCloudLoop()
}

func (a *neoActor) syncCloudLoop() {
	for {
		snapshot, ok := a.threadSnapshot()
		if ok {
			if err := writeNeoLocalThreadSnapshot(snapshot); err != nil {
				log.Warnf("amp neo local runtime thread store sync failed thread=%s: %v", snapshot.threadID, err)
			}

			if cloudSnapshot, ok := a.cloudThreadSnapshot(snapshot); ok {
				if err := uploadNeoCloudThread(cloudSnapshot); err != nil {
					log.Warnf("amp neo local runtime cloud sync failed thread=%s: %v", snapshot.threadID, err)
				}
			}
		}

		a.mu.Lock()
		if a.syncPending {
			a.syncPending = false
			a.mu.Unlock()
			continue
		}
		a.syncRunning = false
		a.mu.Unlock()
		return
	}
}

func (a *neoActor) threadSnapshot() (neoCloudThreadSnapshot, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.threadID == "" {
		return neoCloudThreadSnapshot{}, false
	}
	messages := append([]neoMessage(nil), a.messages...)
	return neoCloudThreadSnapshot{
		threadID:          a.threadID,
		seq:               a.seq,
		createdMs:         neoCloudCreatedMillis(a.record),
		title:             a.title,
		threadStatus:      a.threadStatus,
		settings:          cloneMap(a.settings),
		messages:          messages,
		environment:       cloneMap(a.environment),
		artifacts:         a.artifactListLocked(),
		compactionRecords: a.compactionRecordListLocked(),
	}, true
}

func (a *neoActor) cloudThreadSnapshot(snapshot neoCloudThreadSnapshot) (neoCloudThreadSnapshot, bool) {
	cfg := a.runtime.configSnapshot()
	if cfg == nil {
		return neoCloudThreadSnapshot{}, false
	}
	upstreamURL := strings.TrimSpace(cfg.AmpCode.UpstreamURL)
	apiKey := strings.TrimSpace(cfg.AmpCode.UpstreamAPIKey)
	if upstreamURL == "" || apiKey == "" {
		return neoCloudThreadSnapshot{}, false
	}

	snapshot.upstreamURL = upstreamURL
	snapshot.apiKey = apiKey
	return snapshot, true
}

func uploadNeoCloudThread(snapshot neoCloudThreadSnapshot) error {
	thread := neoCloudThread(snapshot)
	payload := map[string]any{
		"method": "uploadThread",
		"params": map[string]any{
			"thread":          thread,
			"createdOnServer": false,
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	var body bytes.Buffer
	if len(raw) >= neoCloudGzipBytes {
		gz := gzip.NewWriter(&body)
		if _, err := gz.Write(raw); err != nil {
			_ = gz.Close()
			return err
		}
		if err := gz.Close(); err != nil {
			return err
		}
	} else {
		body.Write(raw)
	}

	base, err := url.Parse(snapshot.upstreamURL)
	if err != nil {
		return err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/internal"
	base.RawQuery = url.QueryEscape("uploadThread")

	req, err := http.NewRequest(http.MethodPost, base.String(), bytes.NewReader(body.Bytes()))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+snapshot.apiKey)
	req.Header.Set("Content-Type", "application/json")
	if len(raw) >= neoCloudGzipBytes {
		req.Header.Set("Content-Encoding", "gzip")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, clipNeoErrorBody(respBody))
	}
	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err == nil && decoded["ok"] == false {
		return fmt.Errorf("uploadThread failed: %s", clipNeoErrorBody(respBody))
	}
	log.Debugf("amp neo local runtime cloud sync complete thread=%s", snapshot.threadID)
	return nil
}

func (rt *neoRuntime) remoteControlLoop(ctx context.Context) {
	log.Infof("amp neo local runtime remote-control polling enabled")
	for {
		cfg := rt.configSnapshot()
		if neoRemoteControlEnabled(cfg) {
			rt.pollRemoteControlOnce(ctx, cfg)
		}
		wait := neoRemoteControlPollInterval(cfg)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (rt *neoRuntime) pollRemoteControlOnce(ctx context.Context, cfg *config.Config) {
	if ctx.Err() != nil {
		return
	}
	actors := rt.remoteControlActors(ctx, cfg)
	for _, actor := range actors {
		if ctx.Err() != nil {
			return
		}
		if err := actor.pollRemoteControlThread(ctx, cfg); err != nil {
			log.Debugf("amp neo local runtime remote-control poll failed thread=%s: %v", actor.threadID, err)
		}
	}
}

func (rt *neoRuntime) remoteControlActors(ctx context.Context, cfg *config.Config) []*neoActor {
	seen := map[string]bool{}
	actors := make([]*neoActor, 0)
	maxThreads := neoRemoteControlMaxThreads(cfg)
	canAdd := func() bool { return maxThreads <= 0 || len(actors) < maxThreads }
	for _, actor := range rt.store.threadActors(0) {
		if !neoRemoteControlThreadID(actor.threadID) {
			continue
		}
		if !canAdd() {
			break
		}
		seen[actor.threadID] = true
		actors = append(actors, actor)
	}

	for _, thread := range recentNeoLocalThreads(maxThreads) {
		if !canAdd() {
			break
		}
		threadID := stringValue(thread["id"])
		if !neoRemoteControlThreadID(threadID) || seen[threadID] {
			continue
		}
		actor := rt.store.ensureThreadActor(threadID)
		actor.ensureImportedLocalThread(thread)
		seen[threadID] = true
		actors = append(actors, actor)
	}
	if threads, ok, err := getNeoCloudRecentThreads(ctx, cfg, maxThreads); err != nil {
		log.Debugf("amp neo local runtime remote-control cloud list failed: %v", err)
	} else if ok {
		for _, thread := range threads {
			if !canAdd() {
				break
			}
			threadID := stringValue(thread["id"])
			if !neoRemoteControlThreadID(threadID) || seen[threadID] {
				continue
			}
			actor := rt.store.ensureThreadActor(threadID)
			seen[threadID] = true
			actors = append(actors, actor)
		}
	}
	sort.Slice(actors, func(i, j int) bool {
		return actors[i].threadID < actors[j].threadID
	})
	return actors
}

func (a *neoActor) pollRemoteControlThread(ctx context.Context, cfg *config.Config) error {
	threadID := a.threadID
	if !neoRemoteControlThreadID(threadID) {
		return nil
	}
	threadBody, ok, err := getNeoCloudThreadBody(ctx, cfg, threadID)
	if err != nil || !ok {
		return err
	}
	a.applyRemoteControlThreadBody(threadBody)
	return nil
}

func getNeoCloudThreadBody(ctx context.Context, cfg *config.Config, threadID string) ([]byte, bool, error) {
	if cfg == nil {
		return nil, false, nil
	}
	upstreamURL := strings.TrimSpace(cfg.AmpCode.UpstreamURL)
	apiKey := strings.TrimSpace(cfg.AmpCode.UpstreamAPIKey)
	if upstreamURL == "" || apiKey == "" || !neoRemoteControlThreadID(threadID) {
		return nil, false, nil
	}
	payload := map[string]any{
		"method": "getThread",
		"params": map[string]any{"thread": threadID},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	base, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, false, err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/internal"
	base.RawQuery = url.QueryEscape("getThread")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(raw))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, clipNeoErrorBody(respBody))
	}
	decoded := gjson.ParseBytes(respBody)
	if decoded.Get("ok").Exists() && !decoded.Get("ok").Bool() {
		if decoded.Get("error.code").String() == "thread-not-found" {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("getThread failed: %s", clipNeoErrorBody(respBody))
	}
	return respBody, true, nil
}

func getNeoCloudThread(ctx context.Context, cfg *config.Config, threadID string) (map[string]any, bool, error) {
	respBody, ok, err := getNeoCloudThreadBody(ctx, cfg, threadID)
	if err != nil || !ok {
		return nil, ok, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return nil, false, err
	}
	envelope := mapValue(mapValue(decoded["result"])["thread"])
	thread := mapValue(envelope["data"])
	if len(thread) == 0 {
		thread = envelope
	} else {
		for _, key := range []string{"id", "title", "created", "updatedAt", "creatorUserID"} {
			if _, exists := thread[key]; !exists && envelope[key] != nil {
				thread[key] = envelope[key]
			}
		}
	}
	if len(thread) == 0 {
		return nil, false, nil
	}
	return thread, true, nil
}

func getNeoCloudThreadSearch(ctx context.Context, cfg *config.Config, q url.Values) (map[string]any, bool, error) {
	if cfg == nil || strings.TrimSpace(q.Get("q")) == "" {
		return nil, false, nil
	}
	upstreamURL := strings.TrimSpace(cfg.AmpCode.UpstreamURL)
	apiKey := strings.TrimSpace(cfg.AmpCode.UpstreamAPIKey)
	if upstreamURL == "" || apiKey == "" {
		return nil, false, nil
	}
	base, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, false, err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/threads/find"
	base.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, clipNeoErrorBody(respBody))
	}
	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return nil, false, err
	}
	if _, exists := decoded["threads"]; !exists {
		return nil, false, nil
	}
	return decoded, true, nil
}

func getNeoCloudRecentThreads(ctx context.Context, cfg *config.Config, limit int) ([]map[string]any, bool, error) {
	if cfg == nil {
		return nil, false, nil
	}
	upstreamURL := strings.TrimSpace(cfg.AmpCode.UpstreamURL)
	apiKey := strings.TrimSpace(cfg.AmpCode.UpstreamAPIKey)
	if upstreamURL == "" || apiKey == "" || limit <= 0 {
		return nil, false, nil
	}
	if limit > 100 {
		limit = 100
	}
	payload := map[string]any{
		"method": "listThreads",
		"params": map[string]any{"limit": limit},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, false, err
	}
	base, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, false, err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/api/internal"
	base.RawQuery = url.QueryEscape("listThreads")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.String(), bytes.NewReader(raw))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, clipNeoErrorBody(respBody))
	}
	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return nil, false, err
	}
	if decoded["ok"] == false {
		return nil, false, fmt.Errorf("listThreads failed: %s", clipNeoErrorBody(respBody))
	}
	rawThreads := arrayValue(mapValue(decoded["result"])["threads"])
	threads := make([]map[string]any, 0, len(rawThreads))
	for _, rawThread := range rawThreads {
		thread := mapValue(rawThread)
		if len(thread) > 0 {
			threads = append(threads, thread)
		}
	}
	return threads, true, nil
}

func (a *neoActor) ensureImportedLocalThread(thread map[string]any) {
	a.mu.Lock()
	hasMessages := len(a.messages) > 0
	a.mu.Unlock()
	if hasMessages {
		return
	}
	if err := a.importThreadLocalOnly(thread); err != nil {
		log.Debugf("amp neo local runtime remote-control local import failed thread=%s: %v", a.threadID, err)
	}
}

func (a *neoActor) applyRemoteControlThreadBody(body []byte) {
	thread := gjson.GetBytes(body, "result.thread")
	if !thread.Exists() {
		return
	}
	data := thread.Get("data")
	threadID := firstNonEmptyString(data.Get("id").String(), thread.Get("id").String(), a.threadID)
	messages := data.Get("messages")
	if !messages.Exists() {
		messages = thread.Get("messages")
	}
	a.applyRemoteControlMessages(threadID, messages)
}

func (a *neoActor) applyRemoteControlMessages(threadID string, messages gjson.Result) {
	if threadID == "" || threadID != a.threadID || !messages.Exists() || !messages.IsArray() {
		return
	}

	remoteUsers := make([]neoQueuedMessage, 0)
	a.mu.Lock()
	if a.remoteSeenMessages == nil {
		a.remoteSeenMessages = map[string]bool{}
	}
	existing := make(map[string]bool, len(a.messages)+len(a.queue))
	for _, message := range a.messages {
		if message.MessageID != "" {
			existing[message.MessageID] = true
		}
	}
	for _, queued := range a.queue {
		if queued.MessageID != "" {
			existing[queued.MessageID] = true
		}
	}
	if !a.remoteControlStarted {
		count := 0
		messages.ForEach(func(_, message gjson.Result) bool {
			if messageID := neoRemoteControlMessageID(message); messageID != "" {
				a.remoteSeenMessages[messageID] = true
				count++
			}
			return true
		})
		a.remoteControlStarted = true
		a.mu.Unlock()
		log.Debugf("amp neo local runtime remote-control baselined %d cloud message(s) thread=%s", count, threadID)
		return
	}
	messages.ForEach(func(_, message gjson.Result) bool {
		messageID := neoRemoteControlMessageID(message)
		if messageID == "" || a.remoteSeenMessages[messageID] {
			return true
		}
		a.remoteSeenMessages[messageID] = true
		if message.Get("role").String() != "user" || existing[messageID] || neoRemoteControlMessageIsToolResult(message) {
			return true
		}
		existing[messageID] = true
		remoteUsers = append(remoteUsers, neoQueuedMessage{
			MessageID:       messageID,
			Content:         neoJSONArrayValue(message.Get("content")),
			UserState:       neoJSONObjectValue(message.Get("userState")),
			Meta:            neoJSONObjectValue(message.Get("meta")),
			CreatedAt:       fallbackString(message.Get("createdAt").String(), fallbackString(message.Get("created").String(), time.Now().UTC().Format(time.RFC3339Nano))),
			AgentMode:       message.Get("agentMode").String(),
			ReasoningEffort: message.Get("reasoningEffort").String(),
		})
		return true
	})
	ready := a.executorReady
	a.mu.Unlock()

	a.injectRemoteControlUsers(threadID, remoteUsers, ready)
}

func (a *neoActor) applyRemoteControlThread(thread map[string]any) {
	threadID := firstNonEmptyString(thread["id"], findThreadID(thread))
	if threadID == "" || threadID != a.threadID {
		return
	}
	rawMessages := arrayValue(thread["messages"])
	if len(rawMessages) == 0 {
		return
	}

	remoteMessages := make([]neoMessage, 0, len(rawMessages))
	for i, raw := range rawMessages {
		message := neoMessageFromImportedThread(threadID, raw, i)
		if message.Role != "" && message.MessageID != "" {
			remoteMessages = append(remoteMessages, message)
		}
	}
	if len(remoteMessages) == 0 {
		return
	}

	remoteUsers := make([]neoQueuedMessage, 0)
	a.mu.Lock()
	if a.remoteSeenMessages == nil {
		a.remoteSeenMessages = map[string]bool{}
	}
	existing := make(map[string]bool, len(a.messages)+len(a.queue))
	for _, message := range a.messages {
		if message.MessageID != "" {
			existing[message.MessageID] = true
		}
	}
	for _, queued := range a.queue {
		if queued.MessageID != "" {
			existing[queued.MessageID] = true
		}
	}
	if !a.remoteControlStarted {
		for _, message := range remoteMessages {
			a.remoteSeenMessages[message.MessageID] = true
		}
		a.remoteControlStarted = true
		a.mu.Unlock()
		log.Debugf("amp neo local runtime remote-control baselined %d cloud message(s) thread=%s", len(remoteMessages), threadID)
		return
	}
	for _, message := range remoteMessages {
		if a.remoteSeenMessages[message.MessageID] {
			continue
		}
		a.remoteSeenMessages[message.MessageID] = true
		if message.Role != "user" || existing[message.MessageID] || neoMessageIsToolResult(message) {
			continue
		}
		existing[message.MessageID] = true
		remoteUsers = append(remoteUsers, neoQueuedMessage{
			MessageID:       message.MessageID,
			Content:         message.Content,
			UserState:       message.UserState,
			Meta:            message.Meta,
			CreatedAt:       fallbackString(message.CreatedAt, time.Now().UTC().Format(time.RFC3339Nano)),
			AgentMode:       message.AgentMode,
			ReasoningEffort: message.ReasoningEffort,
		})
	}
	ready := a.executorReady
	a.mu.Unlock()

	a.injectRemoteControlUsers(threadID, remoteUsers, ready)
}

func (a *neoActor) injectRemoteControlUsers(threadID string, remoteUsers []neoQueuedMessage, ready bool) {
	if len(remoteUsers) == 0 {
		return
	}
	log.Infof("amp neo local runtime remote-control injecting %d cloud user message(s) thread=%s", len(remoteUsers), threadID)
	for _, user := range remoteUsers {
		a.receiveUserMessage(map[string]any{
			"type":            "client_append_user_msg",
			"messageId":       user.MessageID,
			"content":         user.Content,
			"userState":       user.UserState,
			"meta":            user.Meta,
			"createdAt":       user.CreatedAt,
			"agentMode":       user.AgentMode,
			"reasoningEffort": user.ReasoningEffort,
		})
	}
	if !ready {
		a.spawnExecutor(map[string]any{
			"type":      "client_spawn_executor",
			"requestId": "remote-control-" + randomBase62(10),
			"threadId":  threadID,
		})
	}
}

func neoRemoteControlMessageID(message gjson.Result) string {
	return firstNonEmptyString(message.Get("messageId").String(), message.Get("id").String())
}

func neoRemoteControlMessageIsToolResult(message gjson.Result) bool {
	content := message.Get("content")
	if !content.Exists() || !content.IsArray() {
		return false
	}
	found := false
	content.ForEach(func(_, part gjson.Result) bool {
		if part.Get("type").String() == "tool_result" {
			found = true
			return false
		}
		return true
	})
	return found
}

func neoJSONArrayValue(value gjson.Result) []any {
	if !value.Exists() || !value.IsArray() {
		return nil
	}
	var decoded []any
	if err := json.Unmarshal([]byte(value.Raw), &decoded); err != nil {
		return nil
	}
	return decoded
}

func neoJSONObjectValue(value gjson.Result) map[string]any {
	if !value.Exists() || !value.IsObject() {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(value.Raw), &decoded); err != nil {
		return nil
	}
	return decoded
}

func neoRemoteControlThreadID(threadID string) bool {
	return neoCloudThreadIDPattern.MatchString(strings.TrimSpace(threadID))
}

func neoMessageIsToolResult(message neoMessage) bool {
	for _, raw := range message.Content {
		if stringValue(mapValue(raw)["type"]) == "tool_result" {
			return true
		}
	}
	return false
}

func recentNeoLocalThreads(limit int) []map[string]any {
	if limit <= 0 {
		return nil
	}
	dir := neoAmpThreadStoreDir()
	if dir == "" {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	threads := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		threadID := strings.TrimSuffix(entry.Name(), ".json")
		if !neoRemoteControlThreadID(threadID) {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		root := gjson.ParseBytes(raw)
		id := firstNonEmptyString(root.Get("id").String(), threadID)
		if !neoRemoteControlThreadID(id) {
			continue
		}
		thread := map[string]any{"id": id}
		for _, key := range []string{"title", "created", "createdAt", "updated", "updatedAt", "userLastInteractedAt", "creatorUserID"} {
			if value := root.Get(key); value.Exists() {
				thread[key] = value.Value()
			}
		}
		if updated := neoThreadUpdatedMillisFromJSON(root); updated > 0 {
			thread["updated"] = updated
		}
		threads = append(threads, thread)
	}
	sort.Slice(threads, func(i, j int) bool {
		return neoThreadUpdatedMillis(threads[i]) > neoThreadUpdatedMillis(threads[j])
	})
	if len(threads) > limit {
		threads = threads[:limit]
	}
	return threads
}

func neoThreadUpdatedMillisFromJSON(thread gjson.Result) int {
	for _, key := range []string{"updatedAt", "updated", "userLastInteractedAt", "createdAt", "created"} {
		if updated := neoJSONMillis(thread.Get(key)); updated > 0 {
			return updated
		}
	}
	updated := 0
	if messages := thread.Get("messages"); messages.Exists() && messages.IsArray() {
		messages.ForEach(func(_, message gjson.Result) bool {
			if created := firstNonZero(neoJSONMillis(message.Get("created")), neoJSONMillis(message.Get("createdAt"))); created > updated {
				updated = created
			}
			return true
		})
	}
	return updated
}

func neoJSONMillis(value gjson.Result) int {
	if !value.Exists() {
		return 0
	}
	if value.Type == gjson.Number {
		return int(value.Int())
	}
	return neoTimeStringMillis(value.String())
}

func neoThreadUpdatedMillis(thread map[string]any) int {
	updated := neoThreadResultUpdatedMillis(thread)
	if updated > 0 {
		return updated
	}
	for _, raw := range arrayValue(thread["messages"]) {
		message := mapValue(raw)
		if created := numberFrom(message["created"], message["createdAt"]); created > updated {
			updated = created
		}
	}
	return firstNonZero(updated, numberFrom(thread["created"]))
}

func neoThreadResultUpdatedMillis(thread map[string]any) int {
	if updated := numberFrom(thread["updatedAt"], thread["updated"], thread["userLastInteractedAt"], thread["created"]); updated > 0 {
		return updated
	}
	for _, key := range []string{"updatedAt", "updated", "createdAt", "created"} {
		if updated := neoTimeStringMillis(stringValue(thread[key])); updated > 0 {
			return updated
		}
	}
	return 0
}

func neoTimeStringMillis(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return int(parsed.UnixMilli())
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return int(parsed.UnixMilli())
	}
	return 0
}

func firstNonZero(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func neoRemoteControlEnabled(cfg *config.Config) bool {
	if cfg == nil || !neoRuntimeEnabled(cfg) {
		return false
	}
	if cfg.AmpCode.NeoLocalRuntime.RemoteControl == nil {
		return false
	}
	return *cfg.AmpCode.NeoLocalRuntime.RemoteControl
}

func neoRemoteControlPollInterval(cfg *config.Config) time.Duration {
	if cfg != nil && cfg.AmpCode.NeoLocalRuntime.RemotePollIntervalSeconds > 0 {
		return time.Duration(cfg.AmpCode.NeoLocalRuntime.RemotePollIntervalSeconds) * time.Second
	}
	return defaultNeoRemoteControlPollInterval
}

func neoRemoteControlMaxThreads(cfg *config.Config) int {
	if cfg != nil && cfg.AmpCode.NeoLocalRuntime.RemoteControlMaxThreads > 0 {
		return cfg.AmpCode.NeoLocalRuntime.RemoteControlMaxThreads
	}
	return defaultNeoRemoteControlMaxThreads
}

func defaultNeoAmpThreadStoreDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".local", "share", "amp", "threads")
}

func writeNeoLocalThreadSnapshot(snapshot neoCloudThreadSnapshot) error {
	if !neoThreadIDExactPattern.MatchString(snapshot.threadID) {
		return fmt.Errorf("invalid thread id %q", snapshot.threadID)
	}
	dir := neoAmpThreadStoreDir()
	if dir == "" {
		return errors.New("amp thread store directory unavailable")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	thread := neoCloudThread(snapshot)
	raw, err := json.MarshalIndent(thread, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, snapshot.threadID+".json")
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	log.Debugf("amp neo local runtime thread store sync complete thread=%s path=%s", snapshot.threadID, path)
	return nil
}

func loadNeoLocalThread(threadID string) (map[string]any, bool) {
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil, false
	}
	dir := neoAmpThreadStoreDir()
	if dir == "" {
		return nil, false
	}
	raw, err := os.ReadFile(filepath.Join(dir, threadID+".json"))
	if err != nil {
		return nil, false
	}
	var thread map[string]any
	if err := json.Unmarshal(raw, &thread); err != nil {
		log.Warnf("amp neo local thread store read failed thread=%s: %v", threadID, err)
		return nil, false
	}
	return thread, true
}

func tryServeNeoLocalThread(c *gin.Context, cfg *config.Config) bool {
	if c == nil || c.Request == nil || c.Request.Method != http.MethodGet {
		return false
	}
	if neoThreadSearchPath(c.Request.URL.Path) {
		result, ok := neoThreadSearchResponse(c.Request.Context(), cfg, c.Request.URL.Query())
		if !ok {
			return false
		}
		c.JSON(http.StatusOK, result)
		return true
	}
	threadID, markdown := neoThreadRequestPath(c.Request.URL.Path)
	if threadID == "" {
		return false
	}
	thread, ok := loadNeoThread(c.Request.Context(), cfg, threadID)
	if !ok {
		return false
	}
	if markdown {
		c.Data(http.StatusOK, "text/markdown; charset=utf-8", []byte(neoThreadMarkdown(thread)))
		return true
	}
	c.JSON(http.StatusOK, thread)
	return true
}

func (m *AmpModule) canServeNeoLocalManagement(r *http.Request) bool {
	if m == nil || r == nil || r.URL == nil {
		return false
	}
	if _, ok := neoThreadActorManagementPath(r.URL.Path); ok && m.neoRuntime != nil {
		return true
	}
	if r.Method != http.MethodGet {
		return false
	}
	if neoThreadSearchPath(r.URL.Path) {
		return m.neoThreadConfigSnapshot() != nil
	}
	threadID, _ := neoThreadRequestPath(r.URL.Path)
	return threadID != "" && m.neoThreadConfigSnapshot() != nil
}

func (m *AmpModule) tryServeNeoLocalThreadActor(c *gin.Context) bool {
	if m == nil || c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	threadID, ok := neoThreadActorManagementPath(c.Request.URL.Path)
	if !ok || m.neoRuntime == nil {
		return false
	}
	if m.getProxy() != nil && !m.forceNeoLocalThreadActors() {
		return false
	}
	if c.Request.Method != http.MethodPost {
		c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "method_not_allowed"})
		return true
	}

	body := readNeoJSON(c.Request.Body)
	response, status := m.neoRuntime.localThreadActorManagementResponse(c.Request.Context(), body, threadID)
	if threadID != "" && status == http.StatusOK {
		c.JSON(http.StatusOK, gin.H{"ok": true, "threadId": response["threadId"]})
		return true
	}
	c.JSON(status, response)
	return true
}

func (m *AmpModule) forceNeoLocalThreadActors() bool {
	cfg := m.neoThreadConfigSnapshot()
	return cfg != nil && cfg.AmpCode.NeoLocalRuntime.ForceThreadActors
}

func neoThreadActorManagementPath(path string) (string, bool) {
	path = strings.TrimPrefix(path, "/api")
	path = "/" + strings.Trim(path, "/")
	if path == "/thread-actors" {
		return "", true
	}
	if !strings.HasPrefix(path, "/thread-actors/") {
		return "", false
	}
	threadID := strings.Trim(strings.TrimPrefix(path, "/thread-actors/"), "/")
	if threadID == "" || strings.Contains(threadID, "/") {
		return "", false
	}
	if unescaped, err := url.PathUnescape(threadID); err == nil {
		threadID = unescaped
	}
	return threadID, true
}

func (rt *neoRuntime) localThreadActorManagementResponse(ctx context.Context, body map[string]any, requestedThreadID string) (map[string]any, int) {
	if rt == nil {
		return map[string]any{"error": "neo_runtime_unavailable"}, http.StatusServiceUnavailable
	}
	threadID := strings.TrimSpace(requestedThreadID)
	if threadID == "" {
		threadID = findThreadID(body)
	}
	if threadID == "" {
		threadID = "T-" + randomUUIDLike()
	}
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return map[string]any{"error": "invalid_thread_id", "threadId": threadID}, http.StatusBadRequest
	}

	actor := rt.store.ensureThreadActor(threadID)
	actor.touch()
	loadedThread := false
	if thread, ok := loadNeoThread(ctx, rt.configSnapshot(), threadID); ok {
		loadedThread = true
		if err := actor.importThreadLocalOnly(thread); err != nil {
			log.Debugf("amp neo local runtime thread-actors import failed thread=%s: %v", threadID, err)
		}
	}

	agentMode := firstNonEmptyString(body["agentMode"], nestedString(body["threadMeta"], "agentMode"))
	executorType := firstNonEmptyString(body["executorType"])
	actor.mu.Lock()
	if agentMode != "" && (!loadedThread || actor.currentAgentMode == "") {
		actor.currentAgentMode = agentMode
		actor.currentReasoningEffort = defaultNeoReasoningEffort(agentMode)
		if actor.settings == nil {
			actor.settings = map[string]any{}
		}
		actor.settings["agentMode"] = agentMode
		if actor.currentReasoningEffort == "" {
			delete(actor.settings, "reasoning.effort")
		} else {
			actor.settings["reasoning.effort"] = actor.currentReasoningEffort
		}
	}
	if actor.currentAgentMode == "" {
		actor.currentAgentMode = "smart"
	}
	agentMode = actor.currentAgentMode
	threadVersion := actor.seq
	if threadVersion <= 0 {
		threadVersion = 1
	}
	actor.mu.Unlock()

	return map[string]any{
		"threadId":         threadID,
		"wsToken":          "local-" + randomBase62(32),
		"ownerUserId":      "local-user",
		"threadVersion":    threadVersion,
		"usesDtw":          true,
		"usesThreadActors": true,
		"executorType":     omitEmpty(executorType),
		"agentMode":        agentMode,
	}, http.StatusOK
}

func neoThreadSearchPath(path string) bool {
	path = strings.TrimPrefix(path, "/api")
	path = strings.Trim(path, "/")
	return path == "threads/find"
}

func neoLocalThreadSearchResponse(q url.Values) (map[string]any, bool) {
	return neoThreadSearchResponse(context.Background(), nil, q)
}

func neoThreadSearchResponse(ctx context.Context, cfg *config.Config, q url.Values) (map[string]any, bool) {
	query := strings.TrimSpace(q.Get("q"))
	if query == "" {
		return nil, false
	}
	limit := neoQueryInt(q.Get("limit"), 10)
	if limit <= 0 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	offset := neoQueryInt(q.Get("offset"), 0)
	if offset < 0 {
		offset = 0
	}

	queryThreadIDs := neoThreadIDsFromQuery(query)
	threads, err := loadNeoLocalThreads()
	if err != nil && len(queryThreadIDs) == 0 {
		log.Debugf("amp neo local thread search unavailable: %v", err)
	}
	matches := make([]map[string]any, 0)
	seen := map[string]bool{}
	for _, thread := range threads {
		rank := neoLocalThreadSearchRank(thread, query)
		if rank == 0 {
			continue
		}
		result := neoLocalThreadSearchResult(thread, query)
		result["_rank"] = rank
		matches = append(matches, result)
		seen[stringValue(result["id"])] = true
	}
	for _, threadID := range queryThreadIDs {
		if seen[threadID] || !neoRemoteControlThreadID(threadID) {
			continue
		}
		thread, ok := loadNeoThread(ctx, cfg, threadID)
		if !ok {
			continue
		}
		result := neoLocalThreadSearchResult(thread, query)
		result["_rank"] = 500
		matches = append(matches, result)
		seen[threadID] = true
	}
	cloudAttempted := false
	cloudHasMore := false
	cloudQuery := cloneURLValues(q)
	cloudLimit := offset + limit + 20
	if cloudLimit > 100 {
		cloudLimit = 100
	}
	cloudQuery.Set("limit", strconv.Itoa(cloudLimit))
	cloudQuery.Set("offset", "0")
	if cloudResult, ok, err := getNeoCloudThreadSearch(ctx, cfg, cloudQuery); err != nil {
		log.Debugf("amp neo cloud thread search failed query=%q: %v", query, err)
	} else if ok {
		cloudAttempted = true
		cloudHasMore = boolValue(cloudResult["hasMore"])
		for index, raw := range arrayValue(cloudResult["threads"]) {
			thread := mapValue(raw)
			threadID := stringValue(thread["id"])
			if threadID == "" || seen[threadID] {
				continue
			}
			result := neoCloudThreadSearchResult(thread, query)
			result["_rank"] = 350 - index
			matches = append(matches, result)
			seen[threadID] = true
		}
	}
	if len(matches) == 0 {
		if cloudAttempted {
			return map[string]any{"threads": []any{}, "hasMore": cloudHasMore}, true
		}
		return nil, false
	}
	sort.Slice(matches, func(i, j int) bool {
		if numberFrom(matches[i]["_rank"]) != numberFrom(matches[j]["_rank"]) {
			return numberFrom(matches[i]["_rank"]) > numberFrom(matches[j]["_rank"])
		}
		return neoThreadResultUpdatedMillis(matches[i]) > neoThreadResultUpdatedMillis(matches[j])
	})
	start := offset
	if start > len(matches) {
		start = len(matches)
	}
	end := start + limit
	if end > len(matches) {
		end = len(matches)
	}
	for _, match := range matches[start:end] {
		delete(match, "_rank")
	}
	return map[string]any{
		"threads": matches[start:end],
		"hasMore": end < len(matches) || cloudHasMore,
	}, true
}

func loadNeoThread(ctx context.Context, cfg *config.Config, threadID string) (map[string]any, bool) {
	local, localOK := loadNeoLocalThread(threadID)
	if localOK && neoThreadHasUsefulContent(local) {
		return local, true
	}
	if neoRemoteControlThreadID(threadID) {
		cloud, ok, err := getNeoCloudThread(ctx, cfg, threadID)
		if err != nil {
			log.Debugf("amp neo cloud thread read failed thread=%s: %v", threadID, err)
		}
		if ok {
			if neoThreadHasUsefulContent(cloud) {
				cacheNeoLocalThread(cloud)
			}
			return cloud, true
		}
	}
	if localOK {
		return local, true
	}
	return nil, false
}

func neoThreadHasUsefulContent(thread map[string]any) bool {
	if len(arrayValue(thread["messages"])) > 0 {
		return true
	}
	if data := mapValue(thread["data"]); len(data) > 0 {
		return neoThreadHasUsefulContent(data)
	}
	return false
}

func cacheNeoLocalThread(thread map[string]any) {
	threadID := stringValue(thread["id"])
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return
	}
	dir := neoAmpThreadStoreDir()
	if dir == "" {
		return
	}
	raw, err := json.MarshalIndent(thread, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	if err := os.WriteFile(filepath.Join(dir, threadID+".json"), append(raw, '\n'), 0o600); err != nil {
		log.Debugf("amp neo cloud thread cache write failed thread=%s: %v", threadID, err)
	}
}

func neoThreadIDsFromQuery(query string) []string {
	seen := map[string]bool{}
	ids := make([]string, 0)
	for _, candidate := range neoThreadIDPattern.FindAllString(query, -1) {
		if !seen[candidate] {
			seen[candidate] = true
			ids = append(ids, candidate)
		}
	}
	split := func(r rune) bool {
		switch r {
		case ':', ',', ';', '"', '\'', '(', ')', '[', ']', '<', '>', '\n', '\t', ' ':
			return true
		default:
			return false
		}
	}
	for _, part := range strings.FieldsFunc(query, split) {
		part = strings.TrimSpace(strings.TrimPrefix(part, "@"))
		if !strings.HasPrefix(part, "T-") || !neoThreadIDExactPattern.MatchString(part) {
			continue
		}
		if !seen[part] {
			seen[part] = true
			ids = append(ids, part)
		}
	}
	return ids
}

func neoQueryInt(value string, fallback int) int {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return parsed
}

func cloneURLValues(values url.Values) url.Values {
	out := make(url.Values, len(values))
	for key, items := range values {
		out[key] = append([]string(nil), items...)
	}
	return out
}

func loadNeoLocalThreads() ([]map[string]any, error) {
	dir := neoAmpThreadStoreDir()
	if dir == "" {
		return nil, fmt.Errorf("thread store directory is empty")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	threads := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		threadID := strings.TrimSuffix(entry.Name(), ".json")
		thread, ok := loadNeoLocalThread(threadID)
		if ok {
			threads = append(threads, thread)
		}
	}
	return threads, nil
}

func neoLocalThreadMatches(thread map[string]any, query string) bool {
	return neoLocalThreadSearchRank(thread, query) > 0
}

func neoLocalThreadSearchRank(thread map[string]any, query string) int {
	terms := neoThreadSearchTerms(query)
	if len(terms) == 0 {
		return 0
	}
	threadID := strings.ToLower(stringValue(thread["id"]))
	title := strings.ToLower(stringValue(thread["title"]))
	markdown := strings.ToLower(neoThreadMarkdown(thread))
	best := 0
	for _, term := range terms {
		rank := 0
		switch {
		case term != "" && threadID == term:
			rank = 400
		case term != "" && strings.Contains(threadID, term):
			rank = 300
		case term != "" && title == term:
			rank = 250
		case term != "" && strings.Contains(title, term):
			rank = 200
		case term != "" && strings.Contains(markdown, term):
			rank = 100
		}
		if rank > best {
			best = rank
		}
	}
	return best
}

func neoThreadSearchTerms(query string) []string {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil
	}
	terms := []string{query}
	if strings.HasPrefix(query, "task:") {
		if trimmed := strings.TrimSpace(strings.TrimPrefix(query, "task:")); trimmed != "" {
			terms = append(terms, trimmed)
		}
	}
	if match := neoThreadIDPattern.FindString(query); match != "" {
		terms = append(terms, strings.ToLower(match))
	}
	return terms
}

func neoLocalThreadSearchResult(thread map[string]any, query string) map[string]any {
	messages := arrayValue(thread["messages"])
	created := numberFrom(thread["created"])
	updated := numberFrom(thread["updatedAt"])
	if updated == 0 {
		updated = created
		for _, raw := range messages {
			message := mapValue(raw)
			if value := numberFrom(message["created"]); value > updated {
				updated = value
			}
		}
	}
	return map[string]any{
		"id":                stringValue(thread["id"]),
		"title":             stringValue(thread["title"]),
		"creatorUserID":     fallbackString(thread["creatorUserID"], "local"),
		"created":           created,
		"updatedAt":         updated,
		"messageCount":      len(messages),
		"matchedSearchText": neoLocalThreadMatchedText(thread, query),
	}
}

func neoCloudThreadSearchResult(thread map[string]any, query string) map[string]any {
	result := cloneMap(thread)
	threadID := stringValue(result["id"])
	if threadID == "" {
		threadID = findThreadID(thread)
		result["id"] = threadID
	}
	if _, exists := result["title"]; !exists {
		result["title"] = ""
	}
	if _, exists := result["creatorUserID"]; !exists {
		result["creatorUserID"] = fallbackString(thread["creatorUserID"], "cloud")
	}
	if _, exists := result["messageCount"]; !exists {
		result["messageCount"] = firstNonZero(numberFrom(mapValue(thread["summaryStats"])["messageCount"]), len(arrayValue(thread["messages"])))
	}
	if _, exists := result["updatedAt"]; !exists {
		if updated := firstNonZero(numberFrom(thread["userLastInteractedAt"], thread["updated"]), neoTimeStringMillis(stringValue(thread["updated"]))); updated > 0 {
			result["updatedAt"] = updated
		}
	}
	if strings.TrimSpace(stringValue(result["matchedSearchText"])) == "" {
		result["matchedSearchText"] = neoCloudThreadMatchedText(thread, query)
	}
	return result
}

func neoLocalThreadMatchedText(thread map[string]any, query string) string {
	for _, term := range neoThreadSearchTerms(query) {
		markdown := neoThreadMarkdown(thread)
		lower := strings.ToLower(markdown)
		idx := strings.Index(lower, term)
		if idx < 0 {
			continue
		}
		start := idx - 120
		if start < 0 {
			start = 0
		}
		end := idx + len(term) + 120
		if end > len(markdown) {
			end = len(markdown)
		}
		return strings.TrimSpace(markdown[start:end])
	}
	return fallbackString(thread["title"], stringValue(thread["id"]))
}

func neoCloudThreadMatchedText(thread map[string]any, query string) string {
	if matched := strings.TrimSpace(stringValue(thread["matchedSearchText"])); matched != "" {
		return matched
	}
	if content := arrayValue(thread["firstUserMessageContent"]); len(content) > 0 {
		if text := strings.TrimSpace(textFromBlocks(content)); text != "" {
			return text
		}
	}
	if title := strings.TrimSpace(stringValue(thread["title"])); title != "" {
		return title
	}
	return stringValue(thread["id"])
}

func neoThreadRequestPath(path string) (string, bool) {
	path = strings.TrimPrefix(path, "/api")
	path = strings.TrimPrefix(path, "/")
	if !strings.HasPrefix(path, "threads/") {
		return "", false
	}
	part := strings.TrimPrefix(path, "threads/")
	part = strings.Trim(part, "/")
	if part == "" || strings.Contains(part, "/") || part == "find" {
		return "", false
	}
	markdown := strings.HasSuffix(part, ".md")
	if markdown {
		part = strings.TrimSuffix(part, ".md")
	}
	if !neoThreadIDExactPattern.MatchString(part) {
		return "", false
	}
	return part, markdown
}

func neoCloudThread(snapshot neoCloudThreadSnapshot) map[string]any {
	messages := append([]neoMessage(nil), snapshot.messages...)
	sort.Slice(messages, func(i, j int) bool { return messages[i].Seq < messages[j].Seq })

	cloudMessages := make([]any, 0, len(messages))
	version := snapshot.seq
	for _, message := range messages {
		if message.Seq > version {
			version = message.Seq
		}
		cloudMessages = append(cloudMessages, neoCloudMessage(message))
	}

	return map[string]any{
		"id":                snapshot.threadID,
		"v":                 version,
		"created":           snapshot.createdMs,
		"title":             fallbackString(snapshot.title, neoCloudTitle(messages)),
		"threadStatus":      neoThreadStatusValue(snapshot.threadStatus),
		"messages":          cloudMessages,
		"agentMode":         neoCloudAgentMode(snapshot, messages),
		"env":               neoCloudEnvironment(snapshot.environment),
		"relationships":     neoThreadRelationships(messages),
		"artifacts":         nonNilArray(snapshot.artifacts),
		"compactionRecords": nonNilArray(snapshot.compactionRecords),
		"nextMessageId":     len(cloudMessages),
		"activatedSkills":   []any{},
		"meta": map[string]any{
			"usesDtw":                  true,
			"usesThreadActors":         true,
			"ampcodeConnectorLocalNeo": true,
			"cliProxyAPILocalNeo":      true,
			"ampcodeLocalRuntime":      true,
			"ampcodeConnectorMode":     "local-neo",
		},
	}
}

func neoThreadRelationships(messages []neoMessage) []any {
	relationships := make([]any, 0)
	seen := map[string]struct{}{}
	for index, message := range messages {
		threadIDs := make([]string, 0)
		if message.Role == "assistant" {
			for _, raw := range message.Content {
				block := mapValue(raw)
				if stringValue(block["type"]) != "tool_use" || stringValue(block["name"]) != "read_thread" {
					continue
				}
				if threadID := neoToolInputThreadID(mapValue(block["input"])); threadID != "" {
					threadIDs = append(threadIDs, threadID)
				}
			}
		}
		if message.Role == "user" {
			for _, threadID := range neoThreadIDPattern.FindAllString(textFromBlocks(message.Content), -1) {
				threadIDs = append(threadIDs, threadID)
			}
		}
		for _, threadID := range threadIDs {
			if threadID == "" || threadID == message.ThreadID {
				continue
			}
			key := threadID + "\x00parent"
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			relationships = append(relationships, map[string]any{
				"threadID":     threadID,
				"type":         "mention",
				"role":         "parent",
				"messageIndex": index,
				"createdAt":    neoMessageCreatedMillis(message),
			})
		}
	}
	return relationships
}

func neoMessageCreatedMillis(message neoMessage) int64 {
	if message.CreatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, message.CreatedAt); err == nil {
			return parsed.UnixMilli()
		}
	}
	return time.Now().UnixMilli()
}

func neoCloudMessage(message neoMessage) map[string]any {
	out := map[string]any{
		"role":              message.Role,
		"content":           message.Content,
		"messageId":         message.MessageID,
		"protocolMessageID": message.MessageID,
	}
	if message.CreatedAt != "" {
		out["createdAt"] = message.CreatedAt
	}
	if message.ReadAt != "" {
		out["readAt"] = message.ReadAt
	}
	if message.CompletionStatus != "" {
		out["completionStatus"] = message.CompletionStatus
	}
	if message.ParentToolUseID != "" {
		out["parentToolUseId"] = message.ParentToolUseID
	}
	switch message.Role {
	case "user":
		if len(message.Meta) > 0 {
			out["meta"] = message.Meta
		}
		if message.UserState != nil {
			out["userState"] = message.UserState
		}
		if message.AgentMode != "" {
			out["agentMode"] = message.AgentMode
		}
		if message.ReasoningEffort != "" {
			out["reasoningEffort"] = message.ReasoningEffort
		}
	case "assistant":
		if len(message.State) > 0 {
			out["state"] = message.State
		} else {
			out["state"] = map[string]any{"type": "complete", "stopReason": "end_turn"}
		}
		if len(message.Usage) > 0 {
			out["usage"] = message.Usage
		}
	}
	return out
}

func neoCloudEnvironment(environment map[string]any) map[string]any {
	initial := map[string]any{}
	for _, key := range []string{"platform", "workspaceRoot", "workingDirectory"} {
		if value := stringValue(environment[key]); value != "" {
			initial[key] = value
		}
	}
	if trees, ok := environment["trees"].([]any); ok && len(trees) > 0 {
		initial["trees"] = trees
	}
	if len(initial) == 0 {
		return nil
	}
	return map[string]any{"initial": initial}
}

func neoCloudAgentMode(snapshot neoCloudThreadSnapshot, messages []neoMessage) string {
	if mode := stringValue(snapshot.settings["agentMode"]); mode != "" {
		return mode
	}
	for _, message := range messages {
		if message.Role == "user" && message.AgentMode != "" {
			return message.AgentMode
		}
	}
	return ""
}

func neoCloudTitle(messages []neoMessage) string {
	for _, message := range messages {
		if message.Role != "user" {
			continue
		}
		if title := neoTitleFromContent(message.Content); title != "" {
			return title
		}
	}
	return ""
}

func neoThreadMarkdown(thread map[string]any) string {
	var out strings.Builder
	threadID := stringValue(thread["id"])
	if threadID == "" {
		threadID = "unknown"
	}
	out.WriteString("---\n")
	out.WriteString("threadId: " + threadID + "\n")
	if created := numberFrom(thread["created"]); created > 0 {
		out.WriteString("created: " + time.UnixMilli(int64(created)).UTC().Format(time.RFC3339Nano) + "\n")
	}
	out.WriteString("---\n\n")
	if title := stringValue(thread["title"]); title != "" {
		out.WriteString("# " + title + "\n\n")
	}
	for _, raw := range arrayValue(thread["messages"]) {
		message := mapValue(raw)
		role := stringValue(message["role"])
		if role == "" {
			continue
		}
		out.WriteString("## " + strings.Title(role) + "\n\n")
		text := strings.TrimSpace(neoMarkdownTextFromBlocks(arrayValue(message["content"])))
		if text == "" {
			text = "[no textual content]"
		}
		out.WriteString(text + "\n\n")
	}
	return out.String()
}

func neoMarkdownTextFromBlocks(blocks []any) string {
	var out strings.Builder
	for _, raw := range blocks {
		block := mapValue(raw)
		switch stringValue(block["type"]) {
		case "text":
			if text := stringValue(block["text"]); text != "" {
				out.WriteString(text)
				out.WriteString("\n")
			}
		case "tool_use":
			name := stringValue(block["name"])
			id := stringValue(block["id"])
			out.WriteString("[tool_use")
			if name != "" {
				out.WriteString(" " + name)
			}
			if id != "" {
				out.WriteString(" " + id)
			}
			if input := mapValue(block["input"]); len(input) > 0 {
				out.WriteString(" " + clipNeoDebugJSON(input, 4096))
			}
			out.WriteString("]\n")
		case "tool_result":
			toolUseID := firstNonEmptyString(block["toolUseID"], block["tool_use_id"], block["toolCallId"])
			if toolUseID != "" {
				out.WriteString("[tool_result " + toolUseID + "]\n")
			}
			if text := runToText(block["run"]); text != "" {
				out.WriteString(text)
				out.WriteString("\n")
			}
		}
	}
	return out.String()
}

func neoTitleFromContent(content []any) string {
	text := strings.TrimSpace(textFromBlocks(content))
	if text == "" {
		return ""
	}
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 80 {
		return text[:77] + "..."
	}
	return text
}

func neoCloudCreatedMillis(record map[string]any) int64 {
	if ts := stringValue(record["create_ts"]); ts != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			return parsed.UnixMilli()
		}
	}
	return time.Now().UnixMilli()
}

func clipNeoErrorBody(body []byte) string {
	text := strings.TrimSpace(string(body))
	if len(text) > 2048 {
		return text[:2048] + "...[truncated]"
	}
	return text
}

func clipNeoDebugJSON(value any, limit int) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	text := string(raw)
	if limit > 0 && len(text) > limit {
		return text[:limit] + "...[truncated]"
	}
	return text
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (a *neoActor) inferenceRequestLocked(agentMode, reasoningEffort, parentToolCallID string) neoInferenceRequest {
	history := scopedNeoHistory(a.history, parentToolCallID)
	tools := make([]neoToolSpec, 0, len(a.tools))
	for _, tool := range a.tools {
		if deferred, _ := tool.Meta["deferred"].(bool); deferred {
			continue
		}
		if !neoToolAllowedForMode(agentMode, tool.Name) {
			continue
		}
		tools = append(tools, tool)
	}
	sort.Slice(tools, func(i, j int) bool { return tools[i].Name < tools[j].Name })
	return neoInferenceRequest{
		ActorID:          a.id,
		ThreadID:         a.threadID,
		AgentMode:        agentMode,
		ReasoningEffort:  reasoningEffort,
		ParentToolCallID: parentToolCallID,
		Settings:         cloneMap(a.settings),
		History:          history,
		Tools:            tools,
		Environment:      cloneMap(a.environment),
		Capabilities:     cloneMap(a.capabilities),
		Guidance:         cloneMap(a.guidanceSnapshot),
	}
}

func scopedNeoHistory(history []neoHistoryMessage, parentToolCallID string) []neoHistoryMessage {
	out := make([]neoHistoryMessage, 0, len(history))
	for _, message := range history {
		if message.ParentToolUseID == "" || (parentToolCallID != "" && message.ParentToolUseID == parentToolCallID) {
			out = append(out, message)
		}
	}
	return out
}

func (a *neoActor) sendSnapshot(socket *neoSocket, sinceSeq int) {
	a.mu.Lock()
	settings := cloneMap(a.settings)
	queue := make([]any, 0, len(a.queue))
	for _, item := range a.queue {
		queue = append(queue, map[string]any{"steer": item.Steer, "queuedMessage": item.protocol()})
	}
	allMessages := append([]neoMessage(nil), a.messages...)
	replayFrames := make([]neoReplayEvent, 0)
	for _, message := range a.messages {
		if message.Seq > sinceSeq {
			replayFrames = append(replayFrames, neoReplayEvent{Seq: message.Seq, Payload: neoMessageAddedPayload(message)})
		}
	}
	env := cloneMap(a.environment)
	title := a.title
	threadStatus := a.threadStatus
	compacting := a.compacting
	activeError := cloneMap(a.activeError)
	activeErrorSeq := a.activeErrorSeq
	agentState := a.agentState
	agentMode := a.currentAgentMode
	effort := a.currentReasoningEffort
	seq := a.seq
	hasExecutor := a.executorID != ""
	registeredTools := len(a.tools)
	guidanceInventory := neoGuidanceInventory(a.guidanceSnapshot)
	artifacts := a.artifactListLocked()
	compactionRecords := a.compactionRecordListLocked()
	approvals := a.approvalQueueListLocked()
	if len(approvals) > 0 {
		agentState = "awaiting_approval"
	}
	spawnedExecutorStatuses := a.spawnedExecutorStatusListLocked()
	relationships := neoThreadRelationships(allMessages)
	if activeErrorSeq > sinceSeq {
		if len(activeError) > 0 {
			replayFrames = append(replayFrames, neoReplayEvent{Seq: activeErrorSeq, Payload: map[string]any{"type": "error_set", "seq": activeErrorSeq, "error": activeError}})
		} else {
			replayFrames = append(replayFrames, neoReplayEvent{Seq: activeErrorSeq, Payload: map[string]any{"type": "error_cleared", "seq": activeErrorSeq}})
		}
	}
	if sinceSeq > 0 {
		for _, event := range a.replayEvents {
			if event.Seq > sinceSeq {
				replayFrames = append(replayFrames, neoReplayEvent{Seq: event.Seq, Payload: cloneNeoProtocolPayload(event.Payload)})
			}
		}
	}
	a.mu.Unlock()
	sort.SliceStable(replayFrames, func(i, j int) bool { return replayFrames[i].Seq < replayFrames[j].Seq })

	send := func(payload any) {
		if socket != nil {
			socket.send(payload)
			return
		}
		a.broadcast(payload)
	}
	send(map[string]any{"type": "thread_settings", "settings": settings})
	send(map[string]any{"type": "queued_messages", "messages": queue})
	send(toolApprovalQueuePayload(approvals))
	send(map[string]any{"type": "observers", "count": len(a.socketList()), "observers": []any{}, "hasExecutor": hasExecutor})
	for _, status := range spawnedExecutorStatuses {
		send(status)
	}
	if hasExecutor {
		send(map[string]any{"type": "executor_connected", "executorId": a.executorID, "registeredToolCount": registeredTools, "guidanceInventory": guidanceInventory, "resumeBootstrap": false})
	}
	if title != "" {
		send(map[string]any{"type": "thread_title", "title": title})
	}
	send(map[string]any{"type": "thread_status", "status": neoThreadStatusValue(threadStatus)})
	if len(env) > 0 {
		send(map[string]any{"type": "environment_update", "environment": env})
	}
	if compacting {
		send(map[string]any{"type": "compaction_started"})
	}
	for _, frame := range replayFrames {
		send(frame.Payload)
	}
	send(map[string]any{"type": "thread_relationships", "seq": seq, "relationships": relationships})
	send(map[string]any{"type": "compaction_records", "records": compactionRecords})
	send(map[string]any{"type": "artifacts_snapshot", "artifacts": artifacts})
	send(map[string]any{"type": "agent_state", "state": agentState, "agentMode": agentMode, "reasoningEffort": omitEmpty(effort)})
}

func (a *neoActor) sendExecutorConnected(socket *neoSocket, resumeBootstrap bool) {
	a.mu.Lock()
	executorID := a.executorID
	if executorID == "" {
		executorID = "local-executor"
	}
	count := len(a.tools)
	guidanceInventory := neoGuidanceInventory(a.guidanceSnapshot)
	a.mu.Unlock()
	payload := map[string]any{"type": "executor_connected", "executorId": executorID, "registeredToolCount": count, "guidanceInventory": guidanceInventory, "resumeBootstrap": resumeBootstrap}
	if socket != nil {
		socket.send(payload)
	} else {
		a.broadcast(payload)
	}
}

func (a *neoActor) setAgentState(state, messageID, agentMode, reasoningEffort string) {
	a.mu.Lock()
	a.agentState = state
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "agent_state", "state": state, "messageId": omitEmpty(messageID), "agentMode": agentMode, "reasoningEffort": omitEmpty(reasoningEffort)})
}

func (a *neoActor) ensureThreadTitle(content []any) {
	title := neoTitleFromContent(content)
	shouldGenerate := false
	broadcastTitle := ""
	a.mu.Lock()
	if a.title == "" && title != "" {
		a.title = title
		a.titleSource = "heuristic"
		broadcastTitle = title
	}
	if !a.titleGenerationStarted && a.titleSource != "explicit" && title != "" && neoTitleGenerationEnabled(a.configSnapshot()) {
		a.titleGenerationStarted = true
		shouldGenerate = true
	}
	a.mu.Unlock()
	if broadcastTitle != "" {
		a.broadcast(map[string]any{"type": "thread_title", "title": broadcastTitle})
	}
	if shouldGenerate {
		go a.generateThreadTitle()
	}
}

func (a *neoActor) generateThreadTitle() {
	request, route, ok := a.titleGenerationRequest()
	if !ok {
		return
	}
	result, err := inferNeoTitleLocal(a.runtime, request, route)
	if err != nil {
		log.Debugf("amp neo local runtime title generation failed thread=%s: %v", request.ThreadID, err)
		return
	}
	title := sanitizeNeoGeneratedTitle(result)
	if title == "" {
		return
	}

	a.mu.Lock()
	if a.titleSource == "explicit" || a.title == title {
		a.mu.Unlock()
		return
	}
	a.title = title
	a.titleSource = "generated"
	a.mu.Unlock()

	a.broadcast(map[string]any{"type": "thread_title", "title": title})
	a.syncCloudAsync()
}

func (a *neoActor) titleGenerationRequest() (neoInferenceRequest, neoModelRoute, bool) {
	cfg := a.configSnapshot()
	if !neoTitleGenerationEnabled(cfg) {
		return neoInferenceRequest{}, neoModelRoute{}, false
	}

	a.mu.Lock()
	history := append([]neoHistoryMessage(nil), a.history...)
	settings := cloneMap(a.settings)
	threadID := a.threadID
	agentMode := a.currentAgentMode
	environment := cloneMap(a.environment)
	a.mu.Unlock()

	if threadID == "" || len(history) == 0 {
		return neoInferenceRequest{}, neoModelRoute{}, false
	}
	if agentMode == "" {
		agentMode = "smart"
	}
	titleHistory := neoTitleHistory(scopedNeoHistory(history, ""))
	if len(titleHistory) == 0 {
		return neoInferenceRequest{}, neoModelRoute{}, false
	}
	request := neoInferenceRequest{
		ThreadID:        threadID,
		AgentMode:       agentMode,
		ReasoningEffort: "low",
		Settings:        settings,
		History:         titleHistory,
		Environment:     environment,
	}
	return request, selectNeoTitleRoute(cfg, request.AgentMode, settings), true
}

func (a *neoActor) configSnapshot() *config.Config {
	if a == nil || a.runtime == nil {
		return nil
	}
	return a.runtime.configSnapshot()
}

func (a *neoActor) setTitle(title string) {
	title = strings.TrimSpace(title)
	a.mu.Lock()
	if a.title == title {
		a.mu.Unlock()
		return
	}
	a.title = title
	a.titleSource = "explicit"
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "thread_title", "title": title})
	a.syncCloudAsync()
}

func (a *neoActor) markMessageRead(messageID string, read bool) {
	if messageID == "" {
		return
	}

	a.mu.Lock()
	index := -1
	for i, message := range a.messages {
		if message.MessageID == messageID {
			index = i
			break
		}
	}
	if index < 0 {
		a.mu.Unlock()
		return
	}

	message := a.messages[index]
	if message.Role != "user" && message.Role != "assistant" {
		a.mu.Unlock()
		return
	}
	if read {
		if message.ReadAt != "" {
			a.mu.Unlock()
			return
		}
		message.ReadAt = time.Now().UTC().Format(time.RFC3339Nano)
	} else {
		if message.ReadAt == "" {
			a.mu.Unlock()
			return
		}
		message.ReadAt = ""
	}
	a.messages[index] = message
	if message.Seq == 0 {
		message.Seq = a.nextSeqLocked()
		a.messages[index] = message
	}
	updateSeq := a.nextSeqLocked()
	updateEvent := map[string]any{"type": "message_updated", "message": message.protocol(), "seq": updateSeq}
	a.rememberReplayEventLocked(updateEvent)
	a.mu.Unlock()

	a.broadcast(updateEvent)
	a.syncCloudAsync()
}

func (a *neoActor) importThread(thread map[string]any) error {
	return a.importThreadWithSync(thread, true)
}

func (a *neoActor) importThreadLocalOnly(thread map[string]any) error {
	return a.importThreadWithSync(thread, false)
}

func (a *neoActor) importThreadWithSync(thread map[string]any, syncCloud bool) error {
	threadID := firstNonEmptyString(thread["id"], findThreadID(thread))
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return fmt.Errorf("invalid thread id %q", threadID)
	}

	rawMessages := arrayValue(thread["messages"])
	messages := make([]neoMessage, 0, len(rawMessages))
	for i, raw := range rawMessages {
		message := neoMessageFromImportedThread(threadID, raw, i)
		if message.Role == "" {
			continue
		}
		messages = append(messages, message)
	}

	agentMode := firstNonEmptyString(neoImportedThreadAgentMode(messages), thread["agentMode"], nestedString(thread["settings"], "agentMode"))
	if agentMode == "" {
		agentMode = "smart"
	}
	title := stringValue(thread["title"])
	if title == "" {
		title = neoCloudTitle(messages)
	}
	artifacts := neoArtifactsMap(thread["artifacts"])
	threadStatus := normalizedNeoThreadStatus(firstNonEmptyString(thread["threadStatus"], thread["status"]))
	compactionRecords := normalizeNeoCompactionRecords(firstArray(thread["compactionRecords"], thread["compaction_records"]))
	approvalQueue := neoRestoredApprovalQueue(messages)
	version := numberFrom(thread["v"])
	if version < len(messages)+1 {
		version = len(messages) + 1
	}

	a.mu.Lock()
	a.threadID = threadID
	a.key = fallbackString(a.key, threadID)
	if syncCloud {
		a.touchLocked()
	}
	a.title = title
	a.threadStatus = threadStatus
	a.messages = messages
	a.artifacts = artifacts
	a.compactionRecords = compactionRecords
	a.pendingTools = map[string]neoPendingTool{}
	a.approvalQueue = approvalQueue
	a.replayEvents = nil
	a.activeError = nil
	a.activeErrorSeq = 0
	a.remoteSeenMessages = map[string]bool{}
	for _, message := range messages {
		if message.MessageID != "" {
			a.remoteSeenMessages[message.MessageID] = true
		}
	}
	a.remoteControlStarted = len(messages) > 0
	a.queue = nil
	if len(approvalQueue) > 0 {
		a.agentState = "awaiting_approval"
	} else {
		a.agentState = "idle"
	}
	a.currentAgentMode = agentMode
	a.currentReasoningEffort = neoImportedThreadReasoningEffort(messages, agentMode)
	if a.settings == nil {
		a.settings = map[string]any{}
	}
	a.settings["agentMode"] = agentMode
	if a.currentReasoningEffort == "" {
		delete(a.settings, "reasoning.effort")
	} else {
		a.settings["reasoning.effort"] = a.currentReasoningEffort
	}
	if env := mapValue(thread["env"]); len(env) > 0 {
		a.environment = cloneMap(env)
	}
	a.seq = version
	a.rebuildHistoryLocked()
	a.mu.Unlock()

	a.sendSnapshot(nil, 0)
	if syncCloud {
		a.syncCloudAsync()
	}
	return nil
}

func neoImportedThreadAgentMode(messages []neoMessage) string {
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		if message.Role == "user" && strings.TrimSpace(message.AgentMode) != "" {
			return message.AgentMode
		}
	}
	return ""
}

func neoImportedThreadReasoningEffort(messages []neoMessage, agentMode string) string {
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		if message.Role != "user" || strings.TrimSpace(message.ReasoningEffort) == "" {
			continue
		}
		if message.AgentMode != "" && !strings.EqualFold(message.AgentMode, agentMode) {
			continue
		}
		if neoReasoningEffortAllowedForMode(agentMode, message.ReasoningEffort) {
			return strings.ToLower(strings.TrimSpace(message.ReasoningEffort))
		}
	}
	return defaultNeoReasoningEffort(agentMode)
}

func neoArtifactsMap(raw any) map[string]any {
	out := map[string]any{}
	add := func(key string, value any) {
		artifact := cloneMap(mapValue(value))
		if len(artifact) == 0 && value != nil {
			artifact = map[string]any{"value": value}
		}
		if len(artifact) == 0 {
			return
		}
		if key == "" {
			key = firstNonEmptyString(artifact["key"], artifact["id"], artifact["path"])
		}
		if key == "" {
			key = "artifact-" + randomBase62(12)
		}
		if _, exists := artifact["key"]; !exists {
			artifact["key"] = key
		}
		out[key] = artifact
	}

	if items := arrayValue(raw); items != nil {
		for _, item := range items {
			add("", item)
		}
		return out
	}
	for key, value := range mapValue(raw) {
		add(key, value)
	}
	return out
}

func neoMessageFromImportedThread(threadID string, raw any, index int) neoMessage {
	message := mapValue(raw)
	role := stringValue(message["role"])
	if role == "" {
		return neoMessage{}
	}
	messageID := messageIDValue(message["messageId"])
	if messageID == "" {
		messageID = messageIDValue(message["protocolMessageID"])
	}
	if messageID == "" {
		messageID = fmt.Sprintf("M-import-%d", index)
	}
	content := arrayValue(message["content"])
	if content == nil {
		content = []any{}
	}
	return neoMessage{
		ThreadID:         threadID,
		MessageID:        messageID,
		Role:             role,
		Content:          content,
		ParentToolUseID:  firstNonEmptyString(message["parentToolUseId"], message["parentToolUseID"], message["parent_tool_use_id"]),
		AgentMode:        stringValue(message["agentMode"]),
		ReasoningEffort:  firstNonEmptyString(message["reasoningEffort"], message["reasoning_effort"]),
		CreatedAt:        stringValue(message["createdAt"]),
		ReadAt:           stringValue(message["readAt"]),
		Meta:             mapValue(message["meta"]),
		UserState:        message["userState"],
		State:            mapValue(message["state"]),
		Usage:            mapValue(message["usage"]),
		Seq:              index + 1,
		CompletionStatus: stringValue(message["completionStatus"]),
	}
}

func (a *neoActor) processQueue() {
	a.mu.Lock()
	if a.agentState != "idle" || len(a.queue) == 0 {
		a.mu.Unlock()
		return
	}
	nextIndex := 0
	for i, item := range a.queue {
		if item.Steer {
			nextIndex = i
			break
		}
	}
	next := a.queue[nextIndex]
	a.queue = append(a.queue[:nextIndex], a.queue[nextIndex+1:]...)
	seq := a.nextSeqLocked()
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "queued_message_dequeued", "queuedMessageId": next.MessageID, "seq": seq})
	a.startUserMessage(next)
}

func (a *neoActor) removeQueuedMessage(messageID string) {
	a.mu.Lock()
	filtered := a.queue[:0]
	for _, item := range a.queue {
		if item.MessageID != messageID {
			filtered = append(filtered, item)
		}
	}
	a.queue = filtered
	seq := a.nextSeqLocked()
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "queued_message_removed", "queuedMessageId": messageID, "seq": seq})
}

func (a *neoActor) steerQueuedMessage(messageID string) {
	a.mu.Lock()
	index := -1
	for i, item := range a.queue {
		if item.MessageID == messageID {
			index = i
			break
		}
	}
	if index < 0 {
		a.mu.Unlock()
		return
	}
	item := a.queue[index]
	item.Steer = true
	a.queue = append(a.queue[:index], a.queue[index+1:]...)
	a.queue = append([]neoQueuedMessage{item}, a.queue...)
	messages := make([]any, 0, len(a.queue))
	for _, item := range a.queue {
		messages = append(messages, map[string]any{"steer": item.Steer, "queuedMessage": item.protocol()})
	}
	shouldProcess := a.agentState == "idle" && a.executorReady
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "queued_messages", "messages": messages})
	if shouldProcess {
		a.processQueue()
	}
}

func (a *neoActor) retry() {
	a.mu.Lock()
	if a.agentState != "idle" {
		a.retryScheduled = false
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "retry_cancelled"})
		return
	}
	mode := a.currentAgentMode
	if mode == "" {
		mode = a.agentModeLocked()
	}
	effort := a.currentReasoningEffort
	if !neoReasoningEffortAllowedForMode(mode, effort) {
		effort = a.reasoningEffortForModeLocked(mode)
	}
	a.retryScheduled = false
	seq := a.nextSeqLocked()
	a.activeError = nil
	a.activeErrorSeq = seq
	a.mu.Unlock()

	a.broadcast(map[string]any{"type": "retry_started"})
	a.broadcast(map[string]any{"type": "error_cleared", "seq": seq})
	go a.runInference(mode, effort)
}

func (a *neoActor) handleRetryEvent(msg map[string]any) {
	switch msg["type"] {
	case "retry_scheduled":
		payload := normalizeNeoRetryScheduled(msg)
		a.mu.Lock()
		a.retryScheduled = true
		a.mu.Unlock()
		a.broadcast(payload)
	case "retry_started":
		a.mu.Lock()
		a.retryScheduled = false
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "retry_started"})
	case "retry_cancelled":
		a.mu.Lock()
		a.retryScheduled = false
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "retry_cancelled"})
	}
}

func (a *neoActor) updateThreadStatus(msg map[string]any) {
	status := normalizedNeoThreadStatus(stringValue(msg["status"]))
	a.mu.Lock()
	a.threadStatus = status
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "thread_status", "status": neoThreadStatusValue(status)})
	a.syncCloudAsync()
}

func (a *neoActor) handleCompactionEvent(msg map[string]any) {
	switch msg["type"] {
	case "compaction_started":
		a.mu.Lock()
		a.compacting = true
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "compaction_started"})
	case "compaction_complete":
		record, ok := neoCompactionRecord(msg, time.Now().UTC().Format(time.RFC3339Nano))
		if !ok {
			return
		}
		a.mu.Lock()
		a.compacting = false
		a.upsertCompactionRecordLocked(record)
		records := a.compactionRecordListLocked()
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "compaction_complete", "cutMessageId": record["cutMessageId"]})
		a.broadcast(map[string]any{"type": "compaction_records", "records": records})
		a.syncCloudAsync()
	case "compaction_records":
		records := normalizeNeoCompactionRecords(msg["records"])
		a.mu.Lock()
		a.compactionRecords = records
		payload := a.compactionRecordListLocked()
		a.mu.Unlock()
		a.broadcast(map[string]any{"type": "compaction_records", "records": payload})
		a.syncCloudAsync()
	}
}

func (a *neoActor) cancel() {
	a.mu.Lock()
	a.generation++
	pending := a.pendingToolIDsLocked()
	updateEvents := a.cancelToolResultMessagesLocked(pending, "user:cancelled")
	a.pendingTools = map[string]neoPendingTool{}
	hadApprovals := len(a.approvalQueue) > 0
	a.approvalQueue = nil
	retryScheduled := a.retryScheduled
	a.retryScheduled = false
	messageID := ""
	if len(a.messages) > 0 {
		messageID = a.messages[len(a.messages)-1].MessageID
	}
	seq := a.nextSeqLocked()
	a.activeError = nil
	a.activeErrorSeq = 0
	cancelEvent := map[string]any{"type": "cancelled", "seq": seq, "messageId": omitEmpty(messageID)}
	a.rememberReplayEventLocked(cancelEvent)
	a.mu.Unlock()
	for _, toolCallID := range pending {
		a.broadcast(map[string]any{"type": "executor_tool_lease_revoked", "toolCallId": toolCallID, "reason": "user_canceled"})
	}
	if hadApprovals {
		a.broadcast(toolApprovalQueuePayload(nil))
	}
	if retryScheduled {
		a.broadcast(map[string]any{"type": "retry_cancelled"})
	}
	for _, event := range updateEvents {
		a.broadcast(event)
	}
	a.broadcast(cancelEvent)
	if messageID != "" {
		a.broadcast(map[string]any{"type": "delta", "messageId": messageID, "role": "assistant", "state": "aborted"})
	}
	a.setAgentState("idle", messageID, a.currentAgentMode, a.currentReasoningEffort)
	a.processQueue()
}

func (a *neoActor) fail(err error) {
	if err == nil {
		return
	}
	log.Errorf("amp neo local actor error: %v", err)
	a.mu.Lock()
	seq := a.nextSeqLocked()
	errorPayload := map[string]any{"message": err.Error(), "code": "INTERNAL_ERROR"}
	a.activeError = cloneMap(errorPayload)
	a.activeErrorSeq = seq
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "error", "message": err.Error(), "code": "INTERNAL_ERROR"})
	a.broadcast(map[string]any{"type": "error_set", "seq": seq, "error": errorPayload})
}

func (a *neoActor) clearActiveError(msg map[string]any) {
	requestedSeq := intValue(msg["seq"])
	a.mu.Lock()
	if requestedSeq > 0 && a.activeErrorSeq > 0 && requestedSeq != a.activeErrorSeq {
		a.mu.Unlock()
		return
	}
	seq := a.nextSeqLocked()
	a.activeError = nil
	a.activeErrorSeq = seq
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "error_cleared", "seq": seq})
}

func (a *neoActor) upsertArtifact(raw any) {
	artifact := cloneMap(mapValue(raw))
	if len(artifact) == 0 && raw != nil {
		artifact = map[string]any{"value": raw}
	}
	key := firstNonEmptyString(artifact["key"], artifact["id"], artifact["path"])
	if key == "" {
		key = "artifact-" + randomBase62(12)
		artifact["key"] = key
	}
	a.mu.Lock()
	if a.artifacts == nil {
		a.artifacts = map[string]any{}
	}
	a.artifacts[key] = artifact
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "artifact_upserted", "artifact": artifact})
	a.syncCloudAsync()
}

func (a *neoActor) deleteArtifact(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}
	a.mu.Lock()
	delete(a.artifacts, key)
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "artifact_deleted", "key": key})
	a.syncCloudAsync()
}

func (a *neoActor) upsertNotificationSubscription(msg map[string]any) {
	subscription := cloneMap(mapValue(msg["subscription"]))
	if len(subscription) == 0 {
		subscription = cloneMap(msg)
		delete(subscription, "type")
	}
	id := firstNonEmptyString(subscription["id"], subscription["subscriptionId"], msg["subscriptionId"], msg["requestId"], "default")
	a.mu.Lock()
	if a.notificationSubs == nil {
		a.notificationSubs = map[string]map[string]any{}
	}
	a.notificationSubs[id] = subscription
	a.mu.Unlock()
}

func (a *neoActor) broadcastObservers() {
	a.mu.Lock()
	count := len(a.sockets)
	hasExecutor := a.executorID != ""
	a.mu.Unlock()
	a.broadcast(map[string]any{"type": "observers", "count": count, "observers": []any{}, "hasExecutor": hasExecutor})
}

func (a *neoActor) broadcast(payload any) {
	for _, socket := range a.socketList() {
		socket.send(payload)
	}
}

func (a *neoActor) socketList() []*neoSocket {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.socketListLocked()
}

func (a *neoActor) socketListLocked() []*neoSocket {
	sockets := make([]*neoSocket, 0, len(a.sockets))
	for socket := range a.sockets {
		sockets = append(sockets, socket)
	}
	return sockets
}

func (a *neoActor) spawnedExecutorListLocked() []*neoSpawnedExecutor {
	executors := make([]*neoSpawnedExecutor, 0, len(a.spawnedExecutors))
	for _, executor := range a.spawnedExecutors {
		if executor != nil {
			executors = append(executors, executor)
		}
	}
	return executors
}

func (a *neoActor) spawnedExecutorStatusListLocked() []any {
	if len(a.spawnedExecutors) == 0 {
		return []any{}
	}
	spawnIDs := make([]string, 0, len(a.spawnedExecutors))
	for spawnID := range a.spawnedExecutors {
		spawnIDs = append(spawnIDs, spawnID)
	}
	sort.Strings(spawnIDs)
	statuses := make([]any, 0, len(spawnIDs))
	for _, spawnID := range spawnIDs {
		spawned := a.spawnedExecutors[spawnID]
		if spawned == nil {
			continue
		}
		statuses = append(statuses, normalizeNeoExecutorStatus(map[string]any{
			"type":    "executor_status",
			"spawnId": spawnID,
			"status":  "running",
			"message": "Waiting for local Amp headless executor to connect.",
			"details": map[string]any{
				"reasonCode": "waiting_for_executor_connect",
				"threadId":   spawned.threadID,
				"pid":        spawned.pid(),
				"logFile":    omitEmpty(spawned.logPath),
				"startedAt":  spawned.startedAt.UTC().Format(time.RFC3339Nano),
			},
		}))
	}
	return statuses
}

func (a *neoActor) artifactListLocked() []any {
	if len(a.artifacts) == 0 {
		return []any{}
	}
	keys := make([]string, 0, len(a.artifacts))
	for key := range a.artifacts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	artifacts := make([]any, 0, len(keys))
	for _, key := range keys {
		if artifact, ok := a.artifacts[key].(map[string]any); ok {
			artifacts = append(artifacts, cloneMap(artifact))
		} else {
			artifacts = append(artifacts, a.artifacts[key])
		}
	}
	return artifacts
}

func (a *neoActor) upsertCompactionRecordLocked(record map[string]any) {
	cutMessageID := stringValue(record["cutMessageId"])
	if cutMessageID == "" {
		return
	}
	for i, existing := range a.compactionRecords {
		if stringValue(existing["cutMessageId"]) == cutMessageID {
			a.compactionRecords[i] = cloneMap(record)
			return
		}
	}
	a.compactionRecords = append(a.compactionRecords, cloneMap(record))
}

func (a *neoActor) compactionRecordListLocked() []any {
	if len(a.compactionRecords) == 0 {
		return []any{}
	}
	records := make([]any, 0, len(a.compactionRecords))
	for _, record := range a.compactionRecords {
		records = append(records, cloneMap(record))
	}
	return records
}

func neoRestoredApprovalQueue(messages []neoMessage) []map[string]any {
	type restoredToolUse struct {
		Name             string
		Input            map[string]any
		ParentToolCallID string
	}

	toolUses := map[string]restoredToolUse{}
	for _, message := range messages {
		if message.Role != "assistant" {
			continue
		}
		for _, rawBlock := range message.Content {
			block := mapValue(rawBlock)
			if stringValue(block["type"]) != "tool_use" {
				continue
			}
			toolCallID := firstNonEmptyString(block["id"], block["toolUseID"], block["tool_use_id"], block["toolCallId"])
			toolName := stringValue(block["name"])
			if toolCallID == "" || toolName == "" {
				continue
			}
			toolUses[toolCallID] = restoredToolUse{
				Name:             toolName,
				Input:            mapValue(block["input"]),
				ParentToolCallID: firstNonEmptyString(block["parentToolCallId"], block["parentToolUseId"], message.ParentToolUseID),
			}
		}
	}

	approvals := make([]map[string]any, 0)
	seen := map[string]bool{}
	fallbackTimestamp := int(time.Now().UnixMilli())
	for _, message := range messages {
		if message.Role != "user" {
			continue
		}
		for _, rawBlock := range message.Content {
			block := mapValue(rawBlock)
			if stringValue(block["type"]) != "tool_result" {
				continue
			}
			run := mapValue(block["run"])
			if strings.ToLower(strings.TrimSpace(stringValue(run["status"]))) != "blocked-on-user" {
				continue
			}
			toolCallID := firstNonEmptyString(block["toolUseID"], block["toolUseId"], block["tool_use_id"], block["toolCallId"])
			if toolCallID == "" || seen[toolCallID] {
				continue
			}
			seen[toolCallID] = true
			toolUse := toolUses[toolCallID]
			toolName := firstNonEmptyString(block["toolName"], run["toolName"], toolUse.Name)
			if toolName == "" {
				toolName = "unknown"
			}
			args := firstMap(block["args"], run["args"], toolUse.Input)
			parentToolCallID := firstNonEmptyString(block["parentToolCallId"], block["parentToolUseId"], run["parentToolCallId"], run["parentToolUseId"], message.ParentToolUseID, toolUse.ParentToolCallID)
			contextValue := stringValue(firstNonNil(block["context"], run["context"]))
			if contextValue != "thread" && contextValue != "subagent" {
				if parentToolCallID != "" {
					contextValue = "subagent"
				} else {
					contextValue = "thread"
				}
			}
			timestamp := numberFrom(block["timestamp"], run["timestamp"])
			if timestamp <= 0 {
				timestamp = fallbackTimestamp
			}
			approval := map[string]any{
				"id":         fallbackString(block["id"], toolCallID),
				"toolCallId": toolCallID,
				"toolUseId":  toolCallID,
				"toolName":   toolName,
				"args":       cloneMap(args),
				"context":    contextValue,
				"timestamp":  timestamp,
			}
			if reason := firstNonEmptyString(block["reason"], run["reason"]); reason != "" {
				approval["reason"] = reason
			}
			if toAllow := stringArrayValue(firstNonNil(block["toAllow"], run["toAllow"])); len(toAllow) > 0 {
				approval["toAllow"] = toAllow
			}
			if subagentToolName := firstNonEmptyString(block["subagentToolName"], run["subagentToolName"]); subagentToolName != "" {
				approval["subagentToolName"] = subagentToolName
			}
			if parentToolCallID != "" {
				approval["parentToolCallId"] = parentToolCallID
			}
			if matchedRule := mapValue(firstNonNil(block["matchedRule"], run["matchedRule"])); len(matchedRule) > 0 {
				approval["matchedRule"] = cloneMap(matchedRule)
			}
			if ruleSource := stringValue(firstNonNil(block["ruleSource"], run["ruleSource"])); ruleSource == "user" || ruleSource == "built-in" {
				approval["ruleSource"] = ruleSource
			}
			approvals = append(approvals, approval)
		}
	}
	return approvals
}

func (a *neoActor) cancelToolResultMessagesLocked(toolCallIDs []string, reason string) []map[string]any {
	if len(toolCallIDs) == 0 || len(a.messages) == 0 {
		return nil
	}
	ids := make(map[string]struct{}, len(toolCallIDs))
	for _, toolCallID := range toolCallIDs {
		if toolCallID != "" {
			ids[toolCallID] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil
	}

	updates := make([]map[string]any, 0)
	for i, message := range a.messages {
		if message.Role != "user" || len(message.Content) == 0 {
			continue
		}
		content := make([]any, len(message.Content))
		changed := false
		for blockIndex, rawBlock := range message.Content {
			block, ok := asMap(rawBlock)
			if !ok {
				content[blockIndex] = rawBlock
				continue
			}
			clonedBlock := cloneMap(block)
			if stringValue(clonedBlock["type"]) != "tool_result" {
				content[blockIndex] = clonedBlock
				continue
			}
			toolCallID := firstNonEmptyString(clonedBlock["toolUseID"], clonedBlock["toolUseId"], clonedBlock["tool_use_id"], clonedBlock["toolCallId"])
			if _, ok := ids[toolCallID]; !ok {
				content[blockIndex] = clonedBlock
				continue
			}
			run := cloneMap(mapValue(clonedBlock["run"]))
			if neoToolRunTerminal(run) {
				content[blockIndex] = clonedBlock
				continue
			}
			run["status"] = "cancelled"
			if reason != "" {
				run["reason"] = reason
			}
			clonedBlock["run"] = run
			content[blockIndex] = clonedBlock
			changed = true
		}
		if !changed {
			continue
		}
		message.Content = content
		message.CompletionStatus = ""
		a.messages[i] = message
		seq := a.nextSeqLocked()
		event := map[string]any{"type": "message_updated", "message": message.protocol(), "seq": seq}
		a.rememberReplayEventLocked(event)
		updates = append(updates, event)
	}
	if len(updates) > 0 {
		a.rebuildHistoryLocked()
	}
	return updates
}

func neoCancelledToolRunReason(leaseReason string) string {
	switch leaseReason {
	case "executor_disconnected", "reassigned":
		return "system:disposed"
	default:
		return "user:cancelled"
	}
}

func (a *neoActor) normalizeToolApprovalRequestLocked(msg map[string]any) (map[string]any, bool) {
	raw := cloneMap(mapValue(msg["approval"]))
	if len(raw) == 0 {
		raw = cloneMap(msg)
		delete(raw, "type")
		delete(raw, "approval")
	}
	toolCallID := firstNonEmptyString(raw["toolCallId"], raw["toolUseId"], raw["toolUseID"], raw["tool_use_id"], msg["toolCallId"], msg["toolUseId"], msg["toolUseID"], msg["tool_use_id"])
	if toolCallID == "" {
		return nil, false
	}
	pending := a.pendingTools[toolCallID]
	toolName := firstNonEmptyString(raw["toolName"], raw["name"], msg["toolName"], msg["name"], pending.Name)
	if toolName == "" {
		toolName = "unknown"
	}
	args := firstMap(raw["args"], raw["input"], msg["args"], msg["input"], pending.Input)
	contextValue := stringValue(raw["context"])
	parentToolCallID := firstNonEmptyString(raw["parentToolCallId"], raw["parentToolUseId"], raw["parent_tool_use_id"], msg["parentToolCallId"], msg["parentToolUseId"], pending.ParentToolCallID)
	subagentToolName := firstNonEmptyString(raw["subagentToolName"], raw["subagentName"], msg["subagentToolName"], msg["subagentName"])
	if contextValue != "thread" && contextValue != "subagent" {
		if parentToolCallID != "" || subagentToolName != "" {
			contextValue = "subagent"
		} else {
			contextValue = "thread"
		}
	}
	timestamp := numberFrom(raw["timestamp"], msg["timestamp"])
	if timestamp <= 0 {
		timestamp = int(time.Now().UnixMilli())
	}
	approval := map[string]any{
		"id":         fallbackString(raw["id"], toolCallID),
		"toolCallId": toolCallID,
		"toolUseId":  toolCallID,
		"toolName":   toolName,
		"args":       cloneMap(args),
		"context":    contextValue,
		"timestamp":  timestamp,
	}
	if reason := firstNonEmptyString(raw["reason"], msg["reason"]); reason != "" {
		approval["reason"] = reason
	}
	if toAllow := stringArrayValue(firstNonNil(raw["toAllow"], msg["toAllow"])); len(toAllow) > 0 {
		approval["toAllow"] = toAllow
	}
	if subagentToolName != "" {
		approval["subagentToolName"] = subagentToolName
	}
	if parentToolCallID != "" {
		approval["parentToolCallId"] = parentToolCallID
	}
	if matchedRule := mapValue(firstNonNil(raw["matchedRule"], msg["matchedRule"])); len(matchedRule) > 0 {
		approval["matchedRule"] = cloneMap(matchedRule)
	}
	if ruleSource := stringValue(firstNonNil(raw["ruleSource"], msg["ruleSource"])); ruleSource == "user" || ruleSource == "built-in" {
		approval["ruleSource"] = ruleSource
	}
	return approval, true
}

func (a *neoActor) upsertApprovalLocked(approval map[string]any) {
	key := neoApprovalKey(approval)
	if key == "" {
		return
	}
	for i, existing := range a.approvalQueue {
		if neoApprovalKey(existing) == key {
			a.approvalQueue[i] = cloneMap(approval)
			return
		}
	}
	a.approvalQueue = append(a.approvalQueue, cloneMap(approval))
}

func (a *neoActor) removeApprovalLocked(toolCallID string) bool {
	if toolCallID == "" || len(a.approvalQueue) == 0 {
		return false
	}
	removed := false
	filtered := a.approvalQueue[:0]
	for _, approval := range a.approvalQueue {
		key := neoApprovalKey(approval)
		if key == toolCallID || stringValue(approval["id"]) == toolCallID {
			removed = true
			continue
		}
		filtered = append(filtered, approval)
	}
	if removed {
		a.approvalQueue = filtered
	}
	return removed
}

func (a *neoActor) approvalQueueListLocked() []any {
	if len(a.approvalQueue) == 0 {
		return []any{}
	}
	approvals := make([]any, 0, len(a.approvalQueue))
	for _, approval := range a.approvalQueue {
		approvals = append(approvals, cloneMap(approval))
	}
	return approvals
}

func (a *neoActor) storeMessage(message neoMessage) neoMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.storeMessageLocked(message)
}

func (a *neoActor) storeMessageLocked(message neoMessage) neoMessage {
	for i, existing := range a.messages {
		if existing.MessageID == message.MessageID {
			message.Seq = existing.Seq
			a.messages[i] = message
			return message
		}
	}
	message.Seq = a.nextSeqLocked()
	a.messages = append(a.messages, message)
	return message
}

func (a *neoActor) storeMessageEventLocked(message neoMessage) (neoMessage, map[string]any) {
	for i, existing := range a.messages {
		if existing.MessageID != message.MessageID {
			continue
		}
		if message.CreatedAt == "" {
			message.CreatedAt = existing.CreatedAt
		}
		if message.ParentToolUseID == "" {
			message.ParentToolUseID = existing.ParentToolUseID
		}
		message.Seq = existing.Seq
		a.messages[i] = message
		seq := a.nextSeqLocked()
		event := map[string]any{"type": "message_updated", "message": message.protocol(), "seq": seq}
		a.rememberReplayEventLocked(event)
		return message, event
	}
	stored := a.storeMessageLocked(message)
	return stored, neoMessageAddedPayload(stored)
}

func (a *neoActor) toolResultRunLocked(toolCallID string) (map[string]any, any) {
	messageID := toolResultMessageID(toolCallID)
	for _, message := range a.messages {
		if message.MessageID != messageID {
			continue
		}
		for _, raw := range message.Content {
			block := mapValue(raw)
			if stringValue(block["type"]) != "tool_result" {
				continue
			}
			if firstNonEmptyString(block["toolUseID"], block["tool_use_id"], block["toolCallId"]) != toolCallID {
				continue
			}
			return cloneMap(mapValue(block["run"])), block["userInput"]
		}
	}
	return nil, nil
}

func (a *neoActor) rebuildHistoryLocked() {
	toolNames := map[string]string{}
	history := make([]neoHistoryMessage, 0, len(a.messages))
	for _, message := range a.messages {
		switch message.Role {
		case "assistant":
			text, calls := neoAssistantHistoryContent(message.Content)
			for _, call := range calls {
				toolNames[call.ID] = call.Name
			}
			history = append(history, neoHistoryMessage{Role: "assistant", Text: text, ToolCalls: calls, ParentToolUseID: message.ParentToolUseID})
		case "user":
			if message.CompletionStatus == "tool_progress" {
				continue
			}
			if toolResults := neoToolResultHistoryContent(message.Content, toolNames, message.ParentToolUseID); len(toolResults) > 0 {
				history = append(history, toolResults...)
				continue
			}
			history = append(history, neoHistoryMessage{Role: "user", Text: textFromBlocks(message.Content), Content: cloneArray(message.Content), ParentToolUseID: message.ParentToolUseID})
		}
	}
	a.history = history
}

func (a *neoActor) nextSeqLocked() int {
	seq := a.seq
	a.seq++
	return seq
}

func (a *neoActor) rememberReplayEventLocked(payload map[string]any) {
	seq := numberFrom(payload["seq"])
	if seq <= 0 {
		return
	}
	a.replayEvents = append(a.replayEvents, neoReplayEvent{Seq: seq, Payload: cloneNeoProtocolPayload(payload)})
	if overflow := len(a.replayEvents) - neoReplayEventLimit; overflow > 0 {
		a.replayEvents = append([]neoReplayEvent(nil), a.replayEvents[overflow:]...)
	}
}

func cloneNeoProtocolPayload(payload map[string]any) map[string]any {
	out := cloneMap(payload)
	if message, ok := asMap(out["message"]); ok {
		cloned := cloneMap(message)
		if content := arrayValue(cloned["content"]); content != nil {
			cloned["content"] = cloneArray(content)
		}
		out["message"] = cloned
	}
	if errorPayload, ok := asMap(out["error"]); ok {
		out["error"] = cloneMap(errorPayload)
	}
	return out
}

func (a *neoActor) agentModeLocked() string {
	if mode := stringValue(a.settings["agentMode"]); mode != "" {
		return mode
	}
	for _, message := range a.messages {
		if message.Role == "user" && message.AgentMode != "" {
			return message.AgentMode
		}
	}
	if a.currentAgentMode != "" {
		return a.currentAgentMode
	}
	return "smart"
}

func (a *neoActor) reasoningEffortLocked() string {
	return a.reasoningEffortForModeLocked(a.agentModeLocked())
}

func (a *neoActor) reasoningEffortForModeLocked(agentMode string) string {
	if !neoModeSupportsReasoningEffort(agentMode) {
		return ""
	}
	if effort := stringValue(a.settings["reasoning.effort"]); neoReasoningEffortAllowedForMode(agentMode, effort) {
		return effort
	}
	if strings.EqualFold(a.currentAgentMode, agentMode) && neoReasoningEffortAllowedForMode(agentMode, a.currentReasoningEffort) {
		return a.currentReasoningEffort
	}
	for i := len(a.messages) - 1; i >= 0; i-- {
		message := a.messages[i]
		if message.Role != "user" || message.ReasoningEffort == "" {
			continue
		}
		if message.AgentMode != "" && !strings.EqualFold(message.AgentMode, agentMode) {
			continue
		}
		if neoReasoningEffortAllowedForMode(agentMode, message.ReasoningEffort) {
			return message.ReasoningEffort
		}
	}
	return defaultNeoReasoningEffort(agentMode)
}

func defaultNeoReasoningEffort(agentMode string) string {
	switch strings.ToLower(strings.TrimSpace(agentMode)) {
	case "smart":
		return "high"
	case "deep":
		return "xhigh"
	default:
		return ""
	}
}

func normalizeNeoReasoningEffortForMode(agentMode, effort string) string {
	if neoReasoningEffortAllowedForMode(agentMode, effort) {
		return strings.ToLower(strings.TrimSpace(effort))
	}
	return defaultNeoReasoningEffort(agentMode)
}

func neoModeSupportsReasoningEffort(agentMode string) bool {
	switch strings.ToLower(strings.TrimSpace(agentMode)) {
	case "smart", "deep":
		return true
	default:
		return false
	}
}

func neoReasoningEffortAllowedForMode(agentMode, effort string) bool {
	effort = strings.ToLower(strings.TrimSpace(effort))
	if effort == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(agentMode)) {
	case "smart":
		return effort == "high" || effort == "xhigh" || effort == "max"
	case "deep":
		return effort == "low" || effort == "medium" || effort == "xhigh"
	default:
		return false
	}
}

func toolSet(names ...string) map[string]bool {
	out := make(map[string]bool, len(names))
	for _, name := range names {
		out[name] = true
	}
	return out
}

func neoToolAllowedForMode(agentMode, name string) bool {
	agentMode = strings.ToLower(strings.TrimSpace(agentMode))
	if agentMode == "" {
		agentMode = "smart"
	}
	allowlist := neoModeToolAllowlist[agentMode]
	if len(allowlist) == 0 {
		return true
	}
	if !neoKnownModeTools[name] {
		// MCP/plugin/custom tools are not in Amp's built-in mode tables.
		return true
	}
	return allowlist[name]
}

func (a *neoActor) toolNamesLocked(agentMode string) []string {
	names := make([]string, 0, len(a.tools))
	for name := range a.tools {
		if !neoToolAllowedForMode(agentMode, name) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (a *neoActor) pendingToolIDsLocked() []string {
	ids := make([]string, 0, len(a.pendingTools))
	for id := range a.pendingTools {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

type neoSocket struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (s *neoSocket) send(payload any) {
	cleaned := pruneNilJSON(payload)
	data, err := json.Marshal(cleaned)
	if err != nil {
		return
	}
	log.Debugf("amp neo local runtime WS send %s", neoProtocolSummary(cleaned))
	s.sendText(string(data))
}

func (s *neoSocket) sendText(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.conn.WriteMessage(websocket.TextMessage, []byte(text)); err != nil {
		log.Debugf("amp neo local runtime WS send failed: %v", err)
	}
}

func pruneNilJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			if item == nil {
				continue
			}
			out[key] = pruneNilJSON(item)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			if item == nil {
				continue
			}
			out = append(out, pruneNilJSON(item))
		}
		return out
	default:
		return value
	}
}

func neoProtocolSummary(payload any) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return fmt.Sprintf("payload=%T", payload)
	}
	parts := make([]string, 0, 10)
	for _, key := range []string{"type", "state", "role", "messageId", "seq", "agentMode", "reasoningEffort", "queuedMessageId", "toolCallId", "parentToolCallId", "parentToolUseId"} {
		if value, ok := m[key]; ok {
			parts = append(parts, fmt.Sprintf("%s=%v", key, value))
		}
	}
	if blocks, ok := m["blocks"].([]any); ok {
		parts = append(parts, fmt.Sprintf("blocks=%d", len(blocks)))
	}
	if message, ok := m["message"].(map[string]any); ok {
		if role := stringValue(message["role"]); role != "" {
			parts = append(parts, "messageRole="+role)
		}
		if id := stringValue(message["messageId"]); id != "" {
			parts = append(parts, "messageId="+id)
		}
		if state := stringValue(mapValue(message["state"])["type"]); state != "" {
			parts = append(parts, "messageState="+state)
		}
		if content, ok := message["content"].([]any); ok {
			parts = append(parts, fmt.Sprintf("messageBlocks=%d", len(content)))
		}
	}
	if len(parts) == 0 {
		return fmt.Sprintf("payload=%T", payload)
	}
	return strings.Join(parts, " ")
}

type neoToolSpec struct {
	Name        string
	Description string
	InputSchema map[string]any
	Meta        map[string]any
}

type neoToolCall struct {
	ID    string
	Name  string
	Input map[string]any
}

type neoPendingTool struct {
	ID               string
	Name             string
	Input            map[string]any
	AgentMode        string
	ReasoningEffort  string
	MessageID        string
	ParentToolCallID string
}

type neoHistoryMessage struct {
	Role            string
	Text            string
	Content         []any
	ToolCallID      string
	ToolName        string
	ToolCalls       []neoToolCall
	ParentToolUseID string
}

type neoQueuedMessage struct {
	MessageID       string
	Content         []any
	UserState       any
	Meta            map[string]any
	CreatedAt       string
	AgentMode       string
	ReasoningEffort string
	Steer           bool
}

func (m neoQueuedMessage) protocol() map[string]any {
	return map[string]any{
		"role":            "user",
		"messageId":       m.MessageID,
		"content":         m.Content,
		"userState":       m.UserState,
		"meta":            m.Meta,
		"createdAt":       m.CreatedAt,
		"agentMode":       omitEmpty(m.AgentMode),
		"reasoningEffort": omitEmpty(m.ReasoningEffort),
	}
}

type neoMessage struct {
	ThreadID         string
	MessageID        string
	Role             string
	Content          []any
	ParentToolUseID  string
	AgentMode        string
	ReasoningEffort  string
	CreatedAt        string
	ReadAt           string
	Meta             map[string]any
	UserState        any
	State            map[string]any
	Usage            map[string]any
	Seq              int
	CompletionStatus string
}

func (m neoMessage) protocol() map[string]any {
	out := map[string]any{
		"threadId":  m.ThreadID,
		"messageId": m.MessageID,
		"role":      m.Role,
		"content":   m.Content,
	}
	if m.ParentToolUseID != "" {
		out["parentToolUseId"] = m.ParentToolUseID
	}
	if m.AgentMode != "" {
		out["agentMode"] = m.AgentMode
	}
	if m.ReasoningEffort != "" {
		out["reasoningEffort"] = m.ReasoningEffort
	}
	if m.CreatedAt != "" {
		out["createdAt"] = m.CreatedAt
	}
	if m.ReadAt != "" {
		out["readAt"] = m.ReadAt
	}
	if len(m.Meta) > 0 {
		out["meta"] = m.Meta
	}
	if m.UserState != nil {
		out["userState"] = m.UserState
	}
	if len(m.State) > 0 {
		out["state"] = m.State
	}
	if len(m.Usage) > 0 {
		out["usage"] = m.Usage
	}
	if m.CompletionStatus != "" {
		out["completionStatus"] = m.CompletionStatus
	}
	return out
}

func neoMessageAddedPayload(message neoMessage) map[string]any {
	out := map[string]any{"type": "message_added", "message": message.protocol(), "seq": message.Seq}
	if message.ParentToolUseID != "" {
		out["parentToolUseId"] = message.ParentToolUseID
	}
	return out
}

func withNeoParentToolCallID(payload map[string]any, parentToolCallID string) map[string]any {
	if parentToolCallID != "" {
		payload["parentToolCallId"] = parentToolCallID
	}
	return payload
}

type neoInferenceRequest struct {
	ActorID          string
	ThreadID         string
	AgentMode        string
	ReasoningEffort  string
	ParentToolCallID string
	Settings         map[string]any
	History          []neoHistoryMessage
	Tools            []neoToolSpec
	Environment      map[string]any
	Capabilities     map[string]any
	Guidance         map[string]any
}

type neoInferenceResult struct {
	Provider  string
	Model     string
	Text      string
	ToolCalls []neoToolCall
	Usage     map[string]any
}

type neoInferenceDelta struct {
	Text  string
	Usage map[string]any
}

type neoStreamCallback func(neoInferenceDelta)

func normalizeNeoToolCalls(calls []neoToolCall) []neoToolCall {
	normalized := make([]neoToolCall, 0, len(calls))
	for _, call := range calls {
		if call.Name == "" {
			continue
		}
		if call.ID == "" || !strings.HasPrefix(call.ID, "TU-") {
			call.ID = newNeoToolCallID()
		}
		normalized = append(normalized, call)
	}
	return normalized
}

func inferNeoLocal(rt *neoRuntime, request neoInferenceRequest) (neoInferenceResult, error) {
	route := selectNeoModelRoute(request.AgentMode, request.Settings)
	switch route.Provider {
	case "anthropic":
		return inferNeoAnthropic(rt, request, route)
	case "openai":
		return inferNeoOpenAI(rt, request, route)
	case "google":
		return inferNeoGoogle(rt, request, route)
	default:
		return neoInferenceResult{}, fmt.Errorf("unsupported local Neo provider %q", route.Provider)
	}
}

func inferNeoLocalStream(rt *neoRuntime, request neoInferenceRequest, onDelta neoStreamCallback) (neoInferenceResult, error) {
	route := selectNeoModelRoute(request.AgentMode, request.Settings)
	switch route.Provider {
	case "anthropic":
		return inferNeoAnthropicStream(rt, request, route, onDelta)
	case "openai":
		return inferNeoOpenAIStream(rt, request, route, onDelta)
	case "google":
		return inferNeoGoogleStream(rt, request, route, onDelta)
	default:
		return neoInferenceResult{}, fmt.Errorf("unsupported local Neo provider %q", route.Provider)
	}
}

type neoModelRoute struct {
	Provider string
	Model    string
}

func selectNeoModelRoute(agentMode string, settings map[string]any) neoModelRoute {
	if explicit := explicitNeoModel(agentMode, settings); explicit.Model != "" {
		return explicit
	}
	agentMode = strings.ToLower(strings.TrimSpace(agentMode))
	switch agentMode {
	case "deep":
		return neoModelRoute{Provider: "openai", Model: "gpt-5.5"}
	case "rush":
		return neoModelRoute{Provider: "anthropic", Model: "claude-haiku-4-5-20251001"}
	case "large":
		return neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-6"}
	default:
		return neoModelRoute{Provider: "anthropic", Model: "claude-opus-4-7"}
	}
}

func explicitNeoModel(agentMode string, settings map[string]any) neoModelRoute {
	raw := settings["internal.model"]
	if raw == nil {
		raw = settings["amp.internal.model"]
	}
	return explicitNeoModelFromRaw(agentMode, raw)
}

func selectNeoTitleRoute(cfg *config.Config, agentMode string, settings map[string]any) neoModelRoute {
	if cfg != nil {
		if value := strings.TrimSpace(cfg.AmpCode.NeoLocalRuntime.TitleModel); value != "" {
			if route := parseNeoModelRoute(value); route.Model != "" {
				return route
			}
		}
	}
	for _, key := range []string{"internal.titleModel", "amp.internal.titleModel", "title.model"} {
		if route := explicitNeoModelFromRaw(agentMode, settings[key]); route.Model != "" {
			return route
		}
	}
	return neoModelRoute{Provider: "anthropic", Model: defaultNeoTitleModel}
}

func explicitNeoModelFromRaw(agentMode string, raw any) neoModelRoute {
	agentMode = strings.TrimSpace(agentMode)
	value := ""
	if s := stringValue(raw); s != "" {
		value = s
	} else if m, ok := asMap(raw); ok {
		value = stringValue(m[agentMode])
		if value == "" {
			value = stringValue(m[strings.ToLower(agentMode)])
		}
	}
	return parseNeoModelRoute(value)
}

func parseNeoModelRoute(value string) neoModelRoute {
	value = strings.TrimSpace(value)
	if value == "" {
		return neoModelRoute{}
	}
	value = strings.Replace(value, ":", "/", 1)
	parts := strings.SplitN(value, "/", 2)
	if len(parts) == 2 {
		provider := strings.ToLower(strings.TrimSpace(parts[0]))
		model := strings.TrimSpace(parts[1])
		switch provider {
		case "openai", "codex":
			return neoModelRoute{Provider: "openai", Model: model}
		case "google", "vertexai", "gemini":
			return neoModelRoute{Provider: "google", Model: model}
		case "anthropic", "claude":
			return neoModelRoute{Provider: "anthropic", Model: model}
		}
		return neoModelRoute{Provider: providerForNeoModel(model), Model: model}
	}
	return neoModelRoute{Provider: providerForNeoModel(value), Model: value}
}

func providerForNeoModel(model string) string {
	switch {
	case strings.HasPrefix(model, "gpt-") || strings.Contains(model, "codex"):
		return "openai"
	case strings.HasPrefix(model, "gemini-"):
		return "google"
	default:
		return "anthropic"
	}
}

func inferNeoAnthropic(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute) (neoInferenceResult, error) {
	body := map[string]any{
		"model":      route.Model,
		"max_tokens": 8192,
		"stream":     false,
		"system":     []any{map[string]any{"type": "text", "text": neoSystemPrompt(request, route)}},
		"messages":   anthropicNeoMessages(request.History),
	}
	if len(request.Tools) > 0 {
		body["tools"] = anthropicNeoTools(request.Tools)
		body["tool_choice"] = map[string]any{"type": "auto"}
	}

	jsonBody, err := callNeoLocalProvider(rt, "anthropic", "/v1/messages", body, request.ThreadID)
	if err != nil {
		return neoInferenceResult{}, err
	}
	content, _ := jsonBody["content"].([]any)
	var text strings.Builder
	toolCalls := make([]neoToolCall, 0)
	for _, raw := range content {
		item := mapValue(raw)
		switch stringValue(item["type"]) {
		case "text":
			text.WriteString(stringValue(item["text"]))
		case "tool_use":
			name := stringValue(item["name"])
			if name != "" {
				toolCalls = append(toolCalls, neoToolCall{ID: fallbackString(item["id"], newNeoToolCallID()), Name: name, Input: mapValue(item["input"])})
			}
		}
	}
	return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: text.String(), ToolCalls: toolCalls, Usage: mapValue(jsonBody["usage"])}, nil
}

func inferNeoOpenAI(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute) (neoInferenceResult, error) {
	body := map[string]any{
		"model":            route.Model,
		"stream":           false,
		"messages":         openAINeoMessages(request.History, neoSystemPrompt(request, route)),
		"reasoning_effort": openAIReasoningEffort(request.ReasoningEffort),
	}
	if len(request.Tools) > 0 {
		body["tools"] = openAINeoTools(request.Tools)
		body["tool_choice"] = "auto"
	}

	jsonBody, err := callNeoLocalProvider(rt, "openai", "/v1/chat/completions", body, request.ThreadID)
	if err != nil {
		return neoInferenceResult{}, err
	}
	choices, _ := jsonBody["choices"].([]any)
	message := map[string]any{}
	if len(choices) > 0 {
		message = mapValue(mapValue(choices[0])["message"])
	}
	toolCalls := make([]neoToolCall, 0)
	if rawCalls, ok := message["tool_calls"].([]any); ok {
		for i, raw := range rawCalls {
			call := mapValue(raw)
			fn := mapValue(call["function"])
			name := stringValue(fn["name"])
			if name == "" {
				continue
			}
			toolCalls = append(toolCalls, neoToolCall{
				ID:    fallbackString(call["id"], fmt.Sprintf("call-%d", i)),
				Name:  name,
				Input: parseToolArguments(fn["arguments"]),
			})
		}
	}
	return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: stringValue(message["content"]), ToolCalls: toolCalls, Usage: mapValue(jsonBody["usage"])}, nil
}

func inferNeoGoogle(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute) (neoInferenceResult, error) {
	body := map[string]any{
		"contents": googleNeoContents(request.History, neoSystemPrompt(request, route)),
	}
	if len(request.Tools) > 0 {
		body["tools"] = []any{map[string]any{"functionDeclarations": googleNeoTools(request.Tools)}}
	}
	subpath := "/v1beta/models/" + url.PathEscape(route.Model) + ":generateContent"
	jsonBody, err := callNeoLocalProvider(rt, "google", subpath, body, request.ThreadID)
	if err != nil {
		return neoInferenceResult{}, err
	}
	candidates, _ := jsonBody["candidates"].([]any)
	var parts []any
	if len(candidates) > 0 {
		parts, _ = mapValue(mapValue(candidates[0])["content"])["parts"].([]any)
	}
	var text strings.Builder
	toolCalls := make([]neoToolCall, 0)
	for _, raw := range parts {
		part := mapValue(raw)
		if s := stringValue(part["text"]); s != "" {
			text.WriteString(s)
		}
		fc := mapValue(part["functionCall"])
		if name := stringValue(fc["name"]); name != "" {
			toolCalls = append(toolCalls, neoToolCall{ID: newNeoToolCallID(), Name: name, Input: mapValue(fc["args"])})
		}
	}
	return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: text.String(), ToolCalls: toolCalls, Usage: mapValue(jsonBody["usageMetadata"])}, nil
}

func inferNeoAnthropicStream(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute, onDelta neoStreamCallback) (neoInferenceResult, error) {
	body := map[string]any{
		"model":      route.Model,
		"max_tokens": 8192,
		"stream":     true,
		"system":     []any{map[string]any{"type": "text", "text": neoSystemPrompt(request, route)}},
		"messages":   anthropicNeoMessages(request.History),
	}
	if len(request.Tools) > 0 {
		body["tools"] = anthropicNeoTools(request.Tools)
		body["tool_choice"] = map[string]any{"type": "auto"}
	}

	type partialBlock struct {
		blockType string
		id        string
		name      string
		input     map[string]any
		text      strings.Builder
		args      strings.Builder
	}

	blocks := map[int]*partialBlock{}
	order := make([]int, 0)
	var fullText strings.Builder
	var usage map[string]any

	ensureBlock := func(index int) *partialBlock {
		if block, ok := blocks[index]; ok {
			return block
		}
		block := &partialBlock{}
		blocks[index] = block
		order = append(order, index)
		return block
	}

	err := callNeoLocalProviderSSE(rt, "anthropic", "/v1/messages", body, request.ThreadID, func(event, data string) error {
		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			return err
		}
		if errorBody := mapValue(payload["error"]); len(errorBody) > 0 {
			return fmt.Errorf("local provider stream error: %s", fallbackString(errorBody["message"], data))
		}
		switch stringValue(payload["type"]) {
		case "message_start":
			usage = mergeNeoUsage(usage, mapValue(mapValue(payload["message"])["usage"]))
		case "message_delta":
			usage = mergeNeoUsage(usage, mapValue(payload["usage"]))
			usage = mergeNeoUsage(usage, mapValue(mapValue(payload["delta"])["usage"]))
		case "content_block_start":
			index := numberFrom(payload["index"])
			contentBlock := mapValue(payload["content_block"])
			block := ensureBlock(index)
			block.blockType = stringValue(contentBlock["type"])
			switch block.blockType {
			case "text":
				if text := stringValue(contentBlock["text"]); text != "" {
					block.text.WriteString(text)
					fullText.WriteString(text)
					if onDelta != nil {
						onDelta(neoInferenceDelta{Text: text, Usage: usage})
					}
				}
			case "tool_use":
				block.id = stringValue(contentBlock["id"])
				block.name = stringValue(contentBlock["name"])
				block.input = mapValue(contentBlock["input"])
			}
		case "content_block_delta":
			index := numberFrom(payload["index"])
			delta := mapValue(payload["delta"])
			block := ensureBlock(index)
			switch stringValue(delta["type"]) {
			case "text_delta":
				block.blockType = "text"
				if text := stringValue(delta["text"]); text != "" {
					block.text.WriteString(text)
					fullText.WriteString(text)
					if onDelta != nil {
						onDelta(neoInferenceDelta{Text: text, Usage: usage})
					}
				}
			case "input_json_delta":
				block.blockType = "tool_use"
				block.args.WriteString(stringValue(delta["partial_json"]))
			}
		case "error":
			return fmt.Errorf("local provider stream error: %s", data)
		}
		return nil
	})
	if err != nil {
		return neoInferenceResult{}, err
	}

	sort.Ints(order)
	toolCalls := make([]neoToolCall, 0)
	for _, index := range order {
		block := blocks[index]
		if block == nil || block.blockType != "tool_use" || block.name == "" {
			continue
		}
		input := block.input
		if block.args.Len() > 0 {
			input = parseToolArguments(block.args.String())
		}
		toolCalls = append(toolCalls, neoToolCall{ID: fallbackString(block.id, newNeoToolCallID()), Name: block.name, Input: input})
	}
	return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: fullText.String(), ToolCalls: toolCalls, Usage: usage}, nil
}

func inferNeoOpenAIStream(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute, onDelta neoStreamCallback) (neoInferenceResult, error) {
	body := map[string]any{
		"model":            route.Model,
		"stream":           true,
		"messages":         openAINeoMessages(request.History, neoSystemPrompt(request, route)),
		"reasoning_effort": openAIReasoningEffort(request.ReasoningEffort),
	}
	if len(request.Tools) > 0 {
		body["tools"] = openAINeoTools(request.Tools)
		body["tool_choice"] = "auto"
	}

	type partialToolCall struct {
		id   string
		name string
		args strings.Builder
	}

	var fullText strings.Builder
	var usage map[string]any
	toolCallsByIndex := map[int]*partialToolCall{}
	toolIndexes := make([]int, 0)

	ensureToolCall := func(index int) *partialToolCall {
		if call, ok := toolCallsByIndex[index]; ok {
			return call
		}
		call := &partialToolCall{}
		toolCallsByIndex[index] = call
		toolIndexes = append(toolIndexes, index)
		return call
	}

	err := callNeoLocalProviderSSE(rt, "openai", "/v1/chat/completions", body, request.ThreadID, func(event, data string) error {
		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			return err
		}
		if errorBody := mapValue(payload["error"]); len(errorBody) > 0 {
			return fmt.Errorf("local provider stream error: %s", fallbackString(errorBody["message"], data))
		}
		usage = mergeNeoUsage(usage, mapValue(payload["usage"]))
		for _, rawChoice := range arrayValue(payload["choices"]) {
			choice := mapValue(rawChoice)
			delta := mapValue(choice["delta"])
			if text := stringValue(delta["content"]); text != "" {
				fullText.WriteString(text)
				if onDelta != nil {
					onDelta(neoInferenceDelta{Text: text, Usage: usage})
				}
			}
			for _, rawCall := range arrayValue(delta["tool_calls"]) {
				callDelta := mapValue(rawCall)
				index := numberFrom(callDelta["index"])
				call := ensureToolCall(index)
				if id := stringValue(callDelta["id"]); id != "" {
					call.id = id
				}
				function := mapValue(callDelta["function"])
				if name := stringValue(function["name"]); name != "" {
					call.name = name
				}
				call.args.WriteString(stringValue(function["arguments"]))
			}
		}
		return nil
	})
	if err != nil {
		return neoInferenceResult{}, err
	}

	sort.Ints(toolIndexes)
	toolCalls := make([]neoToolCall, 0, len(toolIndexes))
	for _, index := range toolIndexes {
		call := toolCallsByIndex[index]
		if call == nil || call.name == "" {
			continue
		}
		toolCalls = append(toolCalls, neoToolCall{
			ID:    fallbackString(call.id, fmt.Sprintf("call-%d", index)),
			Name:  call.name,
			Input: parseToolArguments(call.args.String()),
		})
	}
	return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: fullText.String(), ToolCalls: toolCalls, Usage: usage}, nil
}

func inferNeoGoogleStream(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute, onDelta neoStreamCallback) (neoInferenceResult, error) {
	body := map[string]any{
		"contents": googleNeoContents(request.History, neoSystemPrompt(request, route)),
	}
	if len(request.Tools) > 0 {
		body["tools"] = []any{map[string]any{"functionDeclarations": googleNeoTools(request.Tools)}}
	}

	subpath := "/v1beta/models/" + url.PathEscape(route.Model) + ":streamGenerateContent?alt=sse"
	var fullText strings.Builder
	toolCalls := make([]neoToolCall, 0)
	var usage map[string]any
	received := false

	err := callNeoLocalProviderSSE(rt, "google", subpath, body, request.ThreadID, func(event, data string) error {
		received = true
		var payload map[string]any
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			return err
		}
		if errorBody := mapValue(payload["error"]); len(errorBody) > 0 {
			return fmt.Errorf("local provider stream error: %s", fallbackString(errorBody["message"], data))
		}
		usage = mergeNeoUsage(usage, mapValue(payload["usageMetadata"]))
		for _, rawCandidate := range arrayValue(payload["candidates"]) {
			parts := arrayValue(mapValue(mapValue(rawCandidate)["content"])["parts"])
			for _, rawPart := range parts {
				part := mapValue(rawPart)
				if text := stringValue(part["text"]); text != "" {
					fullText.WriteString(text)
					if onDelta != nil {
						onDelta(neoInferenceDelta{Text: text, Usage: usage})
					}
				}
				fc := mapValue(part["functionCall"])
				if name := stringValue(fc["name"]); name != "" {
					toolCalls = append(toolCalls, neoToolCall{ID: newNeoToolCallID(), Name: name, Input: mapValue(fc["args"])})
				}
			}
		}
		return nil
	})
	if err != nil {
		return neoInferenceResult{}, err
	}
	if !received {
		return inferNeoGoogle(rt, request, route)
	}
	return neoInferenceResult{Provider: route.Provider, Model: route.Model, Text: fullText.String(), ToolCalls: toolCalls, Usage: usage}, nil
}

func inferNeoTitleLocal(rt *neoRuntime, request neoInferenceRequest, route neoModelRoute) (string, error) {
	if len(request.History) == 0 || strings.TrimSpace(request.History[0].Text) == "" {
		return "", errors.New("empty title generation prompt")
	}
	message := request.History[0].Text
	system := `You are an assistant that generates short, descriptive titles (maximum 5 words, "Sentence case" with the first word capitalized not "Title Case") based on user's message to an agentic coding tool. Your titles should be concise (max 5 words) and capture the essence of the query or topic. DO NOT ASSUME OR GUESS the user's intent beyond what is in their message. Omit generic words like "question", "request", etc. Be professional and precise. Use common software engineering terms and acronyms if they are helpful. Use the set_title tool to provide your answer.`

	switch route.Provider {
	case "anthropic":
		body := map[string]any{
			"model":       route.Model,
			"max_tokens":  60,
			"temperature": 0.7,
			"stream":      false,
			"system":      []any{map[string]any{"type": "text", "text": system}},
			"messages":    []any{map[string]any{"role": "user", "content": "<message>" + message + "</message>"}},
			"tools": []any{map[string]any{
				"name": "set_title",
				"input_schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"title": map[string]any{
							"type":        "string",
							"description": `The short thread title (maximum 5 words, "Sentence case" with the first word capitalized not "Title Case") that you generated for the message`,
						},
					},
					"required": []any{"title"},
				},
			}},
			"tool_choice": map[string]any{"type": "tool", "name": "set_title", "disable_parallel_tool_use": true},
		}
		jsonBody, err := callNeoLocalProvider(rt, "anthropic", "/v1/messages", body, request.ThreadID)
		if err != nil {
			return "", err
		}
		for _, raw := range arrayValue(jsonBody["content"]) {
			item := mapValue(raw)
			if stringValue(item["type"]) == "tool_use" && stringValue(item["name"]) == "set_title" {
				return stringValue(mapValue(item["input"])["title"]), nil
			}
		}
		return "", errors.New("missing set_title tool_use in title response")
	case "openai":
		body := map[string]any{
			"model":                 route.Model,
			"stream":                false,
			"messages":              openAINeoMessages([]neoHistoryMessage{{Role: "user", Text: "<message>" + message + "</message>"}}, system),
			"reasoning_effort":      openAIReasoningEffort(request.ReasoningEffort),
			"max_completion_tokens": 64,
		}
		jsonBody, err := callNeoLocalProvider(rt, "openai", "/v1/chat/completions", body, request.ThreadID)
		if err != nil {
			return "", err
		}
		choices := arrayValue(jsonBody["choices"])
		if len(choices) == 0 {
			return "", nil
		}
		return stringValue(mapValue(mapValue(choices[0])["message"])["content"]), nil
	case "google":
		body := map[string]any{
			"contents":         googleNeoContents([]neoHistoryMessage{{Role: "user", Text: "<message>" + message + "</message>"}}, system),
			"generationConfig": map[string]any{"maxOutputTokens": 64},
		}
		subpath := "/v1beta/models/" + url.PathEscape(route.Model) + ":generateContent"
		jsonBody, err := callNeoLocalProvider(rt, "google", subpath, body, request.ThreadID)
		if err != nil {
			return "", err
		}
		candidates := arrayValue(jsonBody["candidates"])
		if len(candidates) == 0 {
			return "", nil
		}
		parts := arrayValue(mapValue(mapValue(candidates[0])["content"])["parts"])
		var out strings.Builder
		for _, raw := range parts {
			out.WriteString(stringValue(mapValue(raw)["text"]))
		}
		return out.String(), nil
	default:
		return "", fmt.Errorf("unsupported local Neo title provider %q", route.Provider)
	}
}

func neoTitleHistory(history []neoHistoryMessage) []neoHistoryMessage {
	for _, message := range history {
		if message.Role != "user" {
			continue
		}
		text := strings.Join(strings.Fields(message.Text), " ")
		if text == "" {
			continue
		}
		return []neoHistoryMessage{{Role: "user", Text: text}}
	}
	return nil
}

func sanitizeNeoGeneratedTitle(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}
	if idx := strings.IndexAny(title, "\r\n"); idx >= 0 {
		title = title[:idx]
	}
	title = strings.TrimSpace(title)
	title = strings.TrimPrefix(title, "Title:")
	title = strings.TrimPrefix(title, "title:")
	title = strings.TrimSpace(title)
	title = strings.Trim(title, "`\"'")
	title = strings.TrimSpace(title)
	for _, prefix := range []string{"- ", "* "} {
		title = strings.TrimPrefix(title, prefix)
	}
	if len(title) >= 3 && title[0] >= '0' && title[0] <= '9' && (title[1] == '.' || title[1] == ')') && title[2] == ' ' {
		title = title[3:]
	}
	title = strings.Join(strings.Fields(title), " ")
	title = strings.TrimRight(title, ".:;")
	title = strings.Trim(title, "`\"'")
	if len(title) > 80 {
		title = strings.TrimSpace(title[:77]) + "..."
	}
	return title
}

func neoTitleGenerationEnabled(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	if cfg.AmpCode.NeoLocalRuntime.TitleGeneration != nil {
		return *cfg.AmpCode.NeoLocalRuntime.TitleGeneration
	}
	return neoRuntimeEnabled(cfg)
}

func callNeoLocalProvider(rt *neoRuntime, provider, subpath string, body map[string]any, threadID string) (map[string]any, error) {
	cfg := rt.configSnapshot()
	if cfg == nil {
		return nil, fmt.Errorf("missing CLIProxyAPI config for local Neo inference")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	endpoint := fmt.Sprintf("%s/api/provider/%s%s", strings.TrimRight(neoProxyBaseURL(cfg), "/"), provider, subpath)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set(localNeoInferenceHeader, "1")
	req.Header.Set("X-Session-ID", threadID)
	req.Header.Set("X-Amp-Thread-ID", threadID)
	if provider == "anthropic" {
		req.Header.Set("Anthropic-Version", "2023-06-01")
	}
	if key := firstConfiguredAPIKey(cfg); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("local provider returned %d: %s", resp.StatusCode, string(respBody))
	}
	var decoded map[string]any
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func callNeoLocalProviderSSE(rt *neoRuntime, provider, subpath string, body map[string]any, threadID string, handle func(event, data string) error) error {
	cfg := rt.configSnapshot()
	if cfg == nil {
		return fmt.Errorf("missing CLIProxyAPI config for local Neo inference")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/api/provider/%s%s", strings.TrimRight(neoProxyBaseURL(cfg), "/"), provider, subpath)
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(localNeoInferenceHeader, "1")
	req.Header.Set("X-Session-ID", threadID)
	req.Header.Set("X-Amp-Thread-ID", threadID)
	if provider == "anthropic" {
		req.Header.Set("Anthropic-Version", "2023-06-01")
	}
	if key := firstConfiguredAPIKey(cfg); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("local provider returned %d: %s", resp.StatusCode, string(respBody))
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	event := ""
	dataLines := make([]string, 0, 4)
	dispatch := func() error {
		if len(dataLines) == 0 {
			event = ""
			return nil
		}
		data := strings.Join(dataLines, "\n")
		eventName := event
		event = ""
		dataLines = dataLines[:0]
		if strings.TrimSpace(data) == "[DONE]" {
			return nil
		}
		if handle == nil {
			return nil
		}
		return handle(eventName, data)
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		if value, ok := strings.CutPrefix(line, "event:"); ok {
			event = strings.TrimSpace(value)
			continue
		}
		if value, ok := strings.CutPrefix(line, "data:"); ok {
			dataLines = append(dataLines, strings.TrimPrefix(value, " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return dispatch()
}

func firstConfiguredAPIKey(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	for _, key := range cfg.APIKeys {
		if strings.TrimSpace(key) != "" {
			return strings.TrimSpace(key)
		}
	}
	return ""
}

func neoProxyBaseURL(cfg *config.Config) string {
	if cfg == nil {
		return fmt.Sprintf("http://127.0.0.1:%d", defaultAmpProxyPort)
	}
	host := strings.TrimSpace(cfg.Host)
	if host == "" || host == "::" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	port := cfg.Port
	if port <= 0 {
		port = defaultAmpProxyPort
	}
	scheme := "http"
	if cfg.TLS.Enable {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, fmt.Sprintf("%d", port)))
}

func neoAmpExecutorCommand(cfg *config.Config) (string, error) {
	if cfg != nil {
		if command := strings.TrimSpace(cfg.AmpCode.NeoLocalRuntime.ExecutorCommand); command != "" {
			return command, nil
		}
	}
	for _, name := range []string{"AMP_EXECUTOR_COMMAND", "AMP_BINARY"} {
		if command := strings.TrimSpace(os.Getenv(name)); command != "" {
			return command, nil
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		command := filepath.Join(home, ".amp", "bin", "amp")
		if info, err := os.Stat(command); err == nil && !info.IsDir() {
			return command, nil
		}
	}
	if command, err := exec.LookPath("amp"); err == nil {
		return command, nil
	}
	return "", fmt.Errorf("amp binary not found; set ampcode.neo-local-runtime.executor-command")
}

func neoHeadlessExecutorEnv(base []string, cfg *config.Config, threadID, workDir, logPath string) []string {
	updates := map[string]string{
		"AMP_EXECUTOR":          "1",
		"AMP_URL":               neoProxyBaseURL(cfg),
		"AMP_THREAD_ID":         threadID,
		"AMP_CURRENT_THREAD_ID": threadID,
		"AMP_SKIP_UPDATE_CHECK": "1",
		"AMP_HEADLESS_OAUTH":    "1",
	}
	if key := firstConfiguredAPIKey(cfg); key != "" {
		updates["AMP_API_KEY"] = key
	}
	if workDir != "" {
		updates["AMP_PWD"] = workDir
	}
	if logPath != "" {
		updates["AMP_LOG_FILE"] = logPath
	}
	return appendEnvOverrides(base, updates)
}

func appendEnvOverrides(base []string, updates map[string]string) []string {
	out := make([]string, 0, len(base)+len(updates))
	for _, item := range base {
		key, _, ok := strings.Cut(item, "=")
		if !ok {
			out = append(out, item)
			continue
		}
		if _, replace := updates[key]; replace {
			continue
		}
		out = append(out, item)
	}
	keys := make([]string, 0, len(updates))
	for key, value := range updates {
		if key != "" && value != "" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		out = append(out, key+"="+updates[key])
	}
	return out
}

func neoHeadlessWorkingDirectory(msg map[string]any, environment map[string]any) string {
	candidates := []any{
		msg["workingDirectory"],
		msg["working_directory"],
		msg["cwd"],
		msg["workspaceRoot"],
		environment["workingDirectory"],
		environment["working_directory"],
		environment["cwd"],
		environment["workspaceRoot"],
	}
	if initial := mapValue(environment["initial"]); len(initial) > 0 {
		candidates = append(candidates, initial["workingDirectory"], initial["cwd"], initial["workspaceRoot"])
		if trees := arrayValue(initial["trees"]); len(trees) > 0 {
			candidates = append(candidates, trees...)
		}
	}
	if trees := arrayValue(environment["trees"]); len(trees) > 0 {
		candidates = append(candidates, trees...)
	}
	for _, candidate := range candidates {
		if dir := neoExistingDirectory(candidate); dir != "" {
			return dir
		}
	}
	if cwd, err := os.Getwd(); err == nil {
		if dir := neoExistingDirectory(cwd); dir != "" {
			return dir
		}
	}
	if home, err := os.UserHomeDir(); err == nil {
		return home
	}
	return ""
}

func neoExistingDirectory(value any) string {
	path := strings.TrimSpace(firstNonEmptyString(value, nestedString(value, "uri"), nestedString(value, "path"), nestedString(value, "fsPath")))
	if path == "" {
		return ""
	}
	if strings.HasPrefix(path, "file:") {
		path = neoGuidancePath(path)
	}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return ""
	}
	if realPath, err := filepath.EvalSymlinks(path); err == nil {
		return realPath
	}
	return path
}

func neoHeadlessExecutorLogPath(threadID, spawnID string) string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".cli-proxy-api", "logs", "amp-neo-headless-"+neoSafeLogPart(threadID)+"-"+neoSafeLogPart(spawnID)+".log")
}

func neoSafeLogPart(value string) string {
	if value == "" {
		return "unknown"
	}
	var out strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
	}
	if out.Len() == 0 {
		return "unknown"
	}
	return out.String()
}

const neoSkillToolName = "skill"

func neoSystemPrompt(request neoInferenceRequest, route neoModelRoute) string {
	deep := strings.EqualFold(request.AgentMode, "deep")
	blocks := []string{neoBasePrompt(request, route)}
	blocks = append(blocks, neoGuidanceBlocks(request, deep)...)
	if environment := neoEnvironmentBlock(request, deep); environment != "" {
		blocks = append(blocks, environment)
	}
	if skills := neoSkillsPrompt(request, deep); skills != "" {
		blocks = append(blocks, skills)
	}
	return strings.Join(compactStrings(blocks), "\n\n")
}

func neoBasePrompt(request neoInferenceRequest, route neoModelRoute) string {
	if strings.EqualFold(request.AgentMode, "rush") {
		return neoRushPrompt(len(request.Tools) == 0 || neoInferenceRequestHasTool(request, "get_diagnostics"))
	}
	if strings.EqualFold(request.AgentMode, "deep") {
		if strings.Contains(strings.ToLower(route.Model), "gpt-5.4") {
			return neoDeepGPT54Prompt()
		}
		return neoDeepPrompt()
	}
	if strings.EqualFold(route.Provider, "openai") {
		return neoGenericOpenAIPrompt()
	}
	return neoDefaultPrompt()
}

func neoInferenceRequestHasTool(request neoInferenceRequest, name string) bool {
	for _, tool := range request.Tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}

func neoRushPrompt(enableDiagnostics bool) string {
	verify := "build/test/lint commands via Bash"
	if enableDiagnostics {
		verify = "get_diagnostics or " + verify
	}
	return strings.Join([]string{
		"You are Amp (Rush Mode), optimized for speed and efficiency.",
		"# Core Rules",
		"**SPEED FIRST**: Minimize thinking time, minimize tokens, maximize action. You are here to execute, so: execute.",
		"# Execution",
		"Do the task with minimal explanation:\n- Use finder and Grep extensively in parallel to understand code\n- Make edits with edit_file or create_file\n- After changes, MUST verify with " + verify + "\n- NEVER make changes without then verifying they work",
		"# Communication Style",
		"**ULTRA CONCISE**. Answer in 1-3 words when possible. One line maximum for simple questions.\n\n<example>\n<user>what's the time complexity?</user>\n<response>O(n)</response>\n</example>\n\n<example>\n<user>how do I run tests?</user>\n<response>`pnpm test`</response>\n</example>\n\nFor code tasks: do the work, minimal or no explanation. Let the code speak.\n\nFor questions: answer directly, no preamble or summary.",
		"# Tool Usage",
		"When invoking Read, ALWAYS use absolute paths.\n\nRead complete files, not line ranges. Do NOT invoke Read on the same file twice.\n\nRun independent read-only tools (Grep, finder, Read, glob) in parallel.\n\nDo NOT run multiple edits to the same file in parallel.",
		"# AGENTS.md",
		"If an AGENTS.md is provided, treat it as ground truth for commands and structure.",
		"# File Links",
		"Link files as: [display text](file:///absolute/path#L10-L20)\n\nAlways link when mentioning files.",
		neoDiagramInstructions("#"),
		"# Final Note",
		"Speed is the priority. Skip explanations unless asked. Keep responses under 2 lines except when doing actual work.",
	}, "\n\n")
}

func neoDeepPrompt() string {
	return strings.Join([]string{
		"You are Amp, an autonomous coding agent. You and the user share one workspace, and your job is to deliver the outcome they're after. You bring a senior engineer's judgment: you read the codebase before you change it, you prefer the smallest correct change, and you carry the work through implementation and verification rather than stopping at a proposal. When the user redirects you, adapt immediately and keep moving toward the result.",
		"## Autonomy And Persistence\n\nFor each task, keep the user's desired outcome in focus and choose the smallest useful definition of done. Let that guide how much context to gather, how much code to change, and which verification to run.\n\nUnless the user is asking a question, brainstorming, or explicitly requesting a plan, assume they want you to solve the problem with code and tools rather than describing a proposed solution. If you hit blockers, try to resolve them yourself.\n\nPrefer making progress over stopping for clarification when the request is already clear enough to attempt. Use context and reasonable assumptions to move forward. Ask for clarification only when the missing information would materially change the answer or create meaningful risk, and keep any question narrow.\n\nIf you notice unexpected changes in the worktree or staging area that you did not make, continue with your task. NEVER revert, undo, or modify changes you did not make unless the user explicitly asks you to. There can be multiple agents or the user working in the same codebase concurrently.\n\nIf you notice a clear misconception or nearby high-impact bug while doing the requested work, mention it briefly. Do not broaden the task unless it blocks the requested outcome or the user asks.\n\nIf an approach fails, diagnose why before switching tactics - read the error, check your assumptions, try a focused fix. Don't retry the identical action blindly, but don't abandon a viable approach after a single failure either.",
		"## Pragmatism And Scope\n\n- The best change is often the smallest correct change. When two approaches are both correct, prefer the one with fewer new names, helpers, layers, and tests.\n- You prefer the repo's existing patterns, frameworks, and local helper APIs over inventing a new style of abstraction.\n- Avoid over-engineering: don't add unrelated cleanup, hypothetical configurability, defensive handling for impossible internal states, or one-use abstractions.\n- NEVER create files unless they are absolutely necessary for achieving your goal. Prefer editing an existing file to creating a new one.\n- If you create any temporary files, scripts, or helper files for iteration, clean them up by removing them at the end of the task.",
		"## Discovery Discipline\n\nRead enough code to avoid guessing, then stop. Senior judgment means knowing when the ownership path is clear, not making the whole subsystem familiar.\n\nUse each read or search to answer a specific uncertainty: where the change belongs, what contract it must preserve, what local pattern to follow, or how to verify it. Once those are clear, move to the edit or the answer.\n\nBefore adding a local wrapper, adapter, one-off helper, or additional type, check whether it can be avoided. If the existing helper is not shared with consumers that need different behavior, change the source of truth directly instead of layering a one-off override. Add new names only when they remove real complexity, are reused, or match an established local pattern.",
		"## Engineering judgment\n\nWhen the user leaves implementation details open, choose conservatively and in sympathy with the codebase already in front of you. Prefer existing patterns, local helpers, narrow module boundaries, and tests that match the risk. Add abstractions only when they remove real complexity, reduce meaningful duplication, or clearly match an established local pattern.",
		"## Verification\n\nVerification should scale with risk and blast radius: a typo fix needs none, a localized change needs a targeted check, and shared/cross-module changes need broader coverage. Before running verification, choose the narrowest check that would change your confidence. Report outcomes honestly. Don't claim tests pass when they don't, don't suppress failing checks to manufacture a green result, and don't hard-code values or add special cases just to satisfy a test.",
		"## Tool Use\n\nParallelize independent reads and searches when they are already needed, especially with commands such as `cat`, `rg`, `sed`, `ls`, `nl`, and `wc`. Use parallelism to reduce latency, not to widen exploration.\n\nWhen searching for text or files, prefer using `rg` or `rg --files` respectively because `rg` is much faster than alternatives like `grep`. If `rg` is not found, use alternatives.\n\nUse finder for complex, multi-step codebase discovery: behavior-level questions, flows spanning multiple modules, or correlating related patterns. For direct symbol, path, or exact-string lookups, use `rg` first. Use librarian when you need understanding outside the local workspace.",
		neoDiagramInstructions("##"),
		"## Working with the user\n\nUse concise intermediary updates when you make an important discovery or decide on an implementation detail. When complete, respond with a concise report covering what was done and any key findings. When referencing code, use fluent Markdown links of the form `[display text](file:///absolute/path#L10-L20)`. Never paste a raw `file://` URL as visible text.\n\nNew user messages during a turn refine the work; the newest message wins on conflict. A status request means: give the update, then keep working. Before finalizing after an interrupt or context compaction, verify your answer addresses the newest request, not an older one still in flight.",
	}, "\n\n")
}

func neoDeepGPT54Prompt() string {
	return strings.Join([]string{
		"You are Amp. You and the user share the same workspace and collaborate to achieve the user's goals.",
		"You are a pragmatic, effective software engineer. You take engineering quality seriously. You build context by examining the codebase first without making assumptions or jumping to conclusions. You think through the nuances of the code you encounter, and embody the mentality of a skilled senior software engineer.",
		"- When searching for text or files, prefer using `rg` or `rg --files` respectively because `rg` is much faster than alternatives like `grep`.\n- Parallelize tool calls whenever possible - especially file reads and searches.\n- Use finder for complex, multi-step codebase discovery. For direct symbol, path, or exact-string lookups, use `rg` first.\n- Use librarian when you need understanding outside the local workspace.",
		"## Pragmatism and Scope\n\n- The best change is often the smallest correct change.\n- Keep obvious single-use logic inline. Do not extract a helper unless it is reused, hides meaningful complexity, or names a real domain concept.\n- Avoid over-engineering. Only make changes that are directly requested or clearly necessary.\n- NEVER create files unless they are absolutely necessary for achieving your goal. Prefer editing an existing file to creating a new one.",
		"## Working Method\n\nRead enough code to avoid guessing, then stop. Make the smallest correct change, keep unrelated edits out of scope, and verify with the narrowest useful check. If a command fails, read the failure and diagnose before switching tactics.",
		"## Final Responses\n\nFor small tasks, prefer 1-2 short paragraphs plus an optional short verification line. When referencing code, use fluent Markdown links like `[display text](file:///absolute/path#L10-L20)`. If you could not verify, say so.",
	}, "\n\n")
}

func neoGenericOpenAIPrompt() string {
	return strings.Join([]string{
		"You are Amp, a powerful AI coding agent. You help the user with software engineering tasks. Use the instructions below and the tools available to you to help the user.",
		"# Role & Agency\n\n- Do the task end to end. Don't hand back half-baked work. FULLY resolve the user's request and objective. Keep working through the problem until you reach a complete solution - don't stop at partial answers or \"here's how you could do it\" responses.\n- Balance initiative with restraint: if the user asks for a plan, give a plan; don't edit files.\n- Do not add explanations unless asked. After edits, stop.",
		"# Guardrails\n\n- **Simple-first**: prefer the smallest, local fix over a cross-file architecture change.\n- **Reuse-first**: search for existing patterns; mirror naming, error handling, I/O, typing, tests.\n- **No surprise edits**: if changes affect more than 3 files or multiple subsystems, show a short plan first.\n- **No new deps** without explicit user approval.",
		"# Fast Context Understanding\n\nGoal: Get enough context fast. Parallelize discovery and stop as soon as you can act. Deduplicate paths and cache; don't repeat queries. Trace only symbols you'll modify or whose contracts you rely on; avoid transitive expansion unless necessary.",
		"# Parallel Execution Policy\n\nDefault to parallel for independent reads, searches, diagnostics, subagents, and disjoint writes. Serialize plan-to-code dependencies and any edits touching the same files or shared state.",
		"# Editing Discipline\n\nPrefer targeted edits. Do not reformat unrelated code. Do not rewrite working code for style. Do not invent wrappers or abstractions unless they remove real complexity or match a clear local pattern.",
		"# Verification\n\nRun the narrowest useful test, typecheck, lint, build, or diagnostic command after edits. Do not claim success unless verification passed. If verification is impossible, state why.",
		"# Final Status Spec\n\n2-10 lines. Lead with what changed and why. Link files with `file://` + line(s). Include verification results. Offer the next action when natural.",
		"# Strict Concision\n\nBe concise. Respond in the fewest words that fully update the user on what you have done or are doing. Never pad with meta commentary.",
	}, "\n\n")
}

func neoDefaultPrompt() string {
	return strings.Join([]string{
		"You are pair programming with a user to solve their coding task. Treat every user message, including interruptions, corrections, and short replies, as an addition to the original specification that refines your direction. When the user redirects you, adapt immediately without defensiveness. Your main goal is to follow the user's instructions and verify that the result works.",
		"<autonomy_and_persistence>",
		"Unless the user explicitly asks for a plan, asks a question about the code, is brainstorming potential solutions, or some other intent that makes it clear that code should not be written, assume the user wants you to make code changes or run tools to solve the user's problem. Do not output your proposed solution in a message -- implement the change. If you encounter challenges or blockers, attempt to resolve them yourself.",
		"Persist until the task is fully handled end-to-end: carry changes through implementation, verification, and a clear explanation of outcomes. Do not stop at analysis or partial fixes unless the user explicitly pauses or redirects you. Continue completing the user's ongoing requests unless they ask you to stop -- especially when they tell you to continue or go on, treat that as a directive to keep working on the current task until it is fully done.",
		"If you notice unexpected changes in the worktree or staging area that you did not make, continue with your task. NEVER revert, undo, or modify changes you did not make unless the user explicitly asks you to.",
		"If you notice the user's request is based on a misconception, or spot a bug adjacent to what they asked about, say so. You're a collaborator, not just an executor -- users benefit from your judgment, not just your compliance.",
		"</autonomy_and_persistence>",
		"<investigate_before_acting>",
		"Never speculate about code you have not read. If the user references a file, symbol, error, or behavior, inspect the relevant code or logs before making changes.",
		"</investigate_before_acting>",
		"<pragmatism_and_scope>\n- Prefer the smallest correct change.\n- Search for existing patterns before inventing new ones.\n- Avoid unrelated cleanup, broad refactors, and new dependencies unless explicitly requested.\n- NEVER create files unless they are absolutely necessary; prefer editing existing files.\n</pragmatism_and_scope>",
		"<tool_use>\nWhen searching for text or files, prefer `rg` or `rg --files`. Parallelize independent reads and searches when useful. Use finder for complex behavior-level discovery; use librarian for external repository understanding.\n</tool_use>",
		"<verification>\nAfter edits, run the narrowest useful verification. Report failures honestly. Do not claim tests pass unless they do.\n</verification>",
		neoDiagramInstructions(""),
		"<file_links>\nWhen referencing files in your response, prefer fluent Markdown links of the form `[display text](file:///absolute/path#L10-L20)`. Do not paste raw file URLs as visible text.\n</file_links>",
	}, "\n")
}

func neoDiagramInstructions(heading string) string {
	prefix := ""
	if heading != "" {
		prefix = heading + " Diagrams\n\n"
	}
	return prefix + "When a diagram would explain architecture, workflows, data flow, state transitions, or relationships better than prose alone, create it with a `diagram` code block in your response. Use plain text or box-drawing characters, preferably rounded-corner boxes (`╭`, `╮`, `╰`, `╯`), inside `diagram` blocks. There is no Mermaid tool or renderer: do not write Mermaid syntax such as `graph TD` or `sequenceDiagram`, and do not use `mermaid` code fences. Keep diagrams readable in monospaced text."
}

func neoGuidanceBlocks(request neoInferenceRequest, deep bool) []string {
	blocks := []string{neoGuidanceOverview(deep)}
	files := neoGuidanceFiles(request.Guidance)
	if len(files) > 0 {
		for _, file := range files {
			if deep {
				blocks = append(blocks, "# AGENTS.md instructions for "+neoGuidanceScope(file.URI)+"\n<INSTRUCTIONS>\n"+file.Content+"\n</INSTRUCTIONS>")
				continue
			}
			name := neoGuidanceName(file.URI)
			scope := neoGuidanceScope(file.URI)
			blocks = append(blocks, "Contents of "+name+" (directory-specific instructions for "+scope+"):\n<instructions>\n"+file.Content+"\n</instructions>")
		}
		return blocks
	}
	if guidance := neoGuidanceText(request.Guidance); guidance != "" {
		if deep {
			blocks = append(blocks, "# AGENTS.md instructions for /\n<INSTRUCTIONS>\n"+guidance+"\n</INSTRUCTIONS>")
		} else {
			blocks = append(blocks, "Contents of AGENTS.md (executor guidance):\n<instructions>\n"+guidance+"\n</instructions>")
		}
	}
	return blocks
}

func neoGuidanceOverview(deep bool) string {
	if deep {
		return "Files called AGENTS.md pass along human guidance to you, the agent. Such guidance can include coding standards, explanations of the project layout, steps for building or testing, and other instructions to be followed.\nEach AGENTS.md governs the entire directory that contains it and every child directory beneath it. Whenever you change a file, you must comply with every AGENTS.md whose scope covers that file. Apply only the parts of these guidance files that are relevant to the current files and task; they define constraints, not extra work to perform by default.\nAGENTS.md instructions are delivered dynamically in the conversation context. They appear with a header \"# AGENTS.md instructions for [path]\" followed by <INSTRUCTIONS> tags."
	}
	return "AGENTS.md guidance files are delivered dynamically in the conversation context after file operations and user file mentions. They appear with a descriptive header like \"Contents of [path] (directory-specific instructions for [scope]):\" followed by <instructions> tags. These guidance files provide directory-specific instructions that take precedence for files in that directory and should be followed carefully. Apply only the parts of these guidance files that are relevant to the current files and task; they define constraints, not extra work to perform by default."
}

type neoGuidanceFile struct {
	URI     string
	Content string
}

func neoGuidanceFiles(guidance map[string]any) []neoGuidanceFile {
	rawFiles := firstArray(guidance["files"], guidance["guidanceFiles"], guidance["guidance_files"])
	files := make([]neoGuidanceFile, 0, len(rawFiles))
	seen := map[string]bool{}
	for _, raw := range rawFiles {
		file := neoGuidanceFileFromAny(raw)
		if strings.TrimSpace(file.Content) == "" {
			continue
		}
		key := file.URI + "\x00" + file.Content
		if seen[key] {
			continue
		}
		seen[key] = true
		files = append(files, file)
	}
	return files
}

func neoGuidanceFileFromAny(value any) neoGuidanceFile {
	if text := stringValue(value); text != "" {
		return neoGuidanceFile{Content: text}
	}
	m := mapValue(value)
	return neoGuidanceFile{
		URI:     firstNonEmptyString(m["uri"], m["path"], m["name"]),
		Content: firstNonEmptyString(m["content"], m["text"], m["instructions"], m["body"]),
	}
}

func neoGuidanceName(uri string) string {
	path := neoGuidancePath(uri)
	if path == "" {
		return "AGENTS.md"
	}
	path = strings.TrimRight(path, "/")
	if idx := strings.LastIndex(path, "/"); idx >= 0 && idx+1 < len(path) {
		return path[idx+1:]
	}
	return path
}

func neoGuidanceScope(uri string) string {
	path := neoGuidancePath(uri)
	if path == "" {
		return "/"
	}
	path = strings.TrimRight(path, "/")
	if idx := strings.LastIndex(path, "/"); idx > 0 {
		return path[:idx]
	}
	return "/"
}

func neoGuidancePath(uri string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return ""
	}
	if parsed, err := url.Parse(uri); err == nil && parsed.Path != "" {
		if unescaped, err := url.PathUnescape(parsed.Path); err == nil {
			return unescaped
		}
		return parsed.Path
	}
	if unescaped, err := url.PathUnescape(uri); err == nil {
		return unescaped
	}
	return uri
}

func neoEnvironmentBlock(request neoInferenceRequest, deep bool) string {
	lines := []string{
		"# Environment",
		"Here is useful information about the environment you are running in:",
		"Today's date: " + time.Now().Format("Mon Jan 02 2006"),
	}
	if cwd := stringValue(request.Environment["workingDirectory"]); cwd != "" {
		lines = append(lines, "Working directory: "+cwd)
	} else {
		lines = append(lines, "Working directory: (none)")
	}
	if root := stringValue(request.Environment["workspaceRoot"]); root != "" {
		lines = append(lines, "Workspace root: "+root)
	} else {
		lines = append(lines, "Workspace root: (none)")
	}
	if platform := neoPlatformText(request.Environment["platform"]); platform != "" {
		lines = append(lines, "Operating system: "+platform)
	}
	if request.ThreadID != "" {
		lines = append(lines, "Amp Thread ID: "+request.ThreadID)
	}
	if !deep {
		if listing := stringValue(request.Environment["rootDirectoryListing"]); listing != "" {
			lines = append(lines, "## Directory listing\nList of files (top-level only) in the user's workspace:\n"+listing)
		}
	}
	return strings.Join(lines, "\n")
}

func neoPlatformText(value any) string {
	m := mapValue(value)
	if len(m) == 0 {
		return stringValue(value)
	}
	osName := stringValue(m["os"])
	if osName == "" {
		osName = stringValue(m["name"])
	}
	if osName == "" {
		return ""
	}
	if version := stringValue(m["osVersion"]); version != "" {
		osName += " (" + version + ")"
	}
	if arch := stringValue(m["cpuArchitecture"]); arch != "" {
		osName += " on " + arch
	}
	return osName
}

type neoPromptSkill struct {
	Name              string
	Description       string
	BaseDir           string
	Location          string
	Frontmatter       map[string]any
	ExcludeAgentModes any
}

func neoSkillsPrompt(request neoInferenceRequest, deep bool) string {
	skills := neoPromptSkills(request)
	if len(skills) == 0 {
		return ""
	}
	filtered := make([]neoPromptSkill, 0, len(skills))
	for _, skill := range skills {
		if boolValue(skill.Frontmatter["disable-model-invocation"]) || neoSkillExcludedForMode(skill, request.AgentMode) {
			continue
		}
		filtered = append(filtered, skill)
	}
	if len(filtered) == 0 {
		return ""
	}
	if deep {
		lines := []string{
			"## Skills",
			"In your workspace you have skills the user created. A **skill** is a guide for proven techniques, patterns, or tools. If a skill exists for a task, you must do it. The following skills provide specialized instructions for specific tasks..",
			"### Available skills",
		}
		for _, skill := range filtered {
			lines = append(lines, "- "+skill.Name+": "+skill.Description+" (file: "+neoSkillLocation(skill)+")")
		}
		lines = append(lines,
			"### How to use skills",
			"- Discovery: The list above is the skills available in this session (name + description + file path). Skill bodies live on disk at the listed paths. Use the "+neoSkillToolName+" tool to load them.",
			"- Trigger rules: If the user names a skill (with `$SkillName` or plain text) OR the task clearly matches a skill's description shown above, you must use that skill for that turn. Multiple mentions mean use them all. Do not carry skills across turns unless re-mentioned.",
			"- Missing/blocked: If a named skill isn't in the list or the path can't be read, say so briefly and continue with the best fallback.",
			"- How to use a skill (progressive disclosure):",
			"  1) After deciding to use a skill, call the "+neoSkillToolName+" tool to load it. Read only enough to follow the workflow.",
			"  2) When `SKILL.md` references relative paths (e.g., `scripts/foo.py`), resolve them relative to the skill directory listed above first.",
			"  3) If `SKILL.md` points to extra folders such as `references/`, load only the specific files needed for the request; don't bulk-load everything.",
			"  4) If `scripts/` exist, prefer running or patching them instead of retyping large code blocks.",
			"  5) If `assets/` or templates exist, reuse them instead of recreating from scratch.",
			"- Context hygiene:",
			"  - Keep context small: summarize long sections instead of pasting them; only load extra files when needed.",
			"  - Avoid deep reference-chasing: prefer opening only files directly linked from `SKILL.md` unless you're blocked.",
			"- Safety and fallback: If a skill can't be applied cleanly (missing files, unclear instructions), state the issue, pick the next-best approach, and continue.",
		)
		return strings.Join(lines, "\n")
	}

	entries := make([]string, 0, len(filtered))
	for _, skill := range filtered {
		entries = append(entries, strings.Join([]string{
			"  <skill>",
			"    <name>" + skill.Name + "</name>",
			"    <description>" + skill.Description + "</description>",
			"    <location>" + neoSkillLocation(skill) + "</location>",
			"  </skill>",
		}, "\n"))
	}
	return strings.Join([]string{
		"## Skills",
		"In your workspace you have skills the user created. A **skill** is a guide for proven techniques, patterns, or tools. If a skill exists for a task, you must do it. The following skills provide specialized instructions for specific tasks.",
		"Use the " + neoSkillToolName + " tool to load a skill when the task matches its description.",
		"After loading a skill, follow only the workflow steps relevant to the current request. Skills are aids for known techniques, not checklists to exhaust.",
		"",
		"Loaded skills appear as `<loaded_skill name=\"...\">` in the conversation.",
		"",
		"<available_skills>",
		strings.Join(entries, "\n"),
		"</available_skills>",
	}, "\n")
}

func neoPromptSkills(request neoInferenceRequest) []neoPromptSkill {
	byName := map[string]neoPromptSkill{}
	add := func(skill neoPromptSkill) {
		skill.Name = strings.TrimSpace(skill.Name)
		if skill.Name == "" {
			return
		}
		if existing, ok := byName[skill.Name]; ok {
			byName[skill.Name] = mergeNeoPromptSkill(existing, skill)
			return
		}
		byName[skill.Name] = skill
	}
	for _, key := range []string{"skills", "skillInventory"} {
		addNeoPromptSkills(request.Capabilities[key], add)
	}
	addNeoPromptSkills(request.Capabilities["skillNames"], add)
	for _, tool := range request.Tools {
		addNeoPromptSkills(tool.Meta["skillNames"], add)
	}
	out := make([]neoPromptSkill, 0, len(byName))
	for _, skill := range byName {
		out = append(out, skill)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func addNeoPromptSkills(value any, add func(neoPromptSkill)) {
	switch v := value.(type) {
	case string:
		if name := strings.TrimSpace(v); name != "" {
			add(neoPromptSkill{Name: name})
		}
	case []string:
		for _, item := range v {
			addNeoPromptSkills(item, add)
		}
	case []any:
		for _, item := range v {
			addNeoPromptSkills(item, add)
		}
	case map[string]any:
		add(neoPromptSkillFromMap(v))
	}
}

func neoPromptSkillFromMap(m map[string]any) neoPromptSkill {
	frontmatter := mapValue(m["frontmatter"])
	location := firstNonEmptyString(m["location"], m["path"])
	baseDir := firstNonEmptyString(m["baseDir"], m["base_dir"], m["dir"])
	if baseDir == "" && strings.HasSuffix(location, "/SKILL.md") {
		baseDir = strings.TrimSuffix(location, "/SKILL.md")
	}
	description := firstNonEmptyString(m["description"], frontmatter["description"])
	return neoPromptSkill{
		Name:              firstNonEmptyString(m["name"], m["id"], m["title"], m["displayName"]),
		Description:       description,
		BaseDir:           baseDir,
		Location:          location,
		Frontmatter:       frontmatter,
		ExcludeAgentModes: m["excludeAgentModes"],
	}
}

func mergeNeoPromptSkill(existing, next neoPromptSkill) neoPromptSkill {
	if existing.Description == "" {
		existing.Description = next.Description
	}
	if existing.BaseDir == "" {
		existing.BaseDir = next.BaseDir
	}
	if existing.Location == "" {
		existing.Location = next.Location
	}
	if len(existing.Frontmatter) == 0 {
		existing.Frontmatter = next.Frontmatter
	}
	if existing.ExcludeAgentModes == nil {
		existing.ExcludeAgentModes = next.ExcludeAgentModes
	}
	return existing
}

func neoSkillLocation(skill neoPromptSkill) string {
	if strings.TrimSpace(skill.Location) != "" {
		return strings.TrimSpace(skill.Location)
	}
	if strings.TrimSpace(skill.BaseDir) != "" {
		return strings.TrimRight(strings.TrimSpace(skill.BaseDir), "/") + "/SKILL.md"
	}
	return skill.Name + "/SKILL.md"
}

func neoSkillExcludedForMode(skill neoPromptSkill, agentMode string) bool {
	mode := strings.TrimSpace(agentMode)
	if mode == "" {
		return false
	}
	excluded := map[string]bool{}
	addNeoSkillNames(excluded, skill.ExcludeAgentModes)
	return excluded[mode]
}

func compactStrings(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, value)
		}
	}
	return out
}

func neoGuidanceText(guidance map[string]any) string {
	if len(guidance) == 0 {
		return ""
	}
	seen := map[string]bool{}
	parts := make([]string, 0, 4)
	collectNeoGuidanceText(guidance, "", seen, &parts)
	return strings.Join(parts, "\n\n")
}

func collectNeoGuidanceText(value any, key string, seen map[string]bool, parts *[]string) {
	switch v := value.(type) {
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return
		}
		if key != "" && !isNeoGuidanceTextKey(key) && !looksLikeNeoGuidance(text) {
			return
		}
		if !seen[text] {
			seen[text] = true
			*parts = append(*parts, text)
		}
	case []any:
		if key == "" || isNeoGuidanceContainerKey(key) {
			for _, item := range v {
				collectNeoGuidanceText(item, "", seen, parts)
			}
		}
	case map[string]any:
		for childKey, childValue := range v {
			if key == "" || isNeoGuidanceContainerKey(key) || isNeoGuidanceKey(childKey) {
				collectNeoGuidanceText(childValue, childKey, seen, parts)
			}
		}
	}
}

func isNeoGuidanceKey(key string) bool {
	return isNeoGuidanceTextKey(key) || isNeoGuidanceContainerKey(key)
}

func isNeoGuidanceTextKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "text", "content", "prompt", "systemprompt", "system_prompt", "instructions", "instruction", "guidance", "message", "body", "markdown", "description":
		return true
	default:
		return false
	}
}

func isNeoGuidanceContainerKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "guidance", "guidances", "guidanceinventory", "guidance_inventory", "inventory", "items", "entries", "files", "system", "prompts", "instructions":
		return true
	default:
		return false
	}
}

func looksLikeNeoGuidance(text string) bool {
	lower := strings.ToLower(text)
	return strings.Contains(lower, "you are amp") ||
		strings.Contains(lower, "coding agent") ||
		strings.Contains(lower, "available skills") ||
		strings.Contains(lower, "registered tools") ||
		strings.Contains(lower, "use registered tools")
}

func neoGuidanceInventory(guidance map[string]any) []any {
	return firstArray(
		guidance["guidanceInventory"],
		guidance["guidance_inventory"],
		guidance["inventory"],
		guidance["guidances"],
		guidance["guidance"],
		guidance["items"],
	)
}

func neoSkillNames(request neoInferenceRequest) []string {
	seen := map[string]bool{}
	for _, key := range []string{"skills", "skillNames", "skillInventory"} {
		addNeoSkillNames(seen, request.Capabilities[key])
	}
	for _, tool := range request.Tools {
		addNeoSkillNames(seen, tool.Meta["skillNames"])
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func neoSkillNamesFromAny(value any) []string {
	seen := map[string]bool{}
	addNeoSkillNames(seen, value)
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func addNeoSkillNames(seen map[string]bool, value any) {
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			seen[strings.TrimSpace(v)] = true
		}
	case []any:
		for _, item := range v {
			addNeoSkillNames(seen, item)
		}
	case []string:
		for _, item := range v {
			addNeoSkillNames(seen, item)
		}
	case map[string]any:
		for _, key := range []string{"name", "id", "title", "displayName"} {
			if name := stringValue(v[key]); name != "" {
				addNeoSkillNames(seen, name)
				return
			}
		}
	}
}

func anthropicNeoMessages(history []neoHistoryMessage) []any {
	history = sanitizeNeoHistoryToolPairs(history)
	messages := make([]any, 0, len(history))
	for _, msg := range history {
		switch msg.Role {
		case "tool":
			messages = append(messages, map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": msg.ToolCallID, "content": msg.Text}}})
		case "assistant":
			content := make([]any, 0, 1+len(msg.ToolCalls))
			if msg.Text != "" {
				content = append(content, map[string]any{"type": "text", "text": msg.Text})
			}
			for _, call := range msg.ToolCalls {
				content = append(content, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": call.Input})
			}
			messages = append(messages, map[string]any{"role": "assistant", "content": content})
		default:
			messages = append(messages, map[string]any{"role": "user", "content": anthropicNeoUserContent(msg)})
		}
	}
	return messages
}

func openAINeoMessages(history []neoHistoryMessage, system string) []any {
	history = sanitizeNeoHistoryToolPairs(history)
	messages := []any{map[string]any{"role": "system", "content": system}}
	for _, msg := range history {
		switch msg.Role {
		case "tool":
			messages = append(messages, map[string]any{"role": "tool", "tool_call_id": msg.ToolCallID, "content": msg.Text})
		case "assistant":
			assistant := map[string]any{"role": "assistant", "content": msg.Text}
			if len(msg.ToolCalls) > 0 {
				calls := make([]any, 0, len(msg.ToolCalls))
				for _, call := range msg.ToolCalls {
					args, _ := json.Marshal(call.Input)
					calls = append(calls, map[string]any{"id": call.ID, "type": "function", "function": map[string]any{"name": call.Name, "arguments": string(args)}})
				}
				assistant["tool_calls"] = calls
				if msg.Text == "" {
					assistant["content"] = nil
				}
			}
			messages = append(messages, assistant)
		default:
			messages = append(messages, map[string]any{"role": "user", "content": openAINeoUserContent(msg)})
		}
	}
	return messages
}

func googleNeoContents(history []neoHistoryMessage, system string) []any {
	history = sanitizeNeoHistoryToolPairs(history)
	contents := []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": system}}}}
	for _, msg := range history {
		switch msg.Role {
		case "tool":
			contents = append(contents, map[string]any{"role": "user", "parts": []any{map[string]any{"functionResponse": map[string]any{"name": fallbackString(msg.ToolName, msg.ToolCallID), "response": map[string]any{"content": msg.Text}}}}})
		case "assistant":
			parts := make([]any, 0, 1+len(msg.ToolCalls))
			if msg.Text != "" {
				parts = append(parts, map[string]any{"text": msg.Text})
			}
			for _, call := range msg.ToolCalls {
				parts = append(parts, map[string]any{"functionCall": map[string]any{"name": call.Name, "args": call.Input}})
			}
			contents = append(contents, map[string]any{"role": "model", "parts": parts})
		default:
			contents = append(contents, map[string]any{"role": "user", "parts": googleNeoUserParts(msg)})
		}
	}
	return contents
}

func anthropicNeoUserContent(msg neoHistoryMessage) []any {
	if len(msg.Content) == 0 {
		return []any{map[string]any{"type": "text", "text": msg.Text}}
	}
	content := make([]any, 0, len(msg.Content))
	for _, raw := range msg.Content {
		block := mapValue(raw)
		switch stringValue(block["type"]) {
		case "text":
			if text := stringValue(block["text"]); text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		case "image", "input_image", "image_url":
			if image := anthropicNeoImageBlock(block); len(image) > 0 {
				content = append(content, image)
			} else if text := neoAttachmentFallbackText(block); text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		case "document":
			if source := mapValue(block["source"]); len(source) > 0 {
				content = append(content, map[string]any{"type": "document", "source": source})
			}
		default:
			if text := neoAttachmentFallbackText(block); text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		}
	}
	if len(content) == 0 {
		content = append(content, map[string]any{"type": "text", "text": msg.Text})
	}
	return content
}

func openAINeoUserContent(msg neoHistoryMessage) any {
	if len(msg.Content) == 0 {
		return msg.Text
	}
	content := make([]any, 0, len(msg.Content))
	for _, raw := range msg.Content {
		block := mapValue(raw)
		switch stringValue(block["type"]) {
		case "text":
			if text := stringValue(block["text"]); text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		case "image", "input_image", "image_url":
			if imageURL := neoImageURL(block); imageURL != "" {
				content = append(content, map[string]any{"type": "image_url", "image_url": map[string]any{"url": imageURL}})
			} else if text := neoAttachmentFallbackText(block); text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		default:
			if text := neoAttachmentFallbackText(block); text != "" {
				content = append(content, map[string]any{"type": "text", "text": text})
			}
		}
	}
	if len(content) == 1 && stringValue(mapValue(content[0])["type"]) == "text" {
		return stringValue(mapValue(content[0])["text"])
	}
	if len(content) == 0 {
		return msg.Text
	}
	return content
}

func googleNeoUserParts(msg neoHistoryMessage) []any {
	if len(msg.Content) == 0 {
		return []any{map[string]any{"text": msg.Text}}
	}
	parts := make([]any, 0, len(msg.Content))
	for _, raw := range msg.Content {
		block := mapValue(raw)
		switch stringValue(block["type"]) {
		case "text":
			if text := stringValue(block["text"]); text != "" {
				parts = append(parts, map[string]any{"text": text})
			}
		case "image", "input_image", "image_url":
			if data, mediaType := neoImageBase64(block); data != "" {
				parts = append(parts, map[string]any{"inlineData": map[string]any{"mimeType": fallbackString(mediaType, "image/png"), "data": data}})
			} else if imageURL := neoImageURL(block); imageURL != "" {
				parts = append(parts, map[string]any{"fileData": map[string]any{"fileUri": imageURL, "mimeType": stringValue(block["media_type"])}})
			}
		default:
			if text := neoAttachmentFallbackText(block); text != "" {
				parts = append(parts, map[string]any{"text": text})
			}
		}
	}
	if len(parts) == 0 {
		parts = append(parts, map[string]any{"text": msg.Text})
	}
	return parts
}

func anthropicNeoImageBlock(block map[string]any) map[string]any {
	if source := mapValue(block["source"]); len(source) > 0 {
		return map[string]any{"type": "image", "source": source}
	}
	if data, mediaType := neoImageBase64(block); data != "" {
		return map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": fallbackString(mediaType, "image/png"), "data": data}}
	}
	if imageURL := neoImageURL(block); imageURL != "" {
		return map[string]any{"type": "image", "source": map[string]any{"type": "url", "url": imageURL}}
	}
	return nil
}

func neoImageURL(block map[string]any) string {
	if imageURL := stringValue(block["image_url"]); imageURL != "" {
		return imageURL
	}
	if imageURL := stringValue(mapValue(block["image_url"])["url"]); imageURL != "" {
		return imageURL
	}
	if urlValue := firstNonEmptyString(block["url"], block["uri"], block["href"]); urlValue != "" {
		return urlValue
	}
	if source := mapValue(block["source"]); len(source) > 0 {
		if urlValue := firstNonEmptyString(source["url"], source["uri"], source["href"]); urlValue != "" {
			return urlValue
		}
		if data, mediaType := neoImageBase64(source); data != "" {
			return "data:" + fallbackString(mediaType, "image/png") + ";base64," + data
		}
	}
	if data, mediaType := neoImageBase64(block); data != "" {
		return "data:" + fallbackString(mediaType, "image/png") + ";base64," + data
	}
	return ""
}

func neoImageBase64(block map[string]any) (string, string) {
	data := firstNonEmptyString(block["data"], block["base64"])
	mediaType := firstNonEmptyString(block["media_type"], block["mediaType"], block["mime_type"], block["mimeType"])
	if data == "" {
		if source := mapValue(block["source"]); len(source) > 0 {
			return neoImageBase64(source)
		}
	}
	if strings.HasPrefix(data, "data:") {
		header, encoded, ok := strings.Cut(data, ",")
		if ok {
			data = encoded
			if strings.Contains(header, ";base64") {
				mediaType = strings.TrimPrefix(strings.TrimSuffix(header, ";base64"), "data:")
			}
		}
	}
	return data, mediaType
}

func neoAttachmentFallbackText(block map[string]any) string {
	switch stringValue(block["type"]) {
	case "text", "tool_result":
		return ""
	}
	name := firstNonEmptyString(block["name"], block["filename"], block["file_name"], block["title"], block["url"], block["uri"])
	if name == "" {
		return ""
	}
	return "[attachment: " + name + "]"
}

func sanitizeNeoHistoryToolPairs(history []neoHistoryMessage) []neoHistoryMessage {
	answered := map[string]bool{}
	for _, msg := range history {
		if msg.Role == "tool" && msg.ToolCallID != "" {
			answered[msg.ToolCallID] = true
		}
	}
	if len(answered) == 0 {
		out := make([]neoHistoryMessage, 0, len(history))
		for _, msg := range history {
			if msg.Role == "assistant" && len(msg.ToolCalls) > 0 && msg.Text == "" {
				continue
			}
			if msg.Role == "assistant" {
				msg.ToolCalls = nil
			}
			if msg.Role != "tool" {
				out = append(out, msg)
			}
		}
		return out
	}

	keptCalls := map[string]bool{}
	out := make([]neoHistoryMessage, 0, len(history))
	for _, msg := range history {
		switch msg.Role {
		case "assistant":
			hadCalls := len(msg.ToolCalls) > 0
			if hadCalls {
				calls := make([]neoToolCall, 0, len(msg.ToolCalls))
				for _, call := range msg.ToolCalls {
					if call.ID != "" && answered[call.ID] {
						calls = append(calls, call)
						keptCalls[call.ID] = true
					}
				}
				msg.ToolCalls = calls
				if msg.Text == "" && len(msg.ToolCalls) == 0 {
					continue
				}
			}
			out = append(out, msg)
		case "tool":
			if msg.ToolCallID != "" && keptCalls[msg.ToolCallID] {
				out = append(out, msg)
			}
		default:
			out = append(out, msg)
		}
	}
	return out
}

func anthropicNeoTools(tools []neoToolSpec) []any {
	out := make([]any, 0, len(tools))
	for _, tool := range tools {
		schema := tool.InputSchema
		if len(schema) == 0 {
			schema = map[string]any{"type": "object"}
		}
		out = append(out, map[string]any{"name": tool.Name, "description": tool.Description, "input_schema": schema})
	}
	return out
}

func openAINeoTools(tools []neoToolSpec) []any {
	out := make([]any, 0, len(tools))
	for _, tool := range tools {
		schema := tool.InputSchema
		if len(schema) == 0 {
			schema = map[string]any{"type": "object"}
		}
		out = append(out, map[string]any{"type": "function", "function": map[string]any{"name": tool.Name, "description": tool.Description, "parameters": schema}})
	}
	return out
}

func googleNeoTools(tools []neoToolSpec) []any {
	out := make([]any, 0, len(tools))
	for _, tool := range tools {
		schema := tool.InputSchema
		if len(schema) == 0 {
			schema = map[string]any{"type": "object"}
		}
		out = append(out, map[string]any{"name": tool.Name, "description": tool.Description, "parameters": schema})
	}
	return out
}

func openAIReasoningEffort(effort string) string {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "minimal", "low", "medium", "high", "xhigh", "max":
		return strings.ToLower(strings.TrimSpace(effort))
	case "none":
		return "low"
	default:
		return "medium"
	}
}

func parseToolArguments(value any) map[string]any {
	if m, ok := asMap(value); ok {
		return m
	}
	text := stringValue(value)
	if text == "" {
		return map[string]any{}
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(text), &decoded); err == nil {
		return decoded
	}
	return map[string]any{"input": text}
}

func mergeNeoUsage(dst, src map[string]any) map[string]any {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		return cloneMap(src)
	}
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func normalizeNeoUsage(usage map[string]any) map[string]any {
	if len(usage) == 0 {
		return nil
	}
	input := numberFrom(usage["inputTokens"], usage["input_tokens"], usage["prompt_tokens"], usage["promptTokenCount"])
	output := numberFrom(usage["outputTokens"], usage["output_tokens"], usage["completion_tokens"], usage["candidatesTokenCount"])
	cacheCreation := numberFrom(usage["cacheCreationInputTokens"], usage["cache_creation_input_tokens"])
	cacheRead := numberFrom(usage["cacheReadInputTokens"], usage["cache_read_input_tokens"], usage["cachedContentTokenCount"], nestedNumberFrom(usage["prompt_tokens_details"], "cached_tokens"))
	total := numberFrom(usage["totalInputTokens"], usage["total_input_tokens"], usage["prompt_tokens"], usage["promptTokenCount"], input+valueOrZero(cacheCreation)+valueOrZero(cacheRead))
	return map[string]any{
		"model":                    omitEmpty(stringValue(usage["model"])),
		"maxInputTokens":           numberFrom(usage["maxInputTokens"], usage["max_input_tokens"]),
		"inputTokens":              input,
		"outputTokens":             output,
		"cacheCreationInputTokens": cacheCreation,
		"cacheReadInputTokens":     cacheRead,
		"totalInputTokens":         total,
		"timestamp":                time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func normalizeNeoRetryScheduled(msg map[string]any) map[string]any {
	retryAt := numberFrom(msg["retryAt"])
	if retryAt <= 0 {
		retryAt = int(time.Now().Add(time.Second).UnixMilli())
	}
	attempt := numberFrom(msg["attempt"])
	if attempt <= 0 {
		attempt = 1
	}
	maxAttempts := numberFrom(msg["maxAttempts"])
	if maxAttempts < attempt {
		maxAttempts = attempt
	}
	reason := stringValue(msg["reason"])
	if reason == "" {
		reason = "retry_requested"
	}
	return map[string]any{"type": "retry_scheduled", "retryAt": retryAt, "attempt": attempt, "maxAttempts": maxAttempts, "reason": reason}
}

func normalizedNeoThreadStatus(status string) string {
	switch status {
	case "merging", "merged":
		return status
	default:
		return ""
	}
}

func neoThreadStatusValue(status string) any {
	if normalized := normalizedNeoThreadStatus(status); normalized != "" {
		return normalized
	}
	return json.RawMessage("null")
}

func normalizeNeoCompactionRecords(raw any) []map[string]any {
	rawRecords := arrayValue(raw)
	records := make([]map[string]any, 0, len(rawRecords))
	fallbackCreatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, rawRecord := range rawRecords {
		record, ok := neoCompactionRecord(mapValue(rawRecord), fallbackCreatedAt)
		if ok {
			records = append(records, record)
		}
	}
	return records
}

func neoCompactionRecord(raw map[string]any, fallbackCreatedAt string) (map[string]any, bool) {
	cutMessageID := messageIDValue(raw["cutMessageId"])
	if cutMessageID == "" {
		return nil, false
	}
	createdAt := stringValue(raw["createdAt"])
	if createdAt == "" {
		createdAt = fallbackCreatedAt
	}
	return map[string]any{"cutMessageId": cutMessageID, "createdAt": createdAt}, true
}

func normalizeNeoExecutorStatus(msg map[string]any) map[string]any {
	status := stringValue(msg["status"])
	switch status {
	case "starting", "running", "failed":
	default:
		status = "failed"
	}
	out := map[string]any{
		"type":    "executor_status",
		"spawnId": omitEmpty(firstNonEmptyString(msg["spawnId"], msg["requestId"])),
		"status":  status,
		"message": omitEmpty(stringValue(msg["message"])),
	}
	if executorID := firstNonEmptyString(msg["executorId"], msg["clientId"]); executorID != "" {
		out["executorId"] = executorID
	}
	if details := normalizeNeoExecutorStatusDetails(mapValue(msg["details"])); len(details) > 0 {
		out["details"] = details
	}
	return out
}

func normalizeNeoExecutorStatusDetails(details map[string]any) map[string]any {
	out := cloneMap(details)
	if reason := stringValue(out["reasonCode"]); reason != "" && !validNeoExecutorReason(reason) {
		out["reasonCode"] = "spawn_failed"
	}
	return out
}

func validNeoExecutorReason(reason string) bool {
	switch reason {
	case "spawn_requested", "spawn_rejected", "environment_recovering", "waiting_for_executor_connect", "executor_connected", "executor_disconnected", "connect_timeout", "executor_connect_rejected", "spawn_failed", "restart_failed", "environment_missing":
		return true
	default:
		return false
	}
}

func normalizeNeoExecutorError(msg map[string]any) map[string]any {
	out := map[string]any{
		"type":    "executor_error",
		"message": fallbackString(msg["message"], "Executor error"),
	}
	if toolCallID := stringValue(msg["toolCallId"]); toolCallID != "" {
		out["toolCallId"] = toolCallID
	}
	code := stringValue(msg["code"])
	if code == "" || !validNeoExecutorErrorCode(code) {
		code = "INTERNAL_ERROR"
	}
	out["code"] = code
	return out
}

func validNeoExecutorErrorCode(code string) bool {
	switch code {
	case "NOT_CONNECTED", "ACCESS_DENIED", "INVALID_MESSAGE", "TOOL_NOT_FOUND", "LEASE_NOT_FOUND", "DUPLICATE_TOOL", "INTERNAL_ERROR":
		return true
	default:
		return false
	}
}

func normalizeNeoToolLeaseRevoked(msg map[string]any) map[string]any {
	reason := stringValue(msg["reason"])
	switch reason {
	case "executor_disconnected", "reassigned", "user_canceled":
	default:
		reason = "user_canceled"
	}
	return map[string]any{
		"type":       "executor_tool_lease_revoked",
		"toolCallId": stringValue(msg["toolCallId"]),
		"reason":     reason,
	}
}

func normalizeNeoToolResultAck(msg map[string]any) map[string]any {
	return map[string]any{"type": "executor_tool_result_ack", "toolCallId": stringValue(msg["toolCallId"])}
}

func normalizeNeoToolApprovalResponse(msg map[string]any) map[string]any {
	toolCallID := firstNonEmptyString(msg["toolCallId"], msg["toolUseId"], msg["id"])
	out := map[string]any{
		"type":       "executor_tool_approval_response",
		"toolCallId": toolCallID,
		"accepted":   boolValue(msg["accepted"]),
	}
	if input := normalizeNeoToolApprovalInput(msg); len(input) > 0 {
		out["input"] = input
	}
	return out
}

func normalizeNeoToolApprovalInput(msg map[string]any) map[string]any {
	input := cloneMap(mapValue(msg["input"]))
	if feedback := stringValue(msg["input"]); feedback != "" {
		input["denyFeedback"] = feedback
	}
	if feedback := firstNonEmptyString(msg["denyFeedback"], msg["feedback"]); feedback != "" {
		input["denyFeedback"] = feedback
	}
	if askAnswers := mapValue(msg["askAnswers"]); len(askAnswers) > 0 {
		input["askAnswers"] = cloneMap(askAnswers)
	}
	return input
}

func toolApprovalQueuePayload(approvals []any) map[string]any {
	if approvals == nil {
		approvals = []any{}
	}
	return map[string]any{"type": "tool_approval_queue", "approvals": approvals}
}

func neoApprovalKey(approval map[string]any) string {
	return firstNonEmptyString(approval["toolCallId"], approval["toolUseId"], approval["id"])
}

func writeNeoJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func readNeoJSON(r io.Reader) map[string]any {
	var payload map[string]any
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return map[string]any{}
	}
	return payload
}

func parseWebSocketProtocols(header string) []string {
	if strings.TrimSpace(header) == "" {
		return nil
	}
	parts := strings.Split(header, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func extractNeoActorID(protocols []string) string {
	for _, protocol := range protocols {
		if strings.HasPrefix(protocol, "rivet_actor.") {
			return strings.TrimPrefix(protocol, "rivet_actor.")
		}
	}
	return ""
}

func extractThreadIDFromActorBody(body map[string]any, key string) string {
	for _, candidate := range []any{body, mapValue(body["input"]), parseJSONString(body["input"]), parseJSONString(key)} {
		if found := findThreadID(candidate); found != "" {
			return found
		}
	}
	if match := neoThreadIDPattern.FindString(key); match != "" {
		return match
	}
	return "T-" + randomUUIDLike()
}

func findThreadID(value any) string {
	if s := stringValue(value); strings.HasPrefix(s, "T-") {
		return s
	}
	if items, ok := value.([]any); ok {
		for _, item := range items {
			if found := findThreadID(item); found != "" {
				return found
			}
		}
	}
	m, ok := asMap(value)
	if !ok {
		return ""
	}
	for _, key := range []string{"threadId", "threadID", "thread_id"} {
		if s := stringValue(m[key]); strings.HasPrefix(s, "T-") {
			return s
		}
	}
	return ""
}

func neoActorRecord(actorID, name, key string) map[string]any {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var keyValue any
	if key != "" {
		keyValue = key
	}
	return map[string]any{
		"actor_id":       actorID,
		"name":           name,
		"key":            keyValue,
		"create_ts":      now,
		"start_ts":       now,
		"connectable_ts": now,
		"sleep_ts":       nil,
		"destroy_ts":     nil,
	}
}

func textFromBlocks(blocks []any) string {
	var out strings.Builder
	for _, block := range blocks {
		m := mapValue(block)
		switch stringValue(m["type"]) {
		case "text":
			out.WriteString(stringValue(m["text"]))
		case "tool_result":
			out.WriteString(runToText(m["run"]))
		}
	}
	return out.String()
}

func neoAssistantHistoryContent(blocks []any) (string, []neoToolCall) {
	var text strings.Builder
	calls := make([]neoToolCall, 0)
	for _, block := range blocks {
		m := mapValue(block)
		switch stringValue(m["type"]) {
		case "text":
			text.WriteString(stringValue(m["text"]))
		case "tool_use":
			name := stringValue(m["name"])
			if name == "" {
				continue
			}
			calls = append(calls, neoToolCall{
				ID:    fallbackString(m["id"], newNeoToolCallID()),
				Name:  name,
				Input: mapValue(m["input"]),
			})
		}
	}
	return text.String(), calls
}

func neoToolResultHistoryContent(blocks []any, toolNames map[string]string, parentToolUseID string) []neoHistoryMessage {
	results := make([]neoHistoryMessage, 0)
	for _, block := range blocks {
		m := mapValue(block)
		if stringValue(m["type"]) != "tool_result" {
			continue
		}
		run := mapValue(m["run"])
		if !neoToolRunTerminal(run) {
			continue
		}
		toolCallID := firstNonEmptyString(m["toolUseID"], m["tool_use_id"], m["toolCallId"])
		if toolCallID == "" {
			continue
		}
		results = append(results, neoHistoryMessage{Role: "tool", ToolCallID: toolCallID, ToolName: toolNames[toolCallID], Text: runToText(run), ParentToolUseID: parentToolUseID})
	}
	return results
}

func neoToolProgressRun(progress any, existingRun map[string]any) (map[string]any, bool) {
	if progressMap, ok := asMap(progress); ok {
		status := stringValue(progressMap["status"])
		if status != "" {
			if status == "in-progress" || neoTerminalToolRunStatus(status) {
				return cloneMap(progressMap), true
			}
			return nil, false
		}
	}

	var merged any
	if len(existingRun) > 0 {
		merged = existingRun["progress"]
	}
	if !emptyNeoProgress(progress) {
		merged = mergeNeoToolProgress(merged, progress)
	}
	run := map[string]any{"status": "in-progress"}
	if !emptyNeoProgress(merged) {
		run["progress"] = merged
	}
	return run, true
}

func neoToolRunTerminal(run map[string]any) bool {
	if len(run) == 0 {
		return false
	}
	return neoTerminalToolRunStatus(stringValue(run["status"]))
}

func neoTerminalToolRunStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "done", "error", "cancelled", "rejected-by-user":
		return true
	default:
		return false
	}
}

func mergeNeoToolProgress(existing, next any) any {
	if emptyNeoProgress(next) {
		return cloneNeoProgress(existing)
	}
	if emptyNeoProgress(existing) {
		return cloneNeoProgress(next)
	}
	if existingMap, ok := asMap(existing); ok {
		if nextMap, ok := asMap(next); ok {
			out := cloneMap(existingMap)
			for key, value := range nextMap {
				out[key] = mergeNeoToolProgress(out[key], value)
			}
			return out
		}
	}
	if existingItems := arrayValue(existing); existingItems != nil {
		if nextItems := arrayValue(next); nextItems != nil {
			out := cloneArray(existingItems)
			out = append(out, cloneArray(nextItems)...)
			return out
		}
	}
	return cloneNeoProgress(next)
}

func emptyNeoProgress(value any) bool {
	if value == nil {
		return true
	}
	if m, ok := asMap(value); ok {
		return len(m) == 0
	}
	if items := arrayValue(value); items != nil {
		return len(items) == 0
	}
	return false
}

func cloneNeoProgress(value any) any {
	if m, ok := asMap(value); ok {
		return cloneMap(m)
	}
	if items := arrayValue(value); items != nil {
		return cloneArray(items)
	}
	return value
}

func runToText(run any) string {
	m, ok := asMap(run)
	if !ok {
		return fmt.Sprint(run)
	}
	for _, key := range []string{"output", "displayMessage", "message", "reason", "text"} {
		if value := stringValue(m[key]); value != "" {
			return value
		}
	}
	if result, ok := m["result"]; ok {
		return fmt.Sprint(result)
	}
	raw, _ := json.Marshal(m)
	return string(raw)
}

func toolResultMessageID(toolCallID string) string {
	suffix := strings.TrimPrefix(toolCallID, "TU-")
	if suffix == "" {
		suffix = randomBase62(22)
	}
	return "M-" + suffix
}

func newNeoMessageID() string  { return "M-" + randomBase62(22) }
func newNeoToolCallID() string { return "TU-" + randomBase62(22) }

func randomUUIDLike() string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", randomHex(8), randomHex(4), randomHex(4), randomHex(4), randomHex(12))
}

func randomHex(n int) string {
	const alphabet = "0123456789abcdef"
	var b strings.Builder
	for i := 0; i < n; i++ {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		b.WriteByte(alphabet[idx.Int64()])
	}
	return b.String()
}

func randomBase62(length int) string {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var b strings.Builder
	for i := 0; i < length; i++ {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		b.WriteByte(alphabet[idx.Int64()])
	}
	return b.String()
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func stringValue(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case fmt.Stringer:
		return value.String()
	default:
		return ""
	}
}

func fallbackString(v any, fallback string) string {
	if s := stringValue(v); s != "" {
		return s
	}
	return fallback
}

func messageIDValue(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case int:
		return fmt.Sprintf("%d", value)
	case int64:
		return fmt.Sprintf("%d", value)
	case float64:
		if value == float64(int64(value)) {
			return fmt.Sprintf("%d", int64(value))
		}
		return fmt.Sprintf("%v", value)
	case json.Number:
		return value.String()
	default:
		return ""
	}
}

func firstNonEmptyString(values ...any) string {
	for _, value := range values {
		if s := stringValue(value); strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func boolValue(v any) bool {
	b, _ := v.(bool)
	return b
}

func intValue(v any) int {
	switch value := v.(type) {
	case int:
		return value
	case float64:
		return int(value)
	case json.Number:
		i, _ := value.Int64()
		return int(i)
	default:
		return 0
	}
}

func numberFrom(values ...any) int {
	for _, value := range values {
		switch typed := value.(type) {
		case int:
			return typed
		case int64:
			return int(typed)
		case float64:
			return int(typed)
		case json.Number:
			i, _ := typed.Int64()
			return int(i)
		}
	}
	return 0
}

func nestedNumberFrom(value any, key string) int {
	m := mapValue(value)
	if len(m) == 0 {
		return 0
	}
	return numberFrom(m[key])
}

func nestedString(value any, key string) string {
	m := mapValue(value)
	if len(m) == 0 {
		return ""
	}
	return stringValue(m[key])
}

func nullableNumberFrom(values ...any) any {
	for _, value := range values {
		switch typed := value.(type) {
		case int:
			return typed
		case int64:
			return int(typed)
		case float64:
			return int(typed)
		case json.Number:
			i, _ := typed.Int64()
			return int(i)
		}
	}
	return nil
}

func valueOrZero(v any) int {
	if v == nil {
		return 0
	}
	return numberFrom(v)
}

func asMap(v any) (map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	m, ok := v.(map[string]any)
	return m, ok
}

func mapValue(v any) map[string]any {
	if m, ok := asMap(v); ok {
		return m
	}
	return map[string]any{}
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func firstMap(values ...any) map[string]any {
	for _, value := range values {
		if m, ok := asMap(value); ok && len(m) > 0 {
			return m
		}
	}
	return map[string]any{}
}

func arrayValue(v any) []any {
	if a, ok := v.([]any); ok {
		return a
	}
	return nil
}

func stringArrayValue(v any) []any {
	switch value := v.(type) {
	case []string:
		out := make([]any, 0, len(value))
		for _, item := range value {
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]any, 0, len(value))
		for _, item := range value {
			if text := stringValue(item); text != "" {
				out = append(out, text)
			}
		}
		return out
	case string:
		if value != "" {
			return []any{value}
		}
	}
	return nil
}

func cloneArray(in []any) []any {
	if in == nil {
		return nil
	}
	return append([]any(nil), in...)
}

func nonNilArray(items []any) []any {
	if items == nil {
		return []any{}
	}
	return items
}

func firstArray(values ...any) []any {
	for _, value := range values {
		if items := arrayValue(value); items != nil {
			return items
		}
	}
	return nil
}

func parseJSONString(v any) any {
	s := stringValue(v)
	if s == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(s), &decoded); err != nil {
		return nil
	}
	return decoded
}

func omitEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
