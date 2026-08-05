package cliproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	log "github.com/sirupsen/logrus"
)

const (
	defaultAmpThreadActorProxyAddr     = "127.0.0.1:6420"
	defaultAmpThreadActorProxyUpstream = "https://actors.ampcode.com"
	rivetTokenHeader                   = "X-Rivet-Token"
)

var ampActorEndpointPattern = regexp.MustCompile(`https://[^"\s/@]+@actors\.ampcode\.com(?:[/?#"\s]|$)`)

type ampThreadActorProxy struct {
	mu         sync.Mutex
	addr       string
	configured string
	upstream   string
	rivetToken string
	hasToken   bool
	server     *http.Server
	listener   net.Listener
	runCtx     context.Context
	stopRun    context.CancelFunc
	conns      map[net.Conn]struct{}
	closed     bool
	done       chan struct{}
}

func (s *Service) startAmpThreadActorProxy(ctx context.Context, cfg *config.Config) {
	if s == nil {
		return
	}

	s.configUpdateMu.Lock()
	if s.ampThreadActorProxy != nil {
		s.configUpdateMu.Unlock()
		return
	}
	s.configUpdateMu.Unlock()

	enabled, addr, upstream := ampThreadActorProxySettings(cfg)
	if !enabled {
		return
	}

	proxy, err := newAmpThreadActorProxy(addr, upstream)
	if err != nil {
		log.Warnf("amp thread actor proxy disabled: %v", err)
		return
	}

	if err := proxy.Start(ctx); err != nil {
		log.Warnf("amp thread actor proxy disabled: %v", err)
		return
	}

	s.configUpdateMu.Lock()
	// If Shutdown already ran, stop the just-started proxy instead of leaking it.
	shutdown := s.shutdownStarted
	if !shutdown {
		s.ampThreadActorProxy = proxy
	}
	s.configUpdateMu.Unlock()

	if shutdown {
		if errShutdown := proxy.Shutdown(context.Background()); errShutdown != nil {
			log.Warnf("amp thread actor proxy shutdown after service stop: %v", errShutdown)
		}
		return
	}

	log.Infof("amp thread actor proxy listening on %s -> %s (rivet token forwarded: %t)", proxy.addr, proxy.upstream, proxy.hasToken)
}

// applyAmpThreadActorProxyConfig reconciles the actor proxy with runtime config
// changes. The caller must hold s.configUpdateMu.
func (s *Service) applyAmpThreadActorProxyConfig(ctx context.Context, cfg *config.Config) {
	if s == nil || cfg == nil || s.shutdownStarted {
		return
	}

	enabled, addr, upstream := ampThreadActorProxySettings(cfg)
	current := s.ampThreadActorProxy
	if current != nil && enabled && current.matchesConfig(addr, upstream) {
		return
	}
	s.ampThreadActorProxy = nil

	if current != nil {
		if errShutdown := current.Shutdown(context.Background()); errShutdown != nil {
			log.Warnf("amp thread actor proxy stop during config update: %v", errShutdown)
		}
	}
	if !enabled {
		return
	}

	proxy, err := newAmpThreadActorProxy(addr, upstream)
	if err != nil {
		log.Warnf("amp thread actor proxy disabled: %v", err)
		return
	}
	if err := proxy.Start(ctx); err != nil {
		log.Warnf("amp thread actor proxy disabled: %v", err)
		return
	}
	if s.shutdownStarted {
		if errShutdown := proxy.Shutdown(context.Background()); errShutdown != nil {
			log.Warnf("amp thread actor proxy shutdown after service stop: %v", errShutdown)
		}
		return
	}
	s.ampThreadActorProxy = proxy
	log.Infof("amp thread actor proxy listening on %s -> %s (rivet token forwarded: %t)", proxy.addr, proxy.upstream, proxy.hasToken)
}

func (p *ampThreadActorProxy) matchesConfig(addr, upstream string) bool {
	if p == nil {
		return false
	}
	parsed, err := url.Parse(strings.TrimSpace(upstream))
	if err != nil {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.configured == strings.TrimSpace(addr) && p.upstream == redactURL(parsed) && p.rivetToken == rivetTokenFromURL(parsed)
}

func ampThreadActorProxySettings(cfg *config.Config) (bool, string, string) {
	if cfg == nil || strings.TrimSpace(cfg.AmpCode.UpstreamURL) == "" {
		return false, "", ""
	}

	// The explicit Neo local runtime owns the local actor endpoint when enabled;
	// this proxy is only a cloud-forwarding fallback and must not share the listener.
	if neo := cfg.AmpCode.NeoLocalRuntime.Enabled; neo != nil && *neo {
		return false, "", ""
	}

	enabled := true
	if cfg.AmpCode.ThreadActorProxyEnabled != nil {
		enabled = *cfg.AmpCode.ThreadActorProxyEnabled
	}
	if !enabled {
		return false, "", ""
	}

	addr := strings.TrimSpace(cfg.AmpCode.ThreadActorProxyAddr)
	if addr == "" {
		addr = defaultAmpThreadActorProxyAddr
	}

	upstream := strings.TrimSpace(cfg.AmpCode.ThreadActorProxyUpstream)
	if upstream == "" {
		upstream = defaultAmpThreadActorProxyUpstreamURL()
	}

	return true, addr, upstream
}

func defaultAmpThreadActorProxyUpstreamURL() string {
	if upstream := ampThreadActorProxyUpstreamFromAmpBinary(); upstream != "" {
		return upstream
	}
	return defaultAmpThreadActorProxyUpstream
}

func ampThreadActorProxyUpstreamFromAmpBinary() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	f, err := os.Open(filepath.Join(home, ".amp", "bin", "amp"))
	if err != nil {
		return ""
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			log.Debugf("amp thread actor proxy: close amp binary: %v", errClose)
		}
	}()

	return ampThreadActorProxyUpstreamFromReader(f)
}

const (
	ampBinaryScanLimit   = 64 << 20
	ampBinaryScanChunk   = 1 << 20
	ampBinaryScanOverlap = 256
)

func ampThreadActorProxyUpstreamFromReader(r io.Reader) string {
	lr := io.LimitReader(r, ampBinaryScanLimit+ampBinaryScanOverlap)
	buf := make([]byte, ampBinaryScanChunk+ampBinaryScanOverlap)
	carried := 0
	for {
		n, err := lr.Read(buf[carried:])
		total := carried + n
		if total > 0 {
			if match := ampActorEndpointPattern.Find(buf[:total]); len(match) != 0 {
				return strings.TrimRight(string(match), "/?#\" \t\r\n")
			}
		}
		if err != nil {
			return ""
		}
		if total > ampBinaryScanOverlap {
			copy(buf, buf[total-ampBinaryScanOverlap:total])
			carried = ampBinaryScanOverlap
		} else {
			copy(buf, buf[:total])
			carried = total
		}
	}
}

func newAmpThreadActorProxy(addr string, upstream string) (*ampThreadActorProxy, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, errors.New("local address is empty")
	}
	if err := validateLoopbackAddr(addr); err != nil {
		return nil, err
	}

	parsed, err := url.Parse(strings.TrimSpace(upstream))
	if err != nil {
		return nil, fmt.Errorf("invalid upstream url: %v", sanitizeURLParseError(err))
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("invalid upstream scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, errors.New("upstream host is empty")
	}

	rivetToken := rivetTokenFromURL(parsed)

	target := *parsed
	target.User = nil

	localHost, _, err := net.SplitHostPort(addr)
	if err != nil {
		localHost = addr
	}

	proxy := httputil.NewSingleHostReverseProxy(&target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		incomingHost := req.Host
		originalDirector(req)
		req.Host = target.Host
		if rivetToken != "" {
			req.Header.Set("Authorization", "Bearer "+rivetToken)
			req.Header.Set(rivetTokenHeader, rivetToken)
			addRivetGatewayToken(req, rivetToken)
			addRivetWebSocketProtocolToken(req, rivetToken)
		}
		if incomingHost != "" {
			req.Header.Set("X-Forwarded-Host", incomingHost)
		}
		// Rewrite Origin only when it names this proxy's own listener, so a page
		// from an unrelated loopback origin is not silently reattributed upstream.
		localPort := localListenerPort(incomingHost)
		requestHostname := localListenerHostname(incomingHost)
		if origin := strings.TrimSpace(req.Header.Get("Origin")); origin != "" {
			if originURL, err := url.Parse(origin); err == nil {
				originHostname, originPort, splitErr := net.SplitHostPort(originURL.Host)
				if splitErr != nil {
					originHostname = originURL.Host
					originPort = ""
				}
				sameHost := isLoopbackHostname(originHostname) &&
					(originHostname == localHost || originHostname == requestHostname)
				if sameHost && (localPort == "" || originPort == localPort) {
					req.Header.Set("Origin", target.Scheme+"://"+target.Host)
				}
			}
		}
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		log.Warnf("amp thread actor proxy error for %s %s: %v", req.Method, req.URL.Path, err)
		http.Error(rw, "amp thread actor proxy upstream error", http.StatusBadGateway)
	}

	p := &ampThreadActorProxy{
		addr:       addr,
		configured: addr,
		upstream:   redactURL(parsed),
		rivetToken: rivetToken,
		hasToken:   rivetToken != "",
		conns:      make(map[net.Conn]struct{}),
		done:       make(chan struct{}),
	}

	server := &http.Server{
		Addr:    addr,
		Handler: proxy,
	}
	// Track connections so Shutdown can force-close hijacked WebSocket sessions,
	// which httputil.ReverseProxy detaches from server.Shutdown.
	server.ConnState = func(conn net.Conn, state http.ConnState) {
		p.mu.Lock()
		defer p.mu.Unlock()
		switch state {
		case http.StateNew, http.StateActive:
			p.conns[conn] = struct{}{}
		case http.StateHijacked:
		case http.StateClosed, http.StateIdle:
			delete(p.conns, conn)
		}
	}
	p.server = server

	return p, nil
}

func localListenerHostname(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if hostname, _, err := net.SplitHostPort(host); err == nil {
		return strings.ToLower(hostname)
	}
	return strings.ToLower(host)
}

func localListenerPort(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	if _, port, err := net.SplitHostPort(host); err == nil {
		return port
	}
	// A Host without an explicit port means the default port for the scheme;
	// this proxy is plain HTTP, so treat it as port 80 rather than a wildcard.
	return "80"
}

func validateLoopbackAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return fmt.Errorf("actor proxy address %q has no host; the local actor endpoint must bind a loopback address", addr)
	}
	if !isLoopbackHostname(host) {
		return fmt.Errorf("actor proxy address %q is not a loopback address; the local actor endpoint must not listen on non-loopback interfaces", addr)
	}
	return nil
}

func isLoopbackHostname(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func rivetTokenFromURL(u *url.URL) string {
	if u == nil || u.User == nil {
		return ""
	}
	password, _ := u.User.Password()
	return password
}

func isRivetGatewayPath(path string) bool {
	return path == "/gateway" || strings.HasPrefix(path, "/gateway/")
}

func addRivetGatewayToken(req *http.Request, token string) {
	if req == nil || req.URL == nil || token == "" || !isRivetGatewayPath(req.URL.Path) {
		return
	}

	q := req.URL.Query()
	if q.Get("rvt-token") != "" {
		return
	}
	q.Set("rvt-token", token)
	req.URL.RawQuery = q.Encode()
}

func addRivetWebSocketProtocolToken(req *http.Request, token string) {
	if req == nil || req.URL == nil || token == "" || !isRivetWebSocketRequest(req) || !isValidWebSocketProtocolToken(token) {
		return
	}

	protocols := splitWebSocketProtocols(req.Header.Get("Sec-WebSocket-Protocol"))
	if len(protocols) == 0 || !containsWebSocketProtocol(protocols, "rivet") || hasWebSocketProtocolPrefix(protocols, "rivet_token.") {
		return
	}

	protocols = append(protocols, "rivet_token."+token)
	req.Header.Set("Sec-WebSocket-Protocol", strings.Join(protocols, ", "))
}

func isRivetWebSocketRequest(req *http.Request) bool {
	if req == nil || req.URL == nil {
		return false
	}

	path := req.URL.Path
	if path != "/connect" && !isRivetGatewayPath(path) && path != "/websocket" && !strings.HasPrefix(path, "/websocket/") {
		return false
	}

	return strings.EqualFold(req.Header.Get("Upgrade"), "websocket")
}

func splitWebSocketProtocols(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	protocols := make([]string, 0, len(parts))
	for _, part := range parts {
		if protocol := strings.TrimSpace(part); protocol != "" {
			protocols = append(protocols, protocol)
		}
	}
	return protocols
}

func containsWebSocketProtocol(protocols []string, want string) bool {
	for _, protocol := range protocols {
		if protocol == want {
			return true
		}
	}
	return false
}

func hasWebSocketProtocolPrefix(protocols []string, prefix string) bool {
	for _, protocol := range protocols {
		if strings.HasPrefix(protocol, prefix) {
			return true
		}
	}
	return false
}

func isValidWebSocketProtocolToken(token string) bool {
	for _, r := range token {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case strings.ContainsRune("!#$%&'*+-.^_`|~", r):
		default:
			return false
		}
	}
	return token != ""
}

func (p *ampThreadActorProxy) Start(ctx context.Context) error {
	if p == nil || p.server == nil {
		return errors.New("proxy is nil")
	}

	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, stopRun := context.WithCancel(ctx)

	listener, err := net.Listen("tcp", p.addr)
	if err != nil {
		stopRun()
		return fmt.Errorf("listen %s: %w", p.addr, err)
	}

	p.mu.Lock()
	p.addr = listener.Addr().String()
	p.listener = listener
	p.runCtx = runCtx
	p.stopRun = stopRun
	p.mu.Unlock()

	go func() {
		<-runCtx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout(ctx))
		defer cancel()
		_ = p.Shutdown(shutdownCtx)
	}()

	go func() {
		defer close(p.done)
		if errServe := p.server.Serve(listener); errServe != nil && !errors.Is(errServe, http.ErrServerClosed) {
			log.Warnf("amp thread actor proxy stopped unexpectedly: %v", errServe)
		}
	}()

	return nil
}

func (p *ampThreadActorProxy) Shutdown(ctx context.Context) error {
	if p == nil || p.server == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		done := p.done
		p.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	p.closed = true
	if p.stopRun != nil {
		p.stopRun()
	}
	server := p.server
	done := p.done
	p.mu.Unlock()

	var shutdownErr error
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownDone <- server.Shutdown(shutdownCtx)
	}()

	p.mu.Lock()
	conns := make([]net.Conn, 0, len(p.conns))
	for conn := range p.conns {
		conns = append(conns, conn)
	}
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}

	if err := <-shutdownDone; err != nil {
		log.Warnf("amp thread actor proxy graceful shutdown: %v", err)
		shutdownErr = err
	}

	if done != nil {
		<-done
	}
	return shutdownErr
}

func shutdownTimeout(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 {
			return d
		}
	}
	return 5 * time.Second
}

func sanitizeURLParseError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	if i := strings.Index(msg, "@"); i >= 0 {
		if j := strings.LastIndex(msg[:i], "://"); j >= 0 {
			return msg[:j+3] + msg[i+1:]
		}
	}
	return msg
}

func redactURL(u *url.URL) string {
	if u == nil {
		return ""
	}
	clone := *u
	clone.User = nil
	return clone.String()
}
