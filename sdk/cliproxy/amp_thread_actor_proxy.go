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
	addr     string
	upstream string
	hasToken bool
	server   *http.Server
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

	localHost, localPort, err := net.SplitHostPort(addr)
	if err != nil {
		localHost = addr
		localPort = ""
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
		if origin := strings.TrimSpace(req.Header.Get("Origin")); origin != "" {
			if originURL, err := url.Parse(origin); err == nil {
				originHostname, originPort, splitErr := net.SplitHostPort(originURL.Host)
				if splitErr != nil {
					originHostname = originURL.Host
					originPort = ""
				}
				sameHost := originHostname == localHost ||
					(originHostname == "localhost" && localHost == "127.0.0.1") ||
					(originHostname == "127.0.0.1" && localHost == "localhost")
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

	server := &http.Server{
		Addr:    addr,
		Handler: proxy,
	}

	return &ampThreadActorProxy{
		addr:     addr,
		upstream: redactURL(parsed),
		hasToken: rivetToken != "",
		server:   server,
	}, nil
}

func rivetTokenFromURL(u *url.URL) string {
	if u == nil || u.User == nil {
		return ""
	}
	password, _ := u.User.Password()
	return password
}

func addRivetGatewayToken(req *http.Request, token string) {
	if req == nil || req.URL == nil || token == "" || !strings.HasPrefix(req.URL.Path, "/gateway/") {
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
	if path != "/connect" && !strings.HasPrefix(path, "/gateway/") && path != "/websocket" && !strings.HasPrefix(path, "/websocket/") {
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

	listener, err := net.Listen("tcp", p.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.addr, err)
	}
	p.addr = listener.Addr().String()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout(ctx))
		defer cancel()
		_ = p.Shutdown(shutdownCtx)
	}()

	go func() {
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
	return p.server.Shutdown(ctx)
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
