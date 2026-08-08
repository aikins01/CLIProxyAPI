package amp

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// neoOrbPortalTargetURL builds the in-container target for a portal request.
// Overridable in tests.
var neoOrbPortalTargetURL = func(containerIP string, port int) (string, error) {
	ip := strings.TrimSpace(containerIP)
	if ip == "" {
		return "", fmt.Errorf("container has no network address")
	}
	if net.ParseIP(ip) == nil {
		return "", fmt.Errorf("container address %q is not a valid IP", ip)
	}
	return "http://" + net.JoinHostPort(ip, strconv.Itoa(port)), nil
}

// serveOrbPortal reverse-proxies /orb/<thread>/p/<port>/<path...> to a
// process listening on that port inside the thread's orb container. HTTP and
// WebSocket upgrades are supported. Access is gated by the same auth and
// localhost middleware as the local project endpoints, and the thread must
// own a live orb on this server.
func (m *AmpModule) serveOrbPortal(c *gin.Context) {
	threadID := strings.TrimSpace(c.Param("threadID"))
	if !neoThreadIDExactPattern.MatchString(threadID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	port, err := strconv.Atoi(strings.TrimSpace(c.Param("port")))
	if err != nil || port < 1 || port > 65535 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid portal port"})
		return
	}
	if m == nil || m.neoRuntime == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	cfg := m.neoThreadConfigSnapshot()
	if !neoOrbsEnabled(cfg) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	manager := m.neoRuntime.orbManagerFor()
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	ownerUserID := m.neoRuntime.neoRequestOwnerUserID(c)
	if !neoRequestOwnerScopeResolved(c, ownerUserID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	if ownerUserID == "" {
		ownerUserID = neoLocalOwnerUserID
	}
	if actor == nil || !actor.ownedByUser(ownerUserID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	record, ok := manager.snapshot(threadID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "no orb for thread"})
		return
	}
	manager.markOrbActivity(threadID)
	if record.state == neoOrbStatePaused {
		c.JSON(http.StatusConflict, gin.H{"error": "orb is paused", "state": record.state})
		return
	}
	if record.state != neoOrbStateRunning || record.containerID == "" {
		c.JSON(http.StatusConflict, gin.H{"error": "orb is not ready", "state": record.state})
		return
	}
	inspectCtx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	ip, err := manager.portalAddress(inspectCtx, cfg, threadID)
	cancel()
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	target, err := neoOrbPortalTargetURL(ip, port)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	targetURL, err := url.Parse(target)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "invalid portal target"})
		return
	}

	remainder := c.Param("path")
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.URL.Path = remainder
			req.URL.RawPath = ""
			req.Host = targetURL.Host
			req.Header.Del("Authorization")
			req.Header.Del("Cookie")
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, proxyErr error) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"orb portal upstream unavailable"}`))
		},
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}
