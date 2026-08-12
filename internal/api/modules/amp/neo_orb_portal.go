package amp

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

const (
	neoOrbPortalTokenQuery     = "cliproxy-orb-token"
	neoOrbPortalCookieName     = "cliproxy-orb-portal"
	neoOrbPortalTokenLabel     = "cliproxy.portal-token"
	neoOrbPortalHelperPath     = "/usr/local/bin/amp-orb-portal"
	neoOrbServiceHelperPath    = "/usr/local/bin/amp-orb-service"
	neoOrbPortalAuthenticated  = "cliproxyOrbPortalAuthenticated"
	neoOrbPortalPrincipal      = "cliproxy-orb-portal"
	neoOrbPortalTokenByteCount = 48
)

type neoOrbPortalOwnerContextKey struct{}

func neoOrbNewPortalToken() string {
	return randomBase62(neoOrbPortalTokenByteCount)
}

func neoOrbPortalTokenValid(token string) bool {
	if len(token) != neoOrbPortalTokenByteCount {
		return false
	}
	for _, character := range []byte(token) {
		if character >= '0' && character <= '9' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' {
			continue
		}
		return false
	}
	return true
}

func (m *AmpModule) orbPortalTokenMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request == nil || c.Request.URL == nil {
			c.Next()
			return
		}
		threadID := strings.TrimSpace(c.Param("threadID"))
		port, err := strconv.Atoi(strings.TrimSpace(c.Param("port")))
		query := c.Request.URL.Query()
		presented := strings.TrimSpace(query.Get(neoOrbPortalTokenQuery))
		fromQuery := presented != ""
		hasAuthorization := strings.TrimSpace(c.GetHeader("Authorization")) != ""
		if presented == "" && !hasAuthorization {
			if cookie, cookieErr := c.Request.Cookie(neoOrbPortalCookieName); cookieErr == nil {
				presented = strings.TrimSpace(cookie.Value)
			}
		}
		if _, exists := query[neoOrbPortalTokenQuery]; exists {
			query.Del(neoOrbPortalTokenQuery)
			c.Request.URL.RawQuery = query.Encode()
		}
		if hasAuthorization {
			c.Next()
			return
		}
		authenticated := false
		if err == nil && neoThreadIDExactPattern.MatchString(threadID) && port >= 1 && port <= 65535 && neoOrbPortalTokenValid(presented) && m != nil && m.neoRuntime != nil {
			cfg := m.neoThreadConfigSnapshot()
			manager := m.neoRuntime.orbManagerFor()
			if manager.ensureRecovered(cfg) == nil {
				if record, ok := manager.snapshot(threadID); ok && hmac.Equal([]byte(presented), []byte(record.portalToken)) {
					if actor, release := m.neoRuntime.store.retainThreadActorWithoutReadyWork(threadID); actor != nil {
						defer release()
						ownerUserID := actor.threadToolOwnerID()
						ctx := context.WithValue(c.Request.Context(), neoOrbPortalOwnerContextKey{}, ownerUserID)
						c.Request = c.Request.WithContext(ctx)
						c.Set(neoOrbPortalAuthenticated, true)
						c.Set("userApiKey", neoOrbPortalPrincipal)
						authenticated = true
					}
				}
			}
		}
		if presented != "" && !authenticated {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid portal capability"})
			return
		}
		if authenticated && fromQuery {
			secure := c.Request.TLS != nil || strings.EqualFold(strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")), "https")
			http.SetCookie(c.Writer, &http.Cookie{
				Name:     neoOrbPortalCookieName,
				Value:    presented,
				Path:     fmt.Sprintf("/orb/%s/p/%d/", threadID, port),
				HttpOnly: true,
				Secure:   secure,
				SameSite: http.SameSiteLaxMode,
			})
			if c.Request.Method == http.MethodGet || c.Request.Method == http.MethodHead {
				location := c.Request.URL.Path
				if c.Request.URL.RawQuery != "" {
					location += "?" + c.Request.URL.RawQuery
				}
				c.Redirect(http.StatusFound, location)
				c.Abort()
				return
			}
		}
		c.Next()
	}
}

func neoOrbPortalHelperScript() string {
	return `#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import re
import urllib.parse

parser = argparse.ArgumentParser(prog="amp-orb-portal", description="Expose an HTTP server in this CLIProxyAPI orb")
parser.add_argument("target", help="local port or loopback URL")
parser.add_argument("--name", default="")
parser.add_argument("--title", default="")
parser.add_argument("--description", default="")
parser.add_argument("--no-manifest", action="store_true", help=argparse.SUPPRESS)
args = parser.parse_args()

thread_id = os.environ.get("AMP_THREAD_ID", "").strip()
base_url = os.environ.get("AMP_ORB_PORTAL_BASE_URL", "").strip().rstrip("/")
token = os.environ.get("AMP_ORB_PORTAL_TOKEN", "").strip()
if not thread_id or not base_url or not token:
    parser.error("AMP_THREAD_ID, AMP_ORB_PORTAL_BASE_URL, and AMP_ORB_PORTAL_TOKEN must be set")

target = args.target.strip()
if target.isdigit():
    port = int(target)
    suffix = "/"
else:
    parsed = urllib.parse.urlsplit(target if "://" in target else "http://" + target)
    if parsed.scheme.lower() != "http":
        parser.error("target URL scheme must be http")
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        parser.error("target URL must use localhost or a loopback address")
    port = parsed.port or 80
    suffix = parsed.path or "/"
    if parsed.query:
        suffix += "?" + parsed.query
if port < 1 or port > 65535:
    parser.error("port must be between 1 and 65535")

portal = f"{base_url}/orb/{urllib.parse.quote(thread_id, safe='')}/p/{port}{suffix}"
separator = "&" if "?" in portal else "?"
portal += separator + urllib.parse.urlencode({"` + neoOrbPortalTokenQuery + `": token})
name = args.name.strip() or f"port-{port}"
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", name):
    parser.error("name must contain only letters, numbers, dot, underscore, or hyphen")
title = args.title.strip() or name
link = {"label": title, "url": portal}
if args.description.strip():
    link["note"] = args.description.strip()
if not args.no_manifest:
    manifest_dir = pathlib.Path.cwd() / ".amp" / "portals"
    manifest_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(manifest_dir, 0o700)
    manifest_path = manifest_dir / f"{name}.json"
    manifest_path.write_text(json.dumps({"links": [link]}, indent=2) + "\n")
    os.chmod(manifest_path, 0o600)
    print(f"Wrote portal manifest {manifest_path}.")
print(portal)
`
}

func neoOrbServiceHelperScript() string {
	return `#!/usr/bin/env python3
import argparse
import fcntl
import json
import os
import pathlib
import re
import shlex
import socket
import subprocess
import sys
import tempfile
import time

root = pathlib.Path.home() / ".cache" / "amp" / "services"
conf_dir = root / "conf.d"
command_dir = root / "commands"
state_path = root / "state.json"
supervisor_conf = root / "supervisord.conf"

def run_ctl(*args, check=True):
    return subprocess.run(["supervisorctl", "-c", str(supervisor_conf), *args], text=True, capture_output=True, check=check)

def atomic_write(path, text, mode):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix="." + path.name + "-", dir=path.parent, text=True)
    try:
        with os.fdopen(descriptor, "w") as output:
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass

def ensure_supervisor():
    conf_dir.mkdir(parents=True, exist_ok=True)
    command_dir.mkdir(parents=True, exist_ok=True)
    atomic_write(supervisor_conf, f"""[unix_http_server]
file={root}/supervisor.sock
chmod=0600

[supervisord]
logfile={root}/supervisord.log
pidfile={root}/supervisord.pid
childlogdir={root}

[rpcinterface:supervisor]
supervisor.rpcinterface_factory=supervisor.rpcinterface:make_main_rpcinterface

[supervisorctl]
serverurl=unix://{root}/supervisor.sock

[include]
files={conf_dir}/*.conf
""", 0o600)
    if run_ctl("pid", check=False).returncode == 0:
        return
    for stale in (root / "supervisor.sock", root / "supervisord.pid"):
        try:
            stale.unlink()
        except FileNotFoundError:
            pass
    subprocess.run(["supervisord", "-c", str(supervisor_conf)], check=True)
    for _ in range(50):
        if run_ctl("pid", check=False).returncode == 0:
            return
        time.sleep(0.1)
    raise SystemExit("supervisord did not become ready")

def valid_name(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", value):
        raise SystemExit("service name must contain only letters, numbers, dot, underscore, or hyphen")
    return value

def load_state():
    try:
        value = json.loads(state_path.read_text())
        return value if isinstance(value, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_state(value):
    atomic_write(state_path, json.dumps(value, indent=2, sort_keys=True) + "\n", 0o600)

def available_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]

def supervisor_value(value):
    return str(value).replace("%", "%%")

def environment_value(value):
    return supervisor_value(value).replace("\\", "\\\\").replace('"', '\\"')

def service_state(result, name):
    fields = result.stdout.strip().split()
    if len(fields) >= 2 and fields[0] == name:
        return fields[1]
    return ""

parser = argparse.ArgumentParser(prog="amp-orb-service")
subparsers = parser.add_subparsers(dest="action", required=True)
start = subparsers.add_parser("start")
start.add_argument("name")
start.add_argument("--command", required=True)
start.add_argument("--port", type=int)
start.add_argument("--portal", action="store_true")
start.add_argument("--title", default="")
start.add_argument("--description", default="")
for action in ("restart", "stop", "status", "logs"):
    child = subparsers.add_parser(action)
    child.add_argument("name")
    if action == "logs":
        child.add_argument("--lines", type=int, default=200)
subparsers.add_parser("list")
args = parser.parse_args()

root.mkdir(parents=True, exist_ok=True)
mutation_lock = open(root / "state.lock", "a+")
os.chmod(root / "state.lock", 0o600)
fcntl.flock(mutation_lock.fileno(), fcntl.LOCK_EX)
ensure_supervisor()
if args.action == "list":
    result = run_ctl("status", check=False)
    sys.stdout.write(result.stdout or result.stderr)
    raise SystemExit(result.returncode)

name = valid_name(args.name)
conf_path = conf_dir / f"{name}.conf"
command_path = command_dir / name
log_path = root / f"{name}.log"
if args.action == "start":
    state = load_state()
    previous = state.get(name, {}) if isinstance(state.get(name), dict) else {}
    previous_conf = conf_path.read_text() if conf_path.exists() else None
    previous_command = command_path.read_text() if command_path.exists() else None
    port = args.port or previous.get("port") or available_port()
    if not isinstance(port, int) or port < 1 or port > 65535:
        raise SystemExit("port must be between 1 and 65535")
    manifest_path = pathlib.Path.cwd() / ".amp" / "portals" / f"{name}.json"
    stale_manifests = {str(manifest_path)}
    if isinstance(previous.get("manifest"), str) and previous.get("manifest"):
        stale_manifests.add(previous["manifest"])
    previous_manifests = {}
    for stale_manifest in stale_manifests:
        stale_path = pathlib.Path(stale_manifest)
        try:
            previous_manifests[stale_manifest] = stale_path.read_text()
        except FileNotFoundError:
            pass
        try:
            stale_path.unlink()
        except FileNotFoundError:
            pass
    public_url = ""
    if args.portal:
        try:
            portal = subprocess.run([
                "amp-orb-portal", str(port), "--name", name,
                "--title", args.title or name, "--description", args.description, "--no-manifest",
            ], text=True, capture_output=True, check=True)
        except (subprocess.CalledProcessError, OSError):
            for manifest, content in previous_manifests.items():
                atomic_write(pathlib.Path(manifest), content, 0o600)
            raise
        public_url = portal.stdout.strip().splitlines()[-1]
    atomic_write(command_path, "#!/bin/sh\nexec /bin/sh -lc " + shlex.quote(args.command) + "\n", 0o700)
    command = supervisor_value(str(command_path))
    directory = supervisor_value(str(pathlib.Path.cwd()))
    environment = ",".join([
        f'PORT="{port}"',
        f'PUBLIC_URL="{environment_value(public_url)}"',
        'AMP_ORB="1"',
        f'HOME="{environment_value(os.environ.get("HOME", "/home/user"))}"',
        f'PATH="{environment_value(os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"))}"',
    ])
    atomic_write(conf_path, f"""[program:{name}]
command={command}
directory={directory}
environment={environment}
autostart=true
autorestart=true
startsecs=1
startretries=20
stopasgroup=true
killasgroup=true
redirect_stderr=true
stdout_logfile={log_path}
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=3
""", 0o600)
    run_ctl("reread")
    run_ctl("update")
    if previous:
        run_ctl("restart", name, check=False)
    started = False
    for _ in range(100):
        status = run_ctl("status", name, check=False)
        state_name = service_state(status, name)
        if state_name == "RUNNING":
            if args.portal:
                try:
                    portal = subprocess.run([
                        "amp-orb-portal", str(port), "--name", name,
                        "--title", args.title or name, "--description", args.description,
                    ], text=True, capture_output=True, check=True)
                except (subprocess.CalledProcessError, OSError):
                    run_ctl("stop", name, check=False)
                    if previous_command is None:
                        command_path.unlink(missing_ok=True)
                    else:
                        atomic_write(command_path, previous_command, 0o700)
                    if previous_conf is None:
                        conf_path.unlink(missing_ok=True)
                    else:
                        atomic_write(conf_path, previous_conf, 0o600)
                    for stale_manifest in stale_manifests:
                        pathlib.Path(stale_manifest).unlink(missing_ok=True)
                    for manifest, content in previous_manifests.items():
                        atomic_write(pathlib.Path(manifest), content, 0o600)
                    run_ctl("reread", check=False)
                    run_ctl("update", check=False)
                    if previous_conf is not None:
                        run_ctl("start", name, check=False)
                    raise
                sys.stdout.write(portal.stdout)
            state[name] = {
                "port": port,
                "command": args.command,
                "portal": public_url,
                "manifest": str(manifest_path) if args.portal else "",
            }
            save_state(state)
            sys.stdout.write(status.stdout)
            raise SystemExit(0)
        if state_name == "STOPPED" and not started:
            run_ctl("start", name, check=False)
            started = True
        if state_name in {"BACKOFF", "EXITED", "FATAL", "UNKNOWN"}:
            sys.stdout.write(status.stdout or status.stderr)
            raise SystemExit(1)
        time.sleep(0.1)
    sys.stdout.write(status.stdout or status.stderr)
    raise SystemExit(1)

if not conf_path.exists():
    raise SystemExit(f"unknown service: {name}")
if args.action == "logs":
    if args.lines < 0:
        raise SystemExit("lines must not be negative")
    try:
        lines = log_path.read_text(errors="replace").splitlines()
    except FileNotFoundError:
        lines = []
    if args.lines > 0:
        sys.stdout.write("\n".join(lines[-args.lines:]) + ("\n" if lines else ""))
    raise SystemExit(0)
result = run_ctl(args.action, name, check=False)
sys.stdout.write(result.stdout or result.stderr)
raise SystemExit(result.returncode)
`
}

func (m *neoOrbManager) orbConfigurePortalHelper(ctx context.Context, client neoOrbProviderClient, containerID string) error {
	if err := client.CopyFileToContainer(ctx, containerID, neoOrbPortalHelperPath, []byte(neoOrbPortalHelperScript()), 0o755); err != nil {
		return err
	}
	return client.CopyFileToContainer(ctx, containerID, neoOrbServiceHelperPath, []byte(neoOrbServiceHelperScript()), 0o755)
}

func neoOrbPortalAppCookiePrefix(threadID string, port int) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\n%d", threadID, port)))
	return "__cliproxy_orb_" + hex.EncodeToString(digest[:8]) + "_"
}

func neoRewriteOrbPortalRequestCookies(req *http.Request, prefix string) {
	cookies := req.Cookies()
	req.Header.Del("Cookie")
	for _, cookie := range cookies {
		if strings.HasPrefix(cookie.Name, prefix) && len(cookie.Name) > len(prefix) {
			cookie.Name = strings.TrimPrefix(cookie.Name, prefix)
			req.AddCookie(cookie)
		}
	}
}

func neoRewriteOrbPortalResponseCookies(response *http.Response, prefix, pathPrefix string) error {
	if response == nil || len(response.Header.Values("Set-Cookie")) == 0 {
		return nil
	}
	cookies := response.Cookies()
	response.Header.Del("Set-Cookie")
	for _, cookie := range cookies {
		if cookie.Name == "" {
			continue
		}
		cookie.Name = prefix + cookie.Name
		cookie.Domain = ""
		path := strings.TrimPrefix(cookie.Path, "/")
		cookie.Path = pathPrefix
		if path != "" {
			cookie.Path += path
		}
		response.Header.Add("Set-Cookie", cookie.String())
	}
	return nil
}

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
	ownerUserID := m.neoRuntime.neoRequestOwnerUserID(c.Request.Context())
	if !neoRequestOwnerScopeResolved(c.Request.Context(), ownerUserID) {
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
	if err := manager.ensureRecovered(cfg); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "orb recovery failed: " + err.Error()})
		return
	}
	record, ok := manager.snapshot(threadID)
	if !ok || record.containerID == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "no orb for thread"})
		return
	}
	if record.state != neoOrbStateRunning && record.state != neoOrbStatePaused {
		body := gin.H{"error": "orb is not ready", "state": record.state}
		if record.state == neoOrbStateConflict {
			if reason := strings.TrimSpace(record.failReason); reason != "" {
				body["error"] = "orb is not ready: " + reason
				body["recoveryBlocker"] = reason
			}
		}
		c.JSON(http.StatusConflict, body)
		return
	}
	wakeCtx, wakeCancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	record, releasePortal, err := manager.acquirePortal(wakeCtx, cfg, threadID)
	wakeCancel()
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	defer releasePortal()
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
	pathPrefix := fmt.Sprintf("/orb/%s/p/%d/", threadID, port)
	cookiePrefix := neoOrbPortalAppCookiePrefix(threadID, port)
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = targetURL.Scheme
			req.URL.Host = targetURL.Host
			req.URL.Path = remainder
			req.URL.RawPath = ""
			req.Host = targetURL.Host
			req.Header.Del("Authorization")
			neoRewriteOrbPortalRequestCookies(req, cookiePrefix)
		},
		ModifyResponse: func(response *http.Response) error {
			return neoRewriteOrbPortalResponseCookies(response, cookiePrefix, pathPrefix)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, proxyErr error) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":"orb portal upstream unavailable"}`))
		},
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}
