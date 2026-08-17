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
						admitted := !record.recovered
						if admitted && record.lifecycleV1 {
							operation, admissionErr := manager.beginLifecycleAdmission(c.Request.Context(), actor, threadID, neoOrbStateRunning, neoOrbStatePaused)
							admitted = admissionErr == nil
							if operation != nil {
								operation.close()
							}
						}
						if admitted {
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
		}
		if presented != "" && !authenticated {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid portal capability"})
			return
		}
		if authenticated && fromQuery {
			secure := c.Request.TLS != nil || strings.EqualFold(strings.TrimSpace(c.GetHeader("X-Forwarded-Proto")), "https")
			sameSite := http.SameSiteLaxMode
			if secure {
				sameSite = http.SameSiteNoneMode
			}
			http.SetCookie(c.Writer, &http.Cookie{
				Name:        neoOrbPortalCookieName,
				Value:       presented,
				Path:        fmt.Sprintf("/orb/%s/p/%d/", threadID, port),
				HttpOnly:    true,
				Secure:      secure,
				SameSite:    sameSite,
				Partitioned: secure,
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
import http.client
import json
import os
import pathlib
import re
import socket
import tempfile
import time
import urllib.parse

parser = argparse.ArgumentParser(prog="amp-orb-portal", description="Expose an HTTP server in this CLIProxyAPI orb")
parser.add_argument("target", help="local port or loopback URL")
parser.add_argument("--name", default="")
parser.add_argument("--title", default="")
parser.add_argument("--description", default="")
parser.add_argument("--health", default="", metavar="PATH", help="wait up to 10 seconds for a 2xx/3xx response from an absolute HTTP path before publishing")
parser.add_argument("--no-manifest", action="store_true", help=argparse.SUPPRESS)
args = parser.parse_args()

thread_id = os.environ.get("AMP_THREAD_ID", "").strip()
base_url = os.environ.get("AMP_ORB_PORTAL_BASE_URL", "").strip().rstrip("/")
token = os.environ.get("AMP_ORB_PORTAL_TOKEN", "").strip()
if not thread_id or not base_url or not token:
    parser.error("AMP_THREAD_ID, AMP_ORB_PORTAL_BASE_URL, and AMP_ORB_PORTAL_TOKEN must be set")

target = args.target.strip()
host = "127.0.0.1"
if target.isdigit():
    port = int(target)
    suffix = "/"
else:
    parsed = urllib.parse.urlsplit(target if "://" in target else "http://" + target)
    if parsed.scheme.lower() != "http":
        parser.error("target URL scheme must be http")
    if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        parser.error("target URL must use localhost or a loopback address")
    host = parsed.hostname
    port = parsed.port or 80
    suffix = parsed.path or "/"
    if parsed.query:
        suffix += "?" + parsed.query
if port < 1 or port > 65535:
    parser.error("port must be between 1 and 65535")
health = args.health.strip()
health_url = urllib.parse.urlsplit(health)
if health and (not health.startswith("/") or health_url.scheme or health_url.netloc or health_url.fragment or not all(0x21 <= ord(character) < 0x7f for character in health)):
    parser.error("health must be an absolute HTTP path")

durable_root = pathlib.Path("/home/user").resolve()

def durable_directory(path):
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(durable_root)
    except ValueError:
        parser.error(f"portal directory must be under {durable_root}")
    parts = relative.parts
    if any(parts[index] == ".amp" and parts[index + 1] == "out" for index in range(len(parts) - 1)):
        parser.error(".amp/out cannot be used as durable portal storage")
    return resolved

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

def ready():
    try:
        if not health:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        connection = http.client.HTTPConnection(host, port, timeout=0.5)
        try:
            connection.request("GET", health)
            response = connection.getresponse()
            return 200 <= response.status < 400
        finally:
            connection.close()
    except (OSError, http.client.HTTPException):
        return False

deadline = time.monotonic() + 10
while not ready():
    if time.monotonic() >= deadline:
        health_detail = f" with health path {health}" if health else ""
        raise SystemExit(f"local service on port {port}{health_detail} did not become ready")
    time.sleep(0.1)

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
portal_directory = durable_directory(pathlib.Path.cwd())
if not args.no_manifest:
    manifest_dir = portal_directory / ".amp" / "portals"
    manifest_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(manifest_dir, 0o700)
    manifest_path = manifest_dir / f"{name}.json"
    atomic_write(manifest_path, json.dumps({"links": [link]}, indent=2) + "\n", 0o600)
    print(f"Wrote portal manifest {manifest_path}.")
print(portal)
`
}

func neoOrbServiceHelperScript() string {
	return `#!/usr/bin/env python3
import argparse
import fcntl
import http.client
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
import urllib.parse

durable_root = pathlib.Path("/home/user").resolve()
root = durable_root / ".cache" / "amp" / "services"
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

def load_state(strict=False):
    try:
        value = json.loads(state_path.read_text())
        if isinstance(value, dict):
            return value
        if strict:
            raise SystemExit("service state must contain a JSON object")
        return {}
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as error:
        if strict:
            raise SystemExit(f"service state is invalid JSON: {error}")
        return {}

def save_state(value):
    atomic_write(state_path, json.dumps(value, indent=2, sort_keys=True) + "\n", 0o600)

def durable_directory(path):
    resolved = path.resolve()
    try:
        relative = resolved.relative_to(durable_root)
    except ValueError:
        raise ValueError(f"service directory must be under {durable_root}")
    parts = relative.parts
    if any(parts[index] == ".amp" and parts[index + 1] == "out" for index in range(len(parts) - 1)):
        raise ValueError(".amp/out cannot be used as durable service storage")
    return resolved

def valid_health(value):
    parsed = urllib.parse.urlsplit(value)
    return not value or value.startswith("/") and not parsed.scheme and not parsed.netloc and not parsed.fragment and all(0x21 <= ord(character) < 0x7f for character in value)

def portal_url(port):
    thread_id = os.environ.get("AMP_THREAD_ID", "").strip()
    base_url = os.environ.get("AMP_ORB_PORTAL_BASE_URL", "").strip().rstrip("/")
    token = os.environ.get("AMP_ORB_PORTAL_TOKEN", "").strip()
    if not thread_id or not base_url or not token:
        raise SystemExit("AMP_THREAD_ID, AMP_ORB_PORTAL_BASE_URL, and AMP_ORB_PORTAL_TOKEN must be set")
    public_url = f"{base_url}/orb/{urllib.parse.quote(thread_id, safe='')}/p/{port}/"
    return public_url + "?" + urllib.parse.urlencode({"` + neoOrbPortalTokenQuery + `": token})

def remove_manifest_path(path):
    path.unlink(missing_ok=True)

def service_manifest_path(name, path):
    try:
        resolved = pathlib.Path(path).resolve()
        resolved.relative_to(durable_root)
    except (OSError, TypeError, ValueError):
        return None
    if resolved.name != f"{name}.json" or resolved.parent.name != "portals" or resolved.parent.parent.name != ".amp":
        return None
    return resolved

def hide_service_manifest(name, service):
    if not isinstance(service, dict):
        return True
    candidates = []
    manifest = service.get("manifest")
    directory = service.get("directory")
    if isinstance(manifest, str) and manifest:
        candidates.append(pathlib.Path(manifest))
    if isinstance(directory, str) and directory:
        candidates.append(pathlib.Path(directory) / ".amp" / "portals" / f"{name}.json")
    removed = True
    for candidate in candidates:
        resolved = service_manifest_path(name, candidate)
        if resolved is None:
            continue
        try:
            remove_manifest_path(resolved)
        except OSError:
            removed = False
    return removed

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

def service_ready(port, health=""):
    try:
        if not health:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=0.5)
        try:
            connection.request("GET", health)
            response = connection.getresponse()
            return 200 <= response.status < 400
        finally:
            connection.close()
    except (OSError, http.client.HTTPException):
        return False

def wait_ready(port, health="", deadline=None):
    if deadline is None:
        deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if service_ready(port, health):
            return True
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.1, remaining))
    return False

parser = argparse.ArgumentParser(prog="amp-orb-service")
subparsers = parser.add_subparsers(dest="action", required=True)
start = subparsers.add_parser("start")
start.add_argument("name")
start.add_argument("--command", required=True)
start.add_argument("--port", type=int)
start.add_argument("--portal", action="store_true")
start.add_argument("--title", default="")
start.add_argument("--description", default="")
start.add_argument("--health", default="", metavar="PATH", help="wait up to 10 seconds for a 2xx/3xx response from an absolute HTTP path before publishing")
for action in ("restart", "stop", "status", "logs"):
    child = subparsers.add_parser(action)
    child.add_argument("name")
    if action == "logs":
        child.add_argument("--lines", type=int, default=200)
subparsers.add_parser("list")
subparsers.add_parser("reconcile", help=argparse.SUPPRESS)
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
if args.action == "reconcile":
    state = load_state(strict=True)
    run_ctl("reread", check=False)
    run_ctl("update", check=False)
    reconcile_deadline = time.monotonic() + 10
    for name in sorted(list(state)):
        service = state[name]
        has_portal_manifest = isinstance(service, dict) and bool(service.get("portal") or service.get("manifest"))
        try:
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", name) or not isinstance(service, dict):
                raise ValueError("invalid service record")
            port = service.get("port")
            command = service.get("command")
            portal_url = service.get("portal", "")
            manifest = service.get("manifest", "")
            title = service.get("title", name)
            description = service.get("description", "")
            health = service.get("health", "")
            if type(port) is not int or port < 1 or port > 65535:
                raise ValueError("invalid port")
            if not isinstance(command, str) or not command or not isinstance(portal_url, str) or not isinstance(manifest, str) or not isinstance(title, str) or not isinstance(description, str) or not isinstance(health, str) or not valid_health(health):
                raise ValueError("invalid service fields")
            conf_path = conf_dir / f"{name}.conf"
            command_path = command_dir / name
            if not conf_path.is_file() or not command_path.is_file():
                raise ValueError("missing supervisor files")
            directory = service.get("directory", "")
            if not isinstance(directory, str):
                raise ValueError("invalid directory")
            if not directory and manifest:
                directory = str(pathlib.Path(manifest).parent.parent.parent)
            if not directory:
                for line in conf_path.read_text().splitlines():
                    if line.startswith("directory="):
                        directory = line.removeprefix("directory=").replace("%%", "%")
                        break
            directory_path = durable_directory(pathlib.Path(directory))
            if not directory_path.is_dir():
                raise ValueError("missing service directory")
        except (OSError, TypeError, ValueError) as error:
            if not hide_service_manifest(name, service):
                print(f"Service {name} has an invalid persisted registration ({error}); its portal manifest could not be removed.", file=sys.stderr)
                continue
            state.pop(name, None)
            if has_portal_manifest:
                print(f"Service {name} has an invalid persisted registration ({error}); its portal manifest was removed.", file=sys.stderr)
            else:
                print(f"Service {name} has an invalid persisted registration ({error}); its registration was removed.", file=sys.stderr)
            continue
        expected_manifest = directory_path / ".amp" / "portals" / f"{name}.json"
        if portal_url and pathlib.Path(manifest).resolve() != expected_manifest:
            if not hide_service_manifest(name, service):
                print(f"Service {name} has an invalid persisted registration (unexpected portal manifest path); its portal manifest could not be removed.", file=sys.stderr)
                continue
            state.pop(name, None)
            print(f"Service {name} has an invalid persisted registration (unexpected portal manifest path); its portal manifest was removed.", file=sys.stderr)
            continue
        status = run_ctl("status", name, check=False)
        state_name = service_state(status, name)
        running = state_name == "RUNNING"
        if not running:
            run_ctl("start", name, check=False)
            while time.monotonic() < reconcile_deadline:
                status = run_ctl("status", name, check=False)
                state_name = service_state(status, name)
                if state_name == "RUNNING":
                    running = True
                    break
                if state_name in {"BACKOFF", "EXITED", "FATAL", "UNKNOWN"}:
                    break
                remaining = reconcile_deadline - time.monotonic()
                if remaining > 0:
                    time.sleep(min(0.1, remaining))
        if not running:
            manifest_removed = not portal_url or hide_service_manifest(name, service)
            if manifest_removed:
                print(f"Service {name} is not running (supervisor state {state_name or 'unknown'}); its portal manifest was not published.", file=sys.stderr)
            else:
                print(f"Service {name} is not running (supervisor state {state_name or 'unknown'}); its portal manifest could not be removed.", file=sys.stderr)
            continue
        if not wait_ready(port, health, reconcile_deadline):
            manifest_removed = not portal_url or hide_service_manifest(name, service)
            health_detail = f" with health path {health}" if health else ""
            if manifest_removed:
                print(f"Service {name} did not become ready on port {port}{health_detail}; its portal manifest was not published.", file=sys.stderr)
            else:
                print(f"Service {name} did not become ready on port {port}{health_detail}; its portal manifest could not be removed.", file=sys.stderr)
            continue
        if portal_url:
            portal_command = [
                "amp-orb-portal", str(port), "--name", name,
                "--title", title or name, "--description", description,
            ]
            portal = subprocess.run(portal_command, text=True, capture_output=True, check=False, cwd=directory_path)
            if portal.returncode != 0:
                manifest_removed = hide_service_manifest(name, service)
                detail = portal.stderr.strip()
                detail_suffix = f": {detail}" if detail else ""
                if manifest_removed:
                    print(f"Service {name} portal registration could not be restored (exit {portal.returncode}){detail_suffix}.", file=sys.stderr)
                else:
                    print(f"Service {name} portal registration could not be restored (exit {portal.returncode}) and its stale manifest could not be removed{detail_suffix}.", file=sys.stderr)
                continue
            portal_lines = portal.stdout.strip().splitlines()
            if portal_lines:
                service["portal"] = portal_lines[-1]
        service["directory"] = str(directory_path)
        service["title"] = title or name
        service["description"] = description
        service["health"] = health
    save_state(state)
    raise SystemExit(0)

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
    if type(port) is not int or port < 1 or port > 65535:
        raise SystemExit("port must be between 1 and 65535")
    health = args.health.strip()
    if not valid_health(health):
        raise SystemExit("health must be an absolute HTTP path")
    try:
        service_directory = durable_directory(pathlib.Path.cwd())
    except ValueError as error:
        raise SystemExit(str(error))
    title = args.title.strip() or name
    description = args.description.strip()
    public_url = ""
    if args.portal:
        public_url = portal_url(port)
    manifest_path = service_directory / ".amp" / "portals" / f"{name}.json"
    manifest_path = service_manifest_path(name, manifest_path)
    if manifest_path is None:
        raise SystemExit("service manifest path must remain under the durable root")
    stale_manifests = {str(manifest_path)}
    previous_manifest = service_manifest_path(name, previous.get("manifest"))
    if previous_manifest is not None:
        stale_manifests.add(str(previous_manifest))
    registration = {
        "port": port,
        "command": args.command,
        "portal": public_url,
        "manifest": str(manifest_path) if args.portal else "",
        "directory": str(service_directory),
        "title": title,
        "description": description,
        "health": health,
    }
    registration_changed = any(previous.get(key) != value for key, value in registration.items())
    expected_link = {"label": title, "url": public_url}
    if description:
        expected_link["note"] = description
    expected_manifest_text = json.dumps({"links": [expected_link]}, indent=2) + "\n"
    try:
        portal_manifest_current = args.portal and not registration_changed and manifest_path.read_text() == expected_manifest_text and manifest_path.stat().st_mode & 0o777 == 0o600
    except OSError:
        portal_manifest_current = False
    manifests_to_replace = set() if portal_manifest_current else stale_manifests
    previous_manifests = {}
    for stale_manifest in manifests_to_replace:
        stale_path = pathlib.Path(stale_manifest)
        try:
            previous_manifests[stale_manifest] = (stale_path.read_text(), stale_path.stat().st_mode & 0o777)
        except FileNotFoundError:
            pass

    def rollback():
        rollback_errors = []
        try:
            run_ctl("stop", name, check=False)
        except (OSError, subprocess.CalledProcessError) as error:
            rollback_errors.append(error)
        try:
            if previous_command is None:
                command_path.unlink(missing_ok=True)
            else:
                atomic_write(command_path, previous_command, 0o700)
        except OSError as error:
            rollback_errors.append(error)
        try:
            if previous_conf is None:
                conf_path.unlink(missing_ok=True)
            else:
                atomic_write(conf_path, previous_conf, 0o600)
        except OSError as error:
            rollback_errors.append(error)
        for stale_manifest in manifests_to_replace:
            try:
                remove_manifest_path(pathlib.Path(stale_manifest))
            except OSError as error:
                rollback_errors.append(error)
        for stale_manifest, (contents, mode) in previous_manifests.items():
            try:
                atomic_write(pathlib.Path(stale_manifest), contents, mode)
            except OSError as error:
                rollback_errors.append(error)
        for action in ("reread", "update"):
            try:
                run_ctl(action, check=False)
            except (OSError, subprocess.CalledProcessError) as error:
                rollback_errors.append(error)
        if previous_conf is not None:
            try:
                run_ctl("start", name, check=False)
            except (OSError, subprocess.CalledProcessError) as error:
                rollback_errors.append(error)
        if rollback_errors:
            raise rollback_errors[0]

    command = supervisor_value(str(command_path))
    directory = supervisor_value(str(service_directory))
    environment = ",".join([
        f'PORT="{port}"',
        f'PUBLIC_URL="{environment_value(public_url)}"',
        'AMP_ORB="1"',
        'HOME="/home/user"',
        f'PATH="{environment_value(os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"))}"',
    ])
    try:
        for stale_manifest in manifests_to_replace:
            remove_manifest_path(pathlib.Path(stale_manifest))
        atomic_write(command_path, "#!/bin/sh\nexec /bin/sh -lc " + shlex.quote(args.command) + "\n", 0o700)
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
    except (OSError, subprocess.CalledProcessError):
        rollback()
        raise
    started = False
    for _ in range(100):
        status = run_ctl("status", name, check=False)
        state_name = service_state(status, name)
        if state_name == "RUNNING":
            if not wait_ready(port, health):
                rollback()
                health_detail = f" with health path {health}" if health else ""
                raise SystemExit(f"service {name} did not become ready on port {port}{health_detail}")
            if args.portal:
                if portal_manifest_current:
                    sys.stdout.write(public_url + "\n")
                else:
                    try:
                        portal_command = [
                            "amp-orb-portal", str(port), "--name", name,
                            "--title", title, "--description", description,
                        ]
                        portal = subprocess.run(portal_command, text=True, capture_output=True, check=True, cwd=service_directory)
                    except (subprocess.CalledProcessError, OSError):
                        rollback()
                        raise
                    sys.stdout.write(portal.stdout)
            state[name] = registration
            save_state(state)
            sys.stdout.write(status.stdout)
            raise SystemExit(0)
        if state_name == "STOPPED" and not started:
            run_ctl("start", name, check=False)
            started = True
        if state_name in {"BACKOFF", "EXITED", "FATAL", "UNKNOWN"}:
            sys.stdout.write(status.stdout or status.stderr)
            rollback()
            raise SystemExit(1)
        time.sleep(0.1)
    sys.stdout.write(status.stdout or status.stderr)
    rollback()
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
	if record.lifecycleV1 && record.state != neoOrbStateRunning {
		c.JSON(http.StatusConflict, gin.H{"error": "orb is not ready", "state": record.state})
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
	if record.lifecycleV1 {
		operation, admissionErr := manager.beginLifecycleAdmission(c.Request.Context(), actor, threadID, neoOrbStateRunning)
		if admissionErr != nil {
			body := gin.H{"error": "orb is not ready"}
			if current, ok := manager.snapshot(threadID); ok && current.state != "" {
				body["state"] = current.state
			}
			c.JSON(http.StatusConflict, body)
			return
		}
		operation.close()
	}
	proxy.ServeHTTP(c.Writer, c.Request)
}
