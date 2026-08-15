package amp

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbconfig"
	log "github.com/sirupsen/logrus"
)

const neoOwnerOrbConfigDirectory = "orb-config-bundles"

type neoOwnerOrbConfigStore struct {
	mu      sync.Mutex
	rootDir string
}

type neoOwnerOrbConfigUploadRequest struct {
	BrokerID          string            `json:"brokerId"`
	SessionID         string            `json:"sessionId"`
	SessionGeneration uint64            `json:"sessionGeneration"`
	Bundle            *orbconfig.Bundle `json:"bundle"`
}

func newNeoOwnerOrbConfigStore(threadDir string) *neoOwnerOrbConfigStore {
	dataRoot := filepath.Dir(strings.TrimSpace(threadDir))
	if strings.TrimSpace(threadDir) == "" || dataRoot == "." {
		return &neoOwnerOrbConfigStore{}
	}
	return &neoOwnerOrbConfigStore{rootDir: filepath.Join(dataRoot, neoOwnerOrbConfigDirectory)}
}

func (store *neoOwnerOrbConfigStore) currentDigest(ownerUserID string) (string, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	bundle, _, found, err := store.readLocked(ownerUserID)
	if err != nil {
		log.Warnf("amp orbs: stored owner configuration is invalid and awaiting replacement: %v", err)
		return "", false, nil
	}
	if !found {
		return "", false, nil
	}
	return bundle.Digest, true, nil
}

func (store *neoOwnerOrbConfigStore) load(ownerUserID string) (orbconfig.Bundle, map[string][]byte, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.readLocked(ownerUserID)
}

func (store *neoOwnerOrbConfigStore) put(ownerUserID string, bundle orbconfig.Bundle) (string, error) {
	normalized, _, err := orbconfig.Validate(bundle)
	if err != nil {
		return "", err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	current, _, found, err := store.readLocked(ownerUserID)
	if err != nil {
		found = false
	}
	if found && current.Digest == normalized.Digest {
		return normalized.Digest, nil
	}
	ownerDir, err := store.ownerDirectory(ownerUserID)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(ownerDir, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(store.rootDir, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(ownerDir, 0o700); err != nil {
		return "", err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	bundlePath := filepath.Join(ownerDir, "bundle.json")
	if err := writeNeoDurableAtomicFile(bundlePath, append(raw, '\n'), 0o600); err != nil {
		return "", err
	}
	if err := syncNeoDurableDirectory(ownerDir); err != nil {
		return "", err
	}
	if err := syncNeoDurableDirectory(store.rootDir); err != nil {
		return "", err
	}
	if err := syncNeoDurableDirectory(filepath.Dir(store.rootDir)); err != nil {
		return "", err
	}
	return normalized.Digest, nil
}

func (store *neoOwnerOrbConfigStore) readLocked(ownerUserID string) (orbconfig.Bundle, map[string][]byte, bool, error) {
	ownerDir, err := store.ownerDirectory(ownerUserID)
	if err != nil {
		return orbconfig.Bundle{}, nil, false, err
	}
	bundlePath := filepath.Join(ownerDir, "bundle.json")
	info, err := os.Lstat(bundlePath)
	if errors.Is(err, os.ErrNotExist) {
		return orbconfig.Bundle{}, nil, false, nil
	}
	if err != nil {
		return orbconfig.Bundle{}, nil, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > orbconfig.MaxBodyBytes {
		return orbconfig.Bundle{}, nil, false, errors.New("stored owner orb configuration is invalid")
	}
	raw, err := os.ReadFile(bundlePath)
	if err != nil {
		return orbconfig.Bundle{}, nil, false, err
	}
	if err := rejectNeoLocalBrokerDuplicateJSONFields(raw); err != nil {
		return orbconfig.Bundle{}, nil, false, errors.New("stored owner orb configuration contains invalid JSON")
	}
	var bundle orbconfig.Bundle
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&bundle); err != nil {
		return orbconfig.Bundle{}, nil, false, errors.New("stored owner orb configuration contains invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return orbconfig.Bundle{}, nil, false, errors.New("stored owner orb configuration contains trailing JSON")
	}
	normalized, files, err := orbconfig.Validate(bundle)
	if err != nil {
		return orbconfig.Bundle{}, nil, false, fmt.Errorf("stored owner orb configuration failed validation: %w", err)
	}
	return normalized, files, true, nil
}

func (store *neoOwnerOrbConfigStore) ownerDirectory(ownerUserID string) (string, error) {
	ownerUserID = strings.TrimSpace(ownerUserID)
	if store == nil || strings.TrimSpace(store.rootDir) == "" {
		return "", errors.New("owner orb configuration store is unavailable")
	}
	if !neoLocalBrokerSafeText(ownerUserID, neoLocalBrokerIdentifierLimit, false) {
		return "", errors.New("owner is invalid")
	}
	digest := sha256.Sum256([]byte(ownerUserID))
	return filepath.Join(store.rootDir, hex.EncodeToString(digest[:])), nil
}

func (rt *neoRuntime) ownerOrbConfigDigest(ownerUserID string) (string, bool, error) {
	if rt == nil || rt.orbConfigStore == nil {
		return "", false, errors.New("owner orb configuration store is unavailable")
	}
	return rt.orbConfigStore.currentDigest(ownerUserID)
}

func (rt *neoRuntime) ownerOrbConfig(ownerUserID string) (orbconfig.Bundle, map[string][]byte, bool, error) {
	if rt == nil || rt.orbConfigStore == nil {
		return orbconfig.Bundle{}, nil, false, errors.New("owner orb configuration store is unavailable")
	}
	return rt.orbConfigStore.load(ownerUserID)
}

func (rt *neoRuntime) validateOwnerOrbConfigBrokerFence(ownerUserID, brokerID, sessionID string, generation uint64) error {
	if rt == nil {
		return errNeoLocalBrokerFenceUnavailable
	}
	rt.brokerFenceMu.Lock()
	defer rt.brokerFenceMu.Unlock()
	if rt.brokerFenceStoreErr != nil {
		return errNeoLocalBrokerFenceUnavailable
	}
	fence := rt.brokerFences[ownerUserID][brokerID]
	if fence.sessionID != sessionID || fence.sessionGeneration != generation {
		return errNeoLocalBrokerStaleSession
	}
	return nil
}

func (actor *neoActor) putOwnerOrbConfigBundle(brokerID, sessionID string, generation uint64, bundle orbconfig.Bundle) (string, error) {
	if actor == nil || actor.runtime == nil || !neoGatewayUserActorTarget(actor.name) {
		return "", errNeoLocalBrokerStaleSession
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	session := actor.userBrokerSessions[brokerID]
	fence := actor.userBrokerFences[brokerID]
	if session.sessionID != sessionID || session.sessionGeneration != generation || session.updatedAt.Before(time.Now().Add(-neoLocalBrokerHeartbeatTTL)) || fence.sessionID != sessionID || fence.sessionGeneration != generation {
		return "", errNeoLocalBrokerStaleSession
	}
	if err := actor.runtime.validateOwnerOrbConfigBrokerFence(actor.key, brokerID, sessionID, generation); err != nil {
		return "", err
	}
	return actor.runtime.orbConfigStore.put(actor.key, bundle)
}

func (m *AmpModule) serveLocalBrokerOrbConfigBundle(c *gin.Context) {
	if strings.TrimSpace(c.GetHeader("Origin")) != "" {
		c.JSON(http.StatusForbidden, gin.H{"ok": false, "error": "browser_origin_forbidden", "message": "browser-origin requests are not accepted"})
		return
	}
	if c.Request.Method != http.MethodPut {
		c.Header("Allow", http.MethodPut)
		c.JSON(http.StatusMethodNotAllowed, gin.H{"ok": false, "error": "method_not_allowed", "message": "method not allowed"})
		return
	}
	if m == nil || m.neoRuntime == nil || m.neoRuntime.store == nil || m.neoRuntime.orbConfigStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "runtime_unavailable", "message": "orb configuration runtime is unavailable"})
		return
	}
	if strings.TrimSpace(getClientAPIKeyFromContext(c.Request.Context())) == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "authentication_required", "message": "authenticated client API key is required"})
		return
	}
	ownerUserID := strings.TrimSpace(m.neoRuntime.neoRequestOwnerUserID(c.Request.Context()))
	if ownerUserID == "" || !neoRequestOwnerScopeResolved(c.Request.Context(), ownerUserID) {
		c.JSON(http.StatusUnauthorized, gin.H{"ok": false, "error": "owner_unavailable", "message": "authenticated owner is unavailable"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		c.JSON(http.StatusUnsupportedMediaType, gin.H{"ok": false, "error": "invalid_content_type", "message": "Content-Type must be application/json"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, orbconfig.MaxBodyBytes)
	payload, err := io.ReadAll(c.Request.Body)
	if err != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"ok": false, "error": "invalid_request", "message": "invalid orb configuration bundle body"})
		return
	}
	request, err := decodeNeoOwnerOrbConfigUpload(payload)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_request", "message": err.Error()})
		return
	}
	userActor := m.neoRuntime.store.userActorForOwner(ownerUserID)
	if userActor == nil {
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "stale_session", "message": errNeoLocalBrokerStaleSession.Error()})
		return
	}
	digest, err := userActor.putOwnerOrbConfigBundle(request.BrokerID, request.SessionID, request.SessionGeneration, *request.Bundle)
	if err != nil && (errors.Is(err, errNeoLocalBrokerStaleSession) || errors.Is(err, errNeoLocalBrokerFenceUnavailable)) {
		status := http.StatusConflict
		code := "stale_session"
		if errors.Is(err, errNeoLocalBrokerFenceUnavailable) {
			status = http.StatusServiceUnavailable
			code = "fence_unavailable"
		}
		c.JSON(status, gin.H{"ok": false, "error": code, "message": err.Error()})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "persistence_failed", "message": "orb configuration bundle could not be persisted"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "digest": digest})
}

func decodeNeoOwnerOrbConfigUpload(payload []byte) (neoOwnerOrbConfigUploadRequest, error) {
	if len(payload) == 0 {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("orb configuration bundle body is empty")
	}
	if err := rejectNeoLocalBrokerDuplicateJSONFields(payload); err != nil {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("orb configuration bundle contains invalid JSON")
	}
	var request neoOwnerOrbConfigUploadRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("orb configuration bundle contains invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("orb configuration bundle contains trailing JSON")
	}
	if !neoLocalBrokerIdentifierPattern.MatchString(request.BrokerID) {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("brokerId is invalid")
	}
	if !neoLocalBrokerIdentifierPattern.MatchString(request.SessionID) {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("sessionId is invalid")
	}
	if request.SessionGeneration == 0 {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("sessionGeneration is invalid")
	}
	if request.Bundle == nil {
		return neoOwnerOrbConfigUploadRequest{}, errors.New("bundle is required")
	}
	normalized, _, err := orbconfig.Validate(*request.Bundle)
	if err != nil {
		return neoOwnerOrbConfigUploadRequest{}, fmt.Errorf("bundle is invalid: %w", err)
	}
	request.Bundle = &normalized
	return request, nil
}
