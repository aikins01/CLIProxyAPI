package amp

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/orbcredentials"
)

const (
	neoOwnerOrbCredentialDirectory  = "orb-credentials"
	neoOrbCredentialKeyFileEnv      = "CLIPROXYAPI_ORB_CREDENTIALS_KEY_FILE"
	neoOrbCredentialEnvelopeLimit   = 2 << 20
	neoOrbCredentialRevisionHeader  = orbcredentials.RevisionHeader
	neoOrbCredentialAbsentRevision  = orbcredentials.AbsentRevision
	neoOrbCredentialRevokedRevision = orbcredentials.RevokedRevision
	neoOrbCredentialRepairRevision  = orbcredentials.RepairRevision
)

var neoOrbCredentialRevisionPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var errNeoStoredOrbCredentialsCorrupt = errors.New("stored owner credentials are invalid")
var errNeoOwnerOrbCredentialRevisionConflict = errors.New("owner credential revision conflict")

type neoOwnerOrbCredentialStore struct {
	mu      sync.Mutex
	rootDir string
	aead    cipher.AEAD
}

type neoOwnerOrbCredentialEnvelope struct {
	Version    int    `json:"version"`
	Revision   string `json:"revision"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

type neoOwnerOrbCredentialUploadRequest struct {
	BrokerID          string                  `json:"brokerId"`
	SessionID         string                  `json:"sessionId"`
	SessionGeneration uint64                  `json:"sessionGeneration"`
	Credentials       orbcredentials.Snapshot `json:"credentials"`
}

type neoOwnerOrbCredentialClearRequest struct {
	BrokerID          string `json:"brokerId"`
	SessionID         string `json:"sessionId"`
	SessionGeneration uint64 `json:"sessionGeneration"`
	ExpectedRevision  string `json:"expectedRevision"`
}

type neoOwnerOrbCredentialTombstone struct {
	Version int    `json:"version"`
	State   string `json:"state"`
}

func newNeoOwnerOrbCredentialStore(threadDir string) (*neoOwnerOrbCredentialStore, error) {
	key, found, err := loadNeoOwnerOrbCredentialKey()
	if err != nil || !found {
		return nil, err
	}
	dataRoot := filepath.Dir(strings.TrimSpace(threadDir))
	if strings.TrimSpace(threadDir) == "" || dataRoot == "." {
		return nil, errors.New("owner credential store is unavailable")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("owner credential store is unavailable")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("owner credential store is unavailable")
	}
	return &neoOwnerOrbCredentialStore{rootDir: filepath.Join(dataRoot, neoOwnerOrbCredentialDirectory), aead: aead}, nil
}

func loadNeoOwnerOrbCredentialKey() ([]byte, bool, error) {
	keyPath := strings.TrimSpace(os.Getenv(neoOrbCredentialKeyFileEnv))
	if keyPath == "" {
		return nil, false, nil
	}
	info, err := os.Lstat(keyPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 4096 || info.Mode().Perm()&0o077 != 0 {
		return nil, false, errors.New("owner credential key file is invalid")
	}
	if !neoOrbFileOwnedByCurrentUser(info) {
		return nil, false, errors.New("owner credential key file is invalid")
	}
	file, err := os.Open(keyPath)
	if err != nil {
		return nil, false, errors.New("owner credential key file is unavailable")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, 4097))
	after, statErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil || statErr != nil || closeErr != nil || after == nil || !after.Mode().IsRegular() || after.Size() <= 0 || after.Size() > 4096 || after.Mode().Perm()&0o077 != 0 || !os.SameFile(info, after) || !neoOrbFileOwnedByCurrentUser(after) || len(raw) > 4096 {
		return nil, false, errors.New("owner credential key file is unavailable")
	}
	encoded := strings.TrimSpace(string(raw))
	key, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(key) != 32 || base64.StdEncoding.EncodeToString(key) != encoded {
		return nil, false, errors.New("owner credential key file is invalid")
	}
	return key, true, nil
}

func (store *neoOwnerOrbCredentialStore) put(ownerUserID string, snapshot orbcredentials.Snapshot) (string, error) {
	return store.putWithCorruptRepair(ownerUserID, snapshot, false)
}

func (store *neoOwnerOrbCredentialStore) putRepairingCorrupt(ownerUserID string, snapshot orbcredentials.Snapshot) (string, error) {
	return store.putWithCorruptRepair(ownerUserID, snapshot, true)
}

func (store *neoOwnerOrbCredentialStore) putWithCorruptRepair(ownerUserID string, snapshot orbcredentials.Snapshot, repairCorrupt bool) (string, error) {
	normalized, _, err := orbcredentials.Validate(snapshot)
	if err != nil {
		return "", errors.New("owner credential snapshot is invalid")
	}
	plaintext, err := json.Marshal(normalized)
	if err != nil {
		return "", errors.New("owner credential snapshot is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	_, currentPlaintext, revision, found, revoked, err := store.readLocked(ownerUserID)
	if err != nil && (!repairCorrupt || !errors.Is(err, errNeoStoredOrbCredentialsCorrupt)) {
		return "", err
	}
	if found && !revoked && bytes.Equal(currentPlaintext, plaintext) {
		return revision, nil
	}
	revisionBytes := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, revisionBytes); err != nil {
		return "", errors.New("owner credential snapshot could not be persisted")
	}
	revision = hex.EncodeToString(revisionBytes)
	if err := store.writeLocked(ownerUserID, revision, plaintext); err != nil {
		return "", err
	}
	return revision, nil
}

func (store *neoOwnerOrbCredentialStore) revokeIfRevision(ownerUserID, expectedRevision string) (string, error) {
	if expectedRevision != neoOrbCredentialAbsentRevision && expectedRevision != neoOrbCredentialRepairRevision && !neoOrbCredentialRevisionPattern.MatchString(expectedRevision) {
		return "", errNeoOwnerOrbCredentialRevisionConflict
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	_, _, revision, found, revoked, err := store.readLocked(ownerUserID)
	if revoked {
		return neoOrbCredentialRevokedRevision, nil
	}
	if errors.Is(err, errNeoStoredOrbCredentialsCorrupt) {
		revision = neoOrbCredentialRepairRevision
		found = true
		err = nil
	}
	if err != nil {
		return "", err
	}
	if !found {
		revision = neoOrbCredentialAbsentRevision
	}
	if revision != expectedRevision {
		return "", errNeoOwnerOrbCredentialRevisionConflict
	}
	plaintext, err := json.Marshal(neoOwnerOrbCredentialTombstone{Version: 1, State: neoOrbCredentialRevokedRevision})
	if err != nil {
		return "", errors.New("owner credential revocation could not be persisted")
	}
	if err := store.writeLocked(ownerUserID, neoOrbCredentialRevokedRevision, plaintext); err != nil {
		return "", errors.New("owner credential revocation could not be persisted")
	}
	return neoOrbCredentialRevokedRevision, nil
}

func (store *neoOwnerOrbCredentialStore) writeLocked(ownerUserID, revision string, plaintext []byte) error {
	nonce := make([]byte, store.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	ciphertext := store.aead.Seal(nil, nonce, plaintext, neoOwnerOrbCredentialAssociatedData(ownerUserID))
	envelope := neoOwnerOrbCredentialEnvelope{
		Version:    1,
		Revision:   revision,
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(ciphertext),
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	ownerDir, err := store.ownerDirectory(ownerUserID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(ownerDir, 0o700); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	if err := os.Chmod(store.rootDir, 0o700); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	if err := os.Chmod(ownerDir, 0o700); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	if err := writeNeoDurableAtomicFile(filepath.Join(ownerDir, "credentials.enc"), append(raw, '\n'), 0o600); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	if err := syncNeoDurableDirectory(ownerDir); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	if err := syncNeoDurableDirectory(store.rootDir); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	if err := syncNeoDurableDirectory(filepath.Dir(store.rootDir)); err != nil {
		return errors.New("owner credential snapshot could not be persisted")
	}
	return nil
}

func (store *neoOwnerOrbCredentialStore) load(ownerUserID string) (orbcredentials.Decoded, string, bool, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	snapshot, _, revision, found, revoked, err := store.readLocked(ownerUserID)
	if err != nil || !found || revoked {
		return orbcredentials.Decoded{}, revision, found && !revoked, revoked, err
	}
	_, decoded, err := orbcredentials.Validate(snapshot)
	if err != nil {
		return orbcredentials.Decoded{}, "", false, false, errors.New("stored owner credentials are invalid")
	}
	return decoded, revision, true, false, nil
}

func (store *neoOwnerOrbCredentialStore) revision(ownerUserID string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	_, _, revision, found, revoked, err := store.readLocked(ownerUserID)
	if errors.Is(err, errNeoStoredOrbCredentialsCorrupt) {
		return neoOrbCredentialRepairRevision, nil
	}
	if err != nil {
		return "", err
	}
	if !found {
		return neoOrbCredentialAbsentRevision, nil
	}
	if revoked {
		return neoOrbCredentialRevokedRevision, nil
	}
	return revision, nil
}

func (store *neoOwnerOrbCredentialStore) readLocked(ownerUserID string) (orbcredentials.Snapshot, []byte, string, bool, bool, error) {
	ownerDir, err := store.ownerDirectory(ownerUserID)
	if err != nil {
		return orbcredentials.Snapshot{}, nil, "", false, false, err
	}
	credentialPath := filepath.Join(ownerDir, "credentials.enc")
	info, err := os.Lstat(credentialPath)
	if errors.Is(err, os.ErrNotExist) {
		return orbcredentials.Snapshot{}, nil, "", false, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > neoOrbCredentialEnvelopeLimit || info.Mode().Perm()&0o077 != 0 {
		return orbcredentials.Snapshot{}, nil, "", false, false, errors.New("stored owner credentials are invalid")
	}
	raw, err := os.ReadFile(credentialPath)
	if err != nil {
		return orbcredentials.Snapshot{}, nil, "", false, false, errors.New("stored owner credentials are invalid")
	}
	if rejectNeoLocalBrokerDuplicateJSONFields(raw) != nil {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	var envelope neoOwnerOrbCredentialEnvelope
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	if envelope.Version != 1 || envelope.Revision != neoOrbCredentialRevokedRevision && !neoOrbCredentialRevisionPattern.MatchString(envelope.Revision) {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	nonce, err := decodeNeoOrbCredentialEnvelopeValue(envelope.Nonce, store.aead.NonceSize())
	if err != nil || len(nonce) != store.aead.NonceSize() {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	ciphertext, err := decodeNeoOrbCredentialEnvelopeValue(envelope.Ciphertext, neoOrbCredentialEnvelopeLimit)
	if err != nil || len(ciphertext) < store.aead.Overhead() {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	plaintext, err := store.aead.Open(nil, nonce, ciphertext, neoOwnerOrbCredentialAssociatedData(ownerUserID))
	if err != nil {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	if envelope.Revision == neoOrbCredentialRevokedRevision {
		if !decodeNeoOwnerOrbCredentialTombstone(plaintext) {
			return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
		}
		return orbcredentials.Snapshot{}, plaintext, envelope.Revision, true, true, nil
	}
	snapshot, _, err := orbcredentials.Decode(plaintext)
	if err != nil {
		return orbcredentials.Snapshot{}, nil, "", false, false, errNeoStoredOrbCredentialsCorrupt
	}
	return snapshot, plaintext, envelope.Revision, true, false, nil
}

func decodeNeoOwnerOrbCredentialTombstone(plaintext []byte) bool {
	if len(plaintext) == 0 || rejectNeoLocalBrokerDuplicateJSONFields(plaintext) != nil {
		return false
	}
	var tombstone neoOwnerOrbCredentialTombstone
	decoder := json.NewDecoder(bytes.NewReader(plaintext))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&tombstone); err != nil {
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return false
	}
	return tombstone.Version == 1 && tombstone.State == neoOrbCredentialRevokedRevision
}

func (store *neoOwnerOrbCredentialStore) ownerDirectory(ownerUserID string) (string, error) {
	ownerUserID = strings.TrimSpace(ownerUserID)
	if store == nil || store.aead == nil || strings.TrimSpace(store.rootDir) == "" {
		return "", errors.New("owner credential store is unavailable")
	}
	if !neoLocalBrokerSafeText(ownerUserID, neoLocalBrokerIdentifierLimit, false) {
		return "", errors.New("owner is invalid")
	}
	digest := sha256.Sum256([]byte(ownerUserID))
	return filepath.Join(store.rootDir, hex.EncodeToString(digest[:])), nil
}

func neoOwnerOrbCredentialAssociatedData(ownerUserID string) []byte {
	return []byte("cliproxyapi-owner-orb-credentials-v1\x00" + strings.TrimSpace(ownerUserID))
}

func decodeNeoOrbCredentialEnvelopeValue(value string, limit int) ([]byte, error) {
	decoded, err := base64.StdEncoding.Strict().DecodeString(value)
	if err != nil || len(decoded) > limit || base64.StdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("invalid credential envelope")
	}
	return decoded, nil
}

func (rt *neoRuntime) ownerOrbCredentials(ownerUserID string) (orbcredentials.Decoded, string, bool, bool, error) {
	if rt == nil || rt.orbCredentialStore == nil {
		return orbcredentials.Decoded{}, "", false, false, errors.New("owner credential store is unavailable")
	}
	return rt.orbCredentialStore.load(ownerUserID)
}

func (actor *neoActor) validateOwnerOrbCredentialBrokerLocked(brokerID, sessionID string, generation uint64) error {
	if actor == nil || actor.runtime == nil || actor.runtime.orbCredentialStore == nil || !neoGatewayUserActorTarget(actor.name) {
		return errNeoLocalBrokerStaleSession
	}
	session := actor.userBrokerSessions[brokerID]
	fence := actor.userBrokerFences[brokerID]
	if session.sessionID != sessionID || session.sessionGeneration != generation || session.updatedAt.Before(time.Now().Add(-neoLocalBrokerHeartbeatTTL)) || fence.sessionID != sessionID || fence.sessionGeneration != generation {
		return errNeoLocalBrokerStaleSession
	}
	if err := actor.runtime.validateOwnerOrbConfigBrokerFence(actor.key, brokerID, sessionID, generation); err != nil {
		return err
	}
	return nil
}

func (actor *neoActor) putOwnerOrbCredentials(brokerID, sessionID string, generation uint64, snapshot orbcredentials.Snapshot) (string, error) {
	if actor == nil {
		return "", errNeoLocalBrokerStaleSession
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if err := actor.validateOwnerOrbCredentialBrokerLocked(brokerID, sessionID, generation); err != nil {
		return "", err
	}
	return actor.runtime.orbCredentialStore.putRepairingCorrupt(actor.key, snapshot)
}

func (actor *neoActor) clearOwnerOrbCredentials(brokerID, sessionID string, generation uint64, expectedRevision string) (string, error) {
	if actor == nil {
		return "", errNeoLocalBrokerStaleSession
	}
	actor.mu.Lock()
	defer actor.mu.Unlock()
	if err := actor.validateOwnerOrbCredentialBrokerLocked(brokerID, sessionID, generation); err != nil {
		return "", err
	}
	return actor.runtime.orbCredentialStore.revokeIfRevision(actor.key, expectedRevision)
}

func (m *AmpModule) serveLocalBrokerOrbCredentials(c *gin.Context) {
	if strings.TrimSpace(c.GetHeader("Origin")) != "" {
		c.JSON(http.StatusForbidden, gin.H{"ok": false, "error": "browser_origin_forbidden", "message": "browser-origin requests are not accepted"})
		return
	}
	if c.Request.Method != http.MethodPut && c.Request.Method != http.MethodDelete {
		c.Header("Allow", http.MethodPut+", "+http.MethodDelete)
		c.JSON(http.StatusMethodNotAllowed, gin.H{"ok": false, "error": "method_not_allowed", "message": "method not allowed"})
		return
	}
	if m == nil || m.neoRuntime == nil || m.neoRuntime.store == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "runtime_unavailable", "message": "orb credential runtime is unavailable"})
		return
	}
	if m.neoRuntime.orbCredentialStore == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"ok": false, "error": "credential_store_unavailable", "message": "orb credential key-file configuration is disabled or invalid"})
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
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, orbcredentials.MaxBodyBytes)
	payload, err := io.ReadAll(c.Request.Body)
	if err != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"ok": false, "error": "invalid_request", "message": "invalid orb credential body"})
		return
	}
	userActor := m.neoRuntime.store.userActorForOwner(ownerUserID)
	if userActor == nil {
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "stale_session", "message": errNeoLocalBrokerStaleSession.Error()})
		return
	}
	var revision string
	if c.Request.Method == http.MethodPut {
		request, decodeErr := decodeNeoOwnerOrbCredentialUpload(payload)
		if decodeErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_request", "message": decodeErr.Error()})
			return
		}
		revision, err = userActor.putOwnerOrbCredentials(request.BrokerID, request.SessionID, request.SessionGeneration, request.Credentials)
	} else {
		request, decodeErr := decodeNeoOwnerOrbCredentialClear(payload)
		if decodeErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "error": "invalid_request", "message": decodeErr.Error()})
			return
		}
		revision, err = userActor.clearOwnerOrbCredentials(request.BrokerID, request.SessionID, request.SessionGeneration, request.ExpectedRevision)
	}
	if errors.Is(err, errNeoLocalBrokerStaleSession) || errors.Is(err, errNeoLocalBrokerFenceUnavailable) {
		status := http.StatusConflict
		code := "stale_session"
		if errors.Is(err, errNeoLocalBrokerFenceUnavailable) {
			status = http.StatusServiceUnavailable
			code = "fence_unavailable"
		}
		c.JSON(status, gin.H{"ok": false, "error": code, "message": code})
		return
	}
	if errors.Is(err, errNeoOwnerOrbCredentialRevisionConflict) {
		c.JSON(http.StatusConflict, gin.H{"ok": false, "error": "revision_conflict", "message": "revision_conflict"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"ok": false, "error": "persistence_failed", "message": "orb credentials could not be persisted"})
		return
	}
	c.Header(neoOrbCredentialRevisionHeader, revision)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func decodeNeoOwnerOrbCredentialUpload(payload []byte) (neoOwnerOrbCredentialUploadRequest, error) {
	if len(payload) == 0 || rejectNeoLocalBrokerDuplicateJSONFields(payload) != nil {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("invalid orb credential body")
	}
	var request neoOwnerOrbCredentialUploadRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("invalid orb credential body")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("invalid orb credential body")
	}
	if !neoLocalBrokerIdentifierPattern.MatchString(request.BrokerID) {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("brokerId is invalid")
	}
	if !neoLocalBrokerIdentifierPattern.MatchString(request.SessionID) {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("sessionId is invalid")
	}
	if request.SessionGeneration == 0 {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("sessionGeneration is invalid")
	}
	normalized, _, err := orbcredentials.Validate(request.Credentials)
	if err != nil {
		return neoOwnerOrbCredentialUploadRequest{}, errors.New("credential snapshot is invalid")
	}
	request.Credentials = normalized
	return request, nil
}

func decodeNeoOwnerOrbCredentialClear(payload []byte) (neoOwnerOrbCredentialClearRequest, error) {
	if len(payload) == 0 || rejectNeoLocalBrokerDuplicateJSONFields(payload) != nil {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("invalid orb credential body")
	}
	var request neoOwnerOrbCredentialClearRequest
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("invalid orb credential body")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("invalid orb credential body")
	}
	if !neoLocalBrokerIdentifierPattern.MatchString(request.BrokerID) {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("brokerId is invalid")
	}
	if !neoLocalBrokerIdentifierPattern.MatchString(request.SessionID) {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("sessionId is invalid")
	}
	if request.SessionGeneration == 0 {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("sessionGeneration is invalid")
	}
	if request.ExpectedRevision != neoOrbCredentialAbsentRevision && request.ExpectedRevision != neoOrbCredentialRepairRevision && !neoOrbCredentialRevisionPattern.MatchString(request.ExpectedRevision) {
		return neoOwnerOrbCredentialClearRequest{}, errors.New("expectedRevision is invalid")
	}
	return request, nil
}
