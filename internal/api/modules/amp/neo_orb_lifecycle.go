package amp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	neoOrbLifecycleStoreVersion = 3
	neoOrbLifecycleStoreV2      = 2
	neoOrbLifecycleFileName     = ".cliproxyapi-orbs.json"
	neoOrbLifecycleLockFileName = ".cliproxyapi-orbs.lock"

	neoOrbLifecyclePhasePending  = "pending"
	neoOrbLifecyclePhaseActive   = "active"
	neoOrbLifecyclePhaseRetained = "retained"

	neoOrbLifecycleRoleContainer = "container"
	neoOrbLifecycleRoleHome      = "home"
	neoOrbLifecycleRoleRoot      = "root"

	neoOrbLifecycleActivationBinding    = "binding"
	neoOrbLifecycleActivationLaunching  = "launching"
	neoOrbLifecycleActivationActionable = "actionable"

	neoOrbLifecycleOperationStart        = "start"
	neoOrbLifecycleOperationUnpause      = "unpause"
	neoOrbLifecycleOperationExecDetached = "exec-detached"
)

type neoOrbLifecycleGeneration struct {
	ThreadID              string `json:"threadId"`
	Generation            uint64 `json:"generation"`
	ContainerID           string `json:"containerId"`
	ContainerName         string `json:"containerName"`
	PortalToken           string `json:"portalToken"`
	HomeVolumeName        string `json:"homeVolumeName"`
	RootVolumeName        string `json:"rootVolumeName"`
	Phase                 string `json:"phase"`
	CreatedAt             string `json:"createdAt"`
	AuthenticatedOwnerID  string `json:"authenticatedOwnerId,omitempty"`
	Revision              uint64 `json:"revision,omitempty"`
	ActivationState       string `json:"activationState,omitempty"`
	OperationID           string `json:"operationId,omitempty"`
	OperationKind         string `json:"operationKind,omitempty"`
	MultiplayerTTLSeconds int    `json:"multiplayerTTLSeconds,omitempty"`
}

type neoOrbLifecycleActivation struct {
	State         string
	OperationID   string
	OperationKind string
}

type neoOrbLifecycleThreadRecord struct {
	ThreadID string                      `json:"threadId"`
	Active   *neoOrbLifecycleGeneration  `json:"active"`
	Pending  *neoOrbLifecycleGeneration  `json:"pending"`
	Retained []neoOrbLifecycleGeneration `json:"retained"`
}

type neoOrbLifecycleCleanupTombstone struct {
	ThreadID string                      `json:"threadId"`
	Active   *neoOrbLifecycleGeneration  `json:"active"`
	Pending  *neoOrbLifecycleGeneration  `json:"pending"`
	Retained []neoOrbLifecycleGeneration `json:"retained"`
}

type neoOrbLifecycleState struct {
	Version             int                                        `json:"version"`
	OwnerID             string                                     `json:"ownerId"`
	Provider            string                                     `json:"provider"`
	GenerationHighWater map[string]uint64                          `json:"generationHighWater"`
	Threads             map[string]neoOrbLifecycleThreadRecord     `json:"threads"`
	CleanupTombstones   map[string]neoOrbLifecycleCleanupTombstone `json:"cleanupTombstones"`
}

type neoOrbLifecycleStore struct {
	mu                sync.Mutex
	path              string
	provider          string
	state             neoOrbLifecycleState
	now               func() time.Time
	lockFile          *os.File
	closed            bool
	durabilityPending bool
	durabilityPath    string
	writeAtomic       func(string, []byte, os.FileMode) error
	syncDirectory     func(string) error
}

type neoOrbGenerationRecord = neoOrbLifecycleGeneration
type neoOrbThreadLifecycleRecord = neoOrbLifecycleThreadRecord
type neoOrbCleanupTombstone = neoOrbLifecycleCleanupTombstone

type neoOrbLifecycleDurabilityError struct {
	err error
}

func (err *neoOrbLifecycleDurabilityError) Error() string {
	return "orb lifecycle store durability is uncertain: " + err.err.Error()
}

func (err *neoOrbLifecycleDurabilityError) Unwrap() error {
	return err.err
}

func newNeoOrbLifecycleStore(threadDir, providerIdentity string) (*neoOrbLifecycleStore, error) {
	return newNeoOrbLifecycleStoreWithIO(threadDir, providerIdentity, writeNeoDurableAtomicFile, syncNeoDurableDirectory)
}

func newNeoOrbLifecycleStoreWithIO(threadDir, providerIdentity string, writeAtomic func(string, []byte, os.FileMode) error, syncDirectory func(string) error) (*neoOrbLifecycleStore, error) {
	threadDir = strings.TrimSpace(threadDir)
	if threadDir == "" {
		return nil, errors.New("orb lifecycle store directory is unavailable")
	}
	parent := filepath.Dir(filepath.Clean(threadDir))
	if parent == "." || parent == string(filepath.Separator) {
		return nil, errors.New("orb lifecycle store directory is invalid")
	}
	provider, err := resolveNeoOrbDockerEndpoint(providerIdentity)
	if err != nil {
		return nil, err
	}
	store := &neoOrbLifecycleStore{
		path:          filepath.Join(parent, neoOrbLifecycleFileName),
		provider:      provider,
		now:           time.Now,
		writeAtomic:   writeAtomic,
		syncDirectory: syncDirectory,
	}
	createdParent, err := prepareNeoOrbLifecycleDirectory(parent, store.path)
	if err != nil {
		return nil, err
	}
	if err := store.acquireLock(); err != nil {
		return nil, err
	}
	if err := store.loadOrInitialize(createdParent); err != nil {
		if closeErr := store.Close(); closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		return nil, err
	}
	return store, nil
}

func prepareNeoOrbLifecycleDirectory(parent, storePath string) (bool, error) {
	info, err := os.Lstat(parent)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(parent, 0o700); err != nil {
			return false, fmt.Errorf("create orb lifecycle directory: %w", err)
		}
		created = true
		info, err = os.Lstat(parent)
	}
	if err != nil {
		return false, fmt.Errorf("inspect orb lifecycle directory: %w", err)
	}
	if !info.IsDir() {
		return false, errors.New("orb lifecycle directory is invalid")
	}
	_, storeErr := os.Lstat(storePath)
	initializing := errors.Is(storeErr, os.ErrNotExist)
	if storeErr != nil && !initializing {
		return false, fmt.Errorf("inspect orb lifecycle store: %w", storeErr)
	}
	if created || initializing {
		if err := os.Chmod(parent, 0o700); err != nil {
			return false, fmt.Errorf("secure orb lifecycle directory: %w", err)
		}
		info, err = os.Lstat(parent)
		if err != nil {
			return false, fmt.Errorf("inspect secured orb lifecycle directory: %w", err)
		}
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 || !neoOrbLifecycleOwnedByCurrentUser(info) {
		return false, errors.New("orb lifecycle directory is not private")
	}
	return created, nil
}

func (store *neoOrbLifecycleStore) acquireLock() error {
	lockPath := filepath.Join(filepath.Dir(store.path), neoOrbLifecycleLockFileName)
	lockFile, err := neoOpenDiffCaptureLockFile(lockPath)
	if err != nil {
		return fmt.Errorf("open orb lifecycle lock: %w", err)
	}
	closeLock := func() {
		if closeErr := lockFile.Close(); closeErr != nil {
			log.Errorf("amp orbs: lifecycle lock close failed: %v", closeErr)
		}
	}
	pathInfo, err := os.Lstat(lockPath)
	if err != nil {
		closeLock()
		return fmt.Errorf("inspect orb lifecycle lock: %w", err)
	}
	fileInfo, err := lockFile.Stat()
	if err != nil {
		closeLock()
		return fmt.Errorf("inspect open orb lifecycle lock: %w", err)
	}
	if !neoOrbLifecyclePrivateRegularFile(pathInfo, 0) || !neoOrbLifecyclePrivateRegularFile(fileInfo, 0) || !os.SameFile(pathInfo, fileInfo) {
		closeLock()
		return errors.New("orb lifecycle lock is not a private regular file")
	}
	locked, err := neoTryLockDiffCaptureFile(lockFile)
	if err != nil {
		closeLock()
		return fmt.Errorf("lock orb lifecycle store: %w", err)
	}
	if !locked {
		closeLock()
		return errors.New("orb lifecycle store is locked by another process")
	}
	currentPathInfo, err := os.Lstat(lockPath)
	if err != nil || !neoOrbLifecyclePrivateRegularFile(currentPathInfo, 0) || !os.SameFile(currentPathInfo, fileInfo) {
		if errUnlock := neoUnlockDiffCaptureFile(lockFile); errUnlock != nil {
			log.Errorf("amp orbs: lifecycle lock rollback failed: %v", errUnlock)
		}
		closeLock()
		return errors.New("orb lifecycle lock changed during acquisition")
	}
	store.lockFile = lockFile
	return nil
}

func (store *neoOrbLifecycleStore) loadOrInitialize(createdParent bool) error {
	parent := filepath.Dir(store.path)
	fileInfo, err := os.Lstat(store.path)
	if errors.Is(err, os.ErrNotExist) {
		ownerBytes := make([]byte, 16)
		if _, err := io.ReadFull(rand.Reader, ownerBytes); err != nil {
			return fmt.Errorf("generate orb lifecycle owner: %w", err)
		}
		store.state = neoOrbLifecycleState{
			Version:             neoOrbLifecycleStoreVersion,
			OwnerID:             hex.EncodeToString(ownerBytes),
			Provider:            store.provider,
			GenerationHighWater: map[string]uint64{},
			Threads:             map[string]neoOrbLifecycleThreadRecord{},
			CleanupTombstones:   map[string]neoOrbLifecycleCleanupTombstone{},
		}
		if published, err := store.persistLocked(); err != nil {
			if !published {
				return err
			}
			if errRetry := store.prepareMutationLocked(); errRetry != nil {
				return errRetry
			}
		}
		if createdParent {
			store.durabilityPath = filepath.Dir(parent)
			if err := store.syncDirectory(store.durabilityPath); err != nil {
				store.durabilityPending = true
				if errRetry := store.prepareMutationLocked(); errRetry != nil {
					return errRetry
				}
			}
			store.durabilityPath = ""
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect orb lifecycle store: %w", err)
	}
	if !neoOrbLifecyclePrivateRegularFile(fileInfo, 4<<20) || fileInfo.Size() <= 0 {
		return errors.New("orb lifecycle store is not a private regular file")
	}
	file, err := os.Open(store.path)
	if err != nil {
		return fmt.Errorf("open orb lifecycle store: %w", err)
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil || !neoOrbLifecyclePrivateRegularFile(openedInfo, 4<<20) || openedInfo.Size() <= 0 || !os.SameFile(fileInfo, openedInfo) {
		if closeErr := file.Close(); closeErr != nil {
			log.Errorf("amp orbs: lifecycle store close failed: %v", closeErr)
		}
		return errors.New("orb lifecycle store changed during open")
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, 4<<20+1))
	afterInfo, afterStatErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil || afterStatErr != nil || closeErr != nil || len(raw) > 4<<20 || !neoOrbLifecyclePrivateRegularFile(afterInfo, 4<<20) || afterInfo.Size() <= 0 || !os.SameFile(openedInfo, afterInfo) {
		return errors.New("orb lifecycle store changed during read")
	}
	state, err := decodeNeoOrbLifecycleState(raw)
	if err != nil {
		return err
	}
	if state.Provider != store.provider {
		return fmt.Errorf("orb lifecycle provider mismatch: stored %q, current %q", state.Provider, store.provider)
	}
	if err := validateNeoOrbLifecycleState(state); err != nil {
		return err
	}
	store.state = state
	return nil
}

func decodeNeoOrbLifecycleState(raw []byte) (neoOrbLifecycleState, error) {
	if err := rejectNeoLocalBrokerDuplicateJSONFields(raw); err != nil {
		return neoOrbLifecycleState{}, errors.New("orb lifecycle store contains invalid JSON")
	}
	var state neoOrbLifecycleState
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return neoOrbLifecycleState{}, errors.New("orb lifecycle store contains invalid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return neoOrbLifecycleState{}, errors.New("orb lifecycle store contains trailing JSON")
	}
	if state.Version == neoOrbLifecycleStoreV2 {
		if neoOrbLifecycleStateHasActivation(state) {
			return neoOrbLifecycleState{}, errors.New("orb lifecycle store version 2 contains activation data")
		}
		state.Version = neoOrbLifecycleStoreVersion
	}
	return state, nil
}

func (store *neoOrbLifecycleStore) persistLocked() (bool, error) {
	if err := validateNeoOrbLifecycleState(store.state); err != nil {
		return false, err
	}
	raw, err := json.MarshalIndent(store.state, "", "  ")
	if err != nil {
		return false, fmt.Errorf("encode orb lifecycle store: %w", err)
	}
	if err := store.writeAtomic(store.path, append(raw, '\n'), 0o600); err != nil {
		return false, fmt.Errorf("write orb lifecycle store: %w", err)
	}
	if err := store.syncDirectory(filepath.Dir(store.path)); err != nil {
		store.durabilityPending = true
		store.durabilityPath = filepath.Dir(store.path)
		return true, &neoOrbLifecycleDurabilityError{err: err}
	}
	store.durabilityPending = false
	store.durabilityPath = ""
	return true, nil
}

func (store *neoOrbLifecycleStore) prepareMutationLocked() error {
	if store.closed || store.lockFile == nil {
		return errors.New("orb lifecycle store is closed")
	}
	if !store.durabilityPending {
		return nil
	}
	if err := store.syncDirectory(store.durabilityPath); err != nil {
		return &neoOrbLifecycleDurabilityError{err: err}
	}
	store.durabilityPending = false
	store.durabilityPath = ""
	return nil
}

func (store *neoOrbLifecycleStore) reserveGeneration(threadID, portalToken string, multiplayerTTL ...int) (neoOrbLifecycleGeneration, error) {
	if !neoThreadIDExactPattern.MatchString(threadID) || len(threadID) > 180 {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle thread is invalid")
	}
	if !neoOrbLifecycleSafeValue(portalToken, 256) {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle portal token is invalid")
	}
	multiplayerTTLSeconds := 0
	if len(multiplayerTTL) > 1 {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle multiplayer TTL is invalid")
	}
	if len(multiplayerTTL) == 1 {
		multiplayerTTLSeconds = multiplayerTTL[0]
		if multiplayerTTLSeconds != 0 && (multiplayerTTLSeconds < neoThreadOpenTTLMinSeconds || multiplayerTTLSeconds > neoThreadOpenTTLMaxSeconds) {
			return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle multiplayer TTL is invalid")
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	if _, exists := store.state.CleanupTombstones[threadID]; exists {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle cleanup is pending")
	}
	record, exists := store.state.Threads[threadID]
	if !exists {
		record = neoOrbLifecycleThreadRecord{ThreadID: threadID, Retained: []neoOrbLifecycleGeneration{}}
	}
	if record.Pending != nil {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle generation is already pending")
	}
	maximum := store.state.GenerationHighWater[threadID]
	if maximum >= 999999 {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle generation limit reached")
	}
	generation := maximum + 1
	containerName, homeVolumeName, rootVolumeName := neoOrbLifecycleResourceNames(store.state.OwnerID, threadID, generation)
	pending := neoOrbLifecycleGeneration{
		ThreadID:              threadID,
		Generation:            generation,
		ContainerName:         containerName,
		PortalToken:           portalToken,
		HomeVolumeName:        homeVolumeName,
		RootVolumeName:        rootVolumeName,
		Phase:                 neoOrbLifecyclePhasePending,
		CreatedAt:             store.now().UTC().Format(time.RFC3339Nano),
		MultiplayerTTLSeconds: multiplayerTTLSeconds,
	}
	previous := cloneNeoOrbLifecycleState(store.state)
	record.Pending = &pending
	store.state.Threads[threadID] = record
	store.state.GenerationHighWater[threadID] = generation
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
			return neoOrbLifecycleGeneration{}, err
		}
		return pending, err
	}
	return pending, nil
}

func (store *neoOrbLifecycleStore) promoteGeneration(generation neoOrbLifecycleGeneration) error {
	_, err := store.promoteGenerationWithBinding(generation, "")
	return err
}

func (store *neoOrbLifecycleStore) promoteGenerationBound(generation neoOrbLifecycleGeneration, authenticatedOwnerID string) (neoOrbLifecycleGeneration, error) {
	return store.promoteGenerationWithBinding(generation, authenticatedOwnerID)
}

func (store *neoOrbLifecycleStore) promoteGenerationWithBinding(generation neoOrbLifecycleGeneration, authenticatedOwnerID string) (neoOrbLifecycleGeneration, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	record, exists := store.state.Threads[generation.ThreadID]
	if !exists || record.Pending == nil || !neoOrbLifecyclePendingMatches(*record.Pending, generation) {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle pending generation does not match")
	}
	if !neoOrbDockerResourceNameValid(generation.ContainerID) {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle container identifier is invalid")
	}
	authenticatedOwnerID = strings.TrimSpace(authenticatedOwnerID)
	if authenticatedOwnerID != "" && !neoOrbLifecycleSafeValue(authenticatedOwnerID, 256) {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle authenticated owner is invalid")
	}
	previous := cloneNeoOrbLifecycleState(store.state)
	if record.Active != nil {
		retained := *record.Active
		retained.Phase = neoOrbLifecyclePhaseRetained
		record.Retained = append(record.Retained, retained)
	}
	active := generation
	active.Phase = neoOrbLifecyclePhaseActive
	if authenticatedOwnerID != "" {
		active.AuthenticatedOwnerID = authenticatedOwnerID
		active.Revision = active.Generation
		active.ActivationState = neoOrbLifecycleActivationBinding
		active.OperationID = ""
		active.OperationKind = ""
	}
	record.Active = &active
	record.Pending = nil
	store.state.Threads[generation.ThreadID] = record
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
		}
		return active, err
	}
	return active, nil
}

func (store *neoOrbLifecycleStore) beginActivation(expected neoOrbLifecycleGeneration, operationID, operationKind string) (neoOrbLifecycleGeneration, error) {
	if !neoOrbLifecycleSafeValue(operationID, 128) || !neoOrbLifecycleOperationKindValid(operationKind) {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle activation operation is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	record, err := store.exactActivatableRecordLocked(expected)
	if err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	validTransition := operationKind == neoOrbLifecycleOperationStart && expected.ActivationState == neoOrbLifecycleActivationBinding && expected.OperationID == "" && expected.OperationKind == "" ||
		operationKind == neoOrbLifecycleOperationUnpause && expected.ActivationState == neoOrbLifecycleActivationActionable && expected.OperationID == "" && expected.OperationKind == "" ||
		operationKind == neoOrbLifecycleOperationExecDetached && expected.ActivationState == neoOrbLifecycleActivationLaunching && (expected.OperationKind == neoOrbLifecycleOperationStart || expected.OperationKind == neoOrbLifecycleOperationUnpause) && neoOrbLifecycleSafeValue(expected.OperationID, 128)
	if !validTransition {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle activation operation does not match")
	}
	candidate := expected
	candidate.ActivationState = neoOrbLifecycleActivationLaunching
	candidate.OperationID = operationID
	candidate.OperationKind = operationKind
	return store.persistActiveActivationLocked(record, expected, candidate)
}

func (store *neoOrbLifecycleStore) finishActivation(expected neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	record, err := store.exactActivatableRecordLocked(expected)
	if err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	if expected.ActivationState != neoOrbLifecycleActivationLaunching || !neoOrbLifecycleSafeValue(expected.OperationID, 128) || expected.OperationKind != neoOrbLifecycleOperationExecDetached && expected.OperationKind != neoOrbLifecycleOperationUnpause {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle activation is not launchable")
	}
	candidate := expected
	candidate.ActivationState = neoOrbLifecycleActivationActionable
	candidate.OperationID = ""
	candidate.OperationKind = ""
	return store.persistActiveActivationLocked(record, expected, candidate)
}

func (store *neoOrbLifecycleStore) abortUninvokedActivation(expected neoOrbLifecycleGeneration, prior neoOrbLifecycleActivation) (neoOrbLifecycleGeneration, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	record, err := store.exactActivatableRecordLocked(expected)
	if err != nil {
		return neoOrbLifecycleGeneration{}, err
	}
	if expected.ActivationState != neoOrbLifecycleActivationLaunching || !neoOrbLifecycleSafeValue(expected.OperationID, 128) || !neoOrbLifecycleOperationKindValid(expected.OperationKind) {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle activation is not abortable")
	}
	validPrior := expected.OperationKind == neoOrbLifecycleOperationStart && prior.State == neoOrbLifecycleActivationBinding && prior.OperationID == "" && prior.OperationKind == "" ||
		expected.OperationKind == neoOrbLifecycleOperationUnpause && prior.State == neoOrbLifecycleActivationActionable && prior.OperationID == "" && prior.OperationKind == "" ||
		expected.OperationKind == neoOrbLifecycleOperationExecDetached && prior.State == neoOrbLifecycleActivationLaunching && neoOrbLifecycleSafeValue(prior.OperationID, 128) && (prior.OperationKind == neoOrbLifecycleOperationStart || prior.OperationKind == neoOrbLifecycleOperationUnpause)
	if !validPrior {
		return neoOrbLifecycleGeneration{}, errors.New("orb lifecycle prior activation is invalid")
	}
	candidate := expected
	candidate.ActivationState = prior.State
	candidate.OperationID = prior.OperationID
	candidate.OperationKind = prior.OperationKind
	return store.persistActiveActivationLocked(record, expected, candidate)
}

func (store *neoOrbLifecycleStore) exactActivatableRecordLocked(expected neoOrbLifecycleGeneration) (neoOrbLifecycleThreadRecord, error) {
	if _, exists := store.state.CleanupTombstones[expected.ThreadID]; exists {
		return neoOrbLifecycleThreadRecord{}, errors.New("orb lifecycle cleanup is pending")
	}
	record, exists := store.state.Threads[expected.ThreadID]
	if !exists || record.Active == nil || *record.Active != expected {
		return neoOrbLifecycleThreadRecord{}, errors.New("orb lifecycle active generation does not match")
	}
	if record.Pending != nil || expected.Phase != neoOrbLifecyclePhaseActive || expected.AuthenticatedOwnerID == "" || expected.Revision == 0 || !neoOrbPortalTokenValid(expected.PortalToken) {
		return neoOrbLifecycleThreadRecord{}, errors.New("orb lifecycle active binding is not exact")
	}
	return record, nil
}

func (store *neoOrbLifecycleStore) persistActiveActivationLocked(record neoOrbLifecycleThreadRecord, expected, candidate neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, error) {
	previous := cloneNeoOrbLifecycleState(store.state)
	record.Active = &candidate
	store.state.Threads[expected.ThreadID] = record
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
		}
		return candidate, err
	}
	return candidate, nil
}

func (store *neoOrbLifecycleStore) replaceActiveContainerID(expectedActive neoOrbLifecycleGeneration, newContainerID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return err
	}
	if _, exists := store.state.CleanupTombstones[expectedActive.ThreadID]; exists {
		return errors.New("orb lifecycle cleanup is pending")
	}
	record, exists := store.state.Threads[expectedActive.ThreadID]
	if !exists || record.Active == nil || *record.Active != expectedActive {
		return errors.New("orb lifecycle active generation does not match")
	}
	if record.Active.Phase != neoOrbLifecyclePhaseActive || record.Pending != nil {
		return errors.New("orb lifecycle active generation is not replaceable")
	}
	if neoOrbLifecycleGenerationHasActivation(expectedActive) {
		return errors.New("bound orb lifecycle active generation is not replaceable")
	}
	if !neoOrbDockerResourceNameValid(newContainerID) || newContainerID == expectedActive.ContainerID {
		return errors.New("orb lifecycle replacement container identifier is invalid")
	}
	candidate := cloneNeoOrbLifecycleState(store.state)
	candidateRecord := candidate.Threads[expectedActive.ThreadID]
	candidateActive := *candidateRecord.Active
	candidateActive.ContainerID = newContainerID
	candidateRecord.Active = &candidateActive
	candidate.Threads[expectedActive.ThreadID] = candidateRecord
	if err := validateNeoOrbLifecycleState(candidate); err != nil {
		return err
	}
	previous := store.state
	store.state = candidate
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
		}
		return err
	}
	return nil
}

func (store *neoOrbLifecycleStore) clearPendingGeneration(generation neoOrbLifecycleGeneration) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return err
	}
	record, exists := store.state.Threads[generation.ThreadID]
	if !exists || record.Pending == nil || !reflect.DeepEqual(*record.Pending, generation) {
		return errors.New("orb lifecycle pending generation does not match")
	}
	previous := cloneNeoOrbLifecycleState(store.state)
	record.Pending = nil
	if record.Active == nil && len(record.Retained) == 0 {
		delete(store.state.Threads, generation.ThreadID)
	} else {
		store.state.Threads[generation.ThreadID] = record
	}
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
		}
		return err
	}
	return nil
}

func (store *neoOrbLifecycleStore) beginCleanup(threadID string) (neoOrbLifecycleCleanupTombstone, bool, error) {
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return neoOrbLifecycleCleanupTombstone{}, false, errors.New("orb lifecycle thread is invalid")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return neoOrbLifecycleCleanupTombstone{}, false, err
	}
	if tombstone, exists := store.state.CleanupTombstones[threadID]; exists {
		return cloneNeoOrbLifecycleCleanupTombstone(tombstone), true, nil
	}
	record, exists := store.state.Threads[threadID]
	if !exists {
		return neoOrbLifecycleCleanupTombstone{}, false, nil
	}
	tombstone := neoOrbLifecycleCleanupTombstone{
		ThreadID: threadID,
		Active:   cloneNeoOrbLifecycleGeneration(record.Active),
		Pending:  cloneNeoOrbLifecycleGeneration(record.Pending),
		Retained: cloneNeoOrbLifecycleGenerations(record.Retained),
	}
	previous := cloneNeoOrbLifecycleState(store.state)
	store.state.CleanupTombstones[threadID] = tombstone
	delete(store.state.Threads, threadID)
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
			return neoOrbLifecycleCleanupTombstone{}, false, err
		}
		return cloneNeoOrbLifecycleCleanupTombstone(tombstone), true, err
	}
	return cloneNeoOrbLifecycleCleanupTombstone(tombstone), true, nil
}

func (store *neoOrbLifecycleStore) completeCleanup(tombstone neoOrbLifecycleCleanupTombstone) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.prepareMutationLocked(); err != nil {
		return err
	}
	stored, exists := store.state.CleanupTombstones[tombstone.ThreadID]
	if !exists || !reflect.DeepEqual(stored, tombstone) {
		return errors.New("orb lifecycle cleanup tombstone does not match")
	}
	previous := cloneNeoOrbLifecycleState(store.state)
	delete(store.state.CleanupTombstones, tombstone.ThreadID)
	published, err := store.persistLocked()
	if err != nil {
		if !published {
			store.state = previous
		}
		return err
	}
	return nil
}

func (store *neoOrbLifecycleStore) snapshot() neoOrbLifecycleState {
	store.mu.Lock()
	defer store.mu.Unlock()
	return cloneNeoOrbLifecycleState(store.state)
}

func (store *neoOrbLifecycleStore) ownerID() string {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.state.OwnerID
}

func (store *neoOrbLifecycleStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	if store.closed {
		store.mu.Unlock()
		return nil
	}
	var durabilityErr error
	if store.durabilityPending {
		if err := store.syncDirectory(store.durabilityPath); err != nil {
			durabilityErr = &neoOrbLifecycleDurabilityError{err: err}
		} else {
			store.durabilityPending = false
			store.durabilityPath = ""
		}
	}
	store.closed = true
	lockFile := store.lockFile
	store.lockFile = nil
	store.mu.Unlock()
	if lockFile == nil {
		return durabilityErr
	}
	errUnlock := neoUnlockDiffCaptureFile(lockFile)
	errClose := lockFile.Close()
	return errors.Join(durabilityErr, errUnlock, errClose)
}

func neoOrbLifecycleOwnedByCurrentUser(info os.FileInfo) bool {
	return neoOrbFileOwnedByCurrentUser(info)
}

func neoOrbLifecyclePrivateRegularFile(info os.FileInfo, maximumSize int64) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !neoOrbLifecycleOwnedByCurrentUser(info) {
		return false
	}
	if !neoOrbFileHasSingleLink(info) {
		return false
	}
	return maximumSize <= 0 || info.Size() <= maximumSize
}

func neoOrbLifecycleResourceNames(ownerID, threadID string, generation uint64) (string, string, string) {
	base := fmt.Sprintf("cliproxy-orb-%s-%s-g%06d", ownerID[:12], strings.ToLower(threadID), generation)
	return base, base + "-home", base + "-root"
}

func neoOrbLifecycleLabels(ownerID string, generation neoOrbLifecycleGeneration, role string) map[string]string {
	return map[string]string{
		"cliproxy.orb":            generation.ThreadID,
		"cliproxy.orb.owner":      ownerID,
		"cliproxy.orb.lifecycle":  "1",
		"cliproxy.orb.generation": strconv.FormatUint(generation.Generation, 10),
		"cliproxy.orb.role":       role,
		"cliproxy.orb.created-at": generation.CreatedAt,
	}
}

func buildNeoOrbLifecycleContainerSpec(ownerID string, generation neoOrbLifecycleGeneration, image, network string, nanoCPUs, memoryMB int64) neoOrbContainerSpec {
	return neoOrbContainerSpec{
		Name:          generation.ContainerName,
		Image:         image,
		Cmd:           []string{"sleep", "infinity"},
		NanoCPUs:      nanoCPUs,
		MemoryMB:      memoryMB,
		ExtraHosts:    []string{"host.docker.internal:host-gateway"},
		Labels:        neoOrbLifecycleLabels(ownerID, generation, neoOrbLifecycleRoleContainer),
		NetworkMode:   network,
		VolumeMounts:  []neoOrbVolumeMount{{Name: generation.HomeVolumeName, Destination: "/home/user", NoCopy: true}, {Name: generation.RootVolumeName, Destination: "/root", NoCopy: true}},
		RestartPolicy: "unless-stopped",
	}
}

func neoOrbLifecycleVolumeName(generation neoOrbLifecycleGeneration, role string) (string, bool) {
	switch role {
	case neoOrbLifecycleRoleHome:
		return generation.HomeVolumeName, true
	case neoOrbLifecycleRoleRoot:
		return generation.RootVolumeName, true
	default:
		return "", false
	}
}

func neoOrbLifecycleVolumeMatches(state neoOrbVolumeState, ownerID string, generation neoOrbLifecycleGeneration, role string) bool {
	name, ok := neoOrbLifecycleVolumeName(generation, role)
	return ok && state.Exists && state.Name == name && state.Driver == "local" && state.Scope == "local" && len(state.Options) == 0 && reflect.DeepEqual(state.Labels, neoOrbLifecycleLabels(ownerID, generation, role))
}

func neoOrbLifecycleContainerMatches(containerID string, state neoOrbContainerState, homeVolume, rootVolume neoOrbVolumeState, ownerID string, generation neoOrbLifecycleGeneration) bool {
	if !state.Exists || state.ID != containerID || containerID != generation.ContainerID || state.Name != "/"+generation.ContainerName || state.RestartPolicy != "unless-stopped" || !reflect.DeepEqual(state.Labels, neoOrbLifecycleLabels(ownerID, generation, neoOrbLifecycleRoleContainer)) || len(state.Mounts) != 2 || !neoOrbLifecycleVolumeMatches(homeVolume, ownerID, generation, neoOrbLifecycleRoleHome) || !neoOrbLifecycleVolumeMatches(rootVolume, ownerID, generation, neoOrbLifecycleRoleRoot) || homeVolume.Mountpoint == "" || rootVolume.Mountpoint == "" {
		return false
	}
	expectedMounts := map[string]neoOrbContainerMount{
		"/home/user": {Type: "volume", Source: homeVolume.Mountpoint, Name: generation.HomeVolumeName, Destination: "/home/user"},
		"/root":      {Type: "volume", Source: rootVolume.Mountpoint, Name: generation.RootVolumeName, Destination: "/root"},
	}
	for _, mount := range state.Mounts {
		expected, ok := expectedMounts[mount.Destination]
		if !ok || mount != expected {
			return false
		}
		delete(expectedMounts, mount.Destination)
	}
	return len(expectedMounts) == 0
}

func neoOrbCreateLifecycleVolume(ctx context.Context, client neoOrbProviderClient, ownerID string, generation neoOrbLifecycleGeneration, role string) (neoOrbVolumeState, error) {
	name, ok := neoOrbLifecycleVolumeName(generation, role)
	if client == nil || !ok {
		return neoOrbVolumeState{}, errors.New("orb lifecycle volume request is invalid")
	}
	labels := neoOrbLifecycleLabels(ownerID, generation, role)
	if _, err := client.CreateVolume(ctx, neoOrbVolumeSpec{Name: name, Labels: labels}); err != nil {
		return neoOrbVolumeState{}, fmt.Errorf("create orb lifecycle volume: %w", err)
	}
	state, err := client.InspectVolume(ctx, name)
	if err != nil {
		return neoOrbVolumeState{}, fmt.Errorf("inspect created orb lifecycle volume: %w", err)
	}
	if !neoOrbLifecycleVolumeMatches(state, ownerID, generation, role) {
		return neoOrbVolumeState{}, errors.New("created orb lifecycle volume ownership does not match")
	}
	return state, nil
}

func validateNeoOrbLifecycleState(state neoOrbLifecycleState) error {
	if state.Version != neoOrbLifecycleStoreVersion {
		return fmt.Errorf("orb lifecycle store version %d is unsupported", state.Version)
	}
	if !neoOrbLifecycleOwnerIDValid(state.OwnerID) {
		return errors.New("orb lifecycle owner is invalid")
	}
	normalizedProvider, err := resolveNeoOrbDockerEndpoint(state.Provider)
	if err != nil || normalizedProvider != state.Provider {
		return errors.New("orb lifecycle provider is invalid")
	}
	if state.GenerationHighWater == nil || state.Threads == nil || state.CleanupTombstones == nil {
		return errors.New("orb lifecycle records are invalid")
	}
	for threadID, generation := range state.GenerationHighWater {
		if !neoThreadIDExactPattern.MatchString(threadID) || len(threadID) > 180 || generation == 0 || generation > 999999 {
			return errors.New("orb lifecycle generation high-water mark is invalid")
		}
	}
	resourceNames := map[string]bool{}
	containerIDs := map[string]bool{}
	validateGeneration := func(generation neoOrbLifecycleGeneration, expectedThreadID, expectedPhase string) error {
		if generation.ThreadID != expectedThreadID || generation.Generation == 0 || generation.Generation > 999999 || generation.Phase != expectedPhase {
			return errors.New("orb lifecycle generation is inconsistent")
		}
		if !neoOrbLifecycleSafeValue(generation.PortalToken, 256) {
			return errors.New("orb lifecycle portal token is invalid")
		}
		if generation.MultiplayerTTLSeconds != 0 && (generation.MultiplayerTTLSeconds < neoThreadOpenTTLMinSeconds || generation.MultiplayerTTLSeconds > neoThreadOpenTTLMaxSeconds) {
			return errors.New("orb lifecycle multiplayer TTL is invalid")
		}
		createdAt, err := time.Parse(time.RFC3339Nano, generation.CreatedAt)
		if err != nil || createdAt.UTC().Format(time.RFC3339Nano) != generation.CreatedAt {
			return errors.New("orb lifecycle creation timestamp is invalid")
		}
		containerName, homeVolumeName, rootVolumeName := neoOrbLifecycleResourceNames(state.OwnerID, generation.ThreadID, generation.Generation)
		if generation.ContainerName != containerName || generation.HomeVolumeName != homeVolumeName || generation.RootVolumeName != rootVolumeName {
			return errors.New("orb lifecycle resource names are inconsistent")
		}
		for _, name := range []string{generation.ContainerName, generation.HomeVolumeName, generation.RootVolumeName} {
			if resourceNames[name] {
				return errors.New("orb lifecycle resource name is duplicated")
			}
			resourceNames[name] = true
		}
		if expectedPhase == neoOrbLifecyclePhasePending {
			if generation.ContainerID != "" || neoOrbLifecycleGenerationHasActivation(generation) {
				return errors.New("orb lifecycle pending container identifier is invalid")
			}
		} else {
			if !neoOrbDockerResourceNameValid(generation.ContainerID) || containerIDs[generation.ContainerID] {
				return errors.New("orb lifecycle container identifier is invalid")
			}
			containerIDs[generation.ContainerID] = true
			if err := validateNeoOrbLifecycleActivation(generation); err != nil {
				return err
			}
		}
		return nil
	}
	validateResources := func(threadID string, active, pending *neoOrbLifecycleGeneration, retained []neoOrbLifecycleGeneration) error {
		if !neoThreadIDExactPattern.MatchString(threadID) || len(threadID) > 180 {
			return errors.New("orb lifecycle thread is invalid")
		}
		highWater := state.GenerationHighWater[threadID]
		if highWater == 0 {
			return errors.New("orb lifecycle generation high-water mark is missing")
		}
		seenGenerations := map[uint64]bool{}
		if active != nil {
			if active.Generation > highWater {
				return errors.New("orb lifecycle generation exceeds high-water mark")
			}
			if err := validateGeneration(*active, threadID, neoOrbLifecyclePhaseActive); err != nil {
				return err
			}
			seenGenerations[active.Generation] = true
		}
		if pending != nil {
			if pending.Generation > highWater {
				return errors.New("orb lifecycle generation exceeds high-water mark")
			}
			if err := validateGeneration(*pending, threadID, neoOrbLifecyclePhasePending); err != nil {
				return err
			}
			if seenGenerations[pending.Generation] {
				return errors.New("orb lifecycle generation is duplicated")
			}
			seenGenerations[pending.Generation] = true
		}
		if retained == nil {
			return errors.New("orb lifecycle retained generations are invalid")
		}
		for _, generation := range retained {
			if generation.Generation > highWater {
				return errors.New("orb lifecycle generation exceeds high-water mark")
			}
			if err := validateGeneration(generation, threadID, neoOrbLifecyclePhaseRetained); err != nil {
				return err
			}
			if seenGenerations[generation.Generation] {
				return errors.New("orb lifecycle generation is duplicated")
			}
			seenGenerations[generation.Generation] = true
		}
		if active == nil && pending == nil && len(retained) == 0 {
			return errors.New("orb lifecycle thread record is empty")
		}
		return nil
	}
	for threadID, record := range state.Threads {
		if record.ThreadID != threadID {
			return errors.New("orb lifecycle thread record is inconsistent")
		}
		if _, cleanupPending := state.CleanupTombstones[threadID]; cleanupPending {
			return errors.New("orb lifecycle thread and cleanup records overlap")
		}
		if err := validateResources(threadID, record.Active, record.Pending, record.Retained); err != nil {
			return err
		}
	}
	for threadID, tombstone := range state.CleanupTombstones {
		if tombstone.ThreadID != threadID {
			return errors.New("orb lifecycle cleanup record is inconsistent")
		}
		if err := validateResources(threadID, tombstone.Active, tombstone.Pending, tombstone.Retained); err != nil {
			return err
		}
	}
	return nil
}

func neoOrbLifecyclePendingMatches(pending, candidate neoOrbLifecycleGeneration) bool {
	return pending.ThreadID == candidate.ThreadID &&
		pending.Generation == candidate.Generation &&
		pending.ContainerID == "" &&
		candidate.ContainerID != "" &&
		pending.ContainerName == candidate.ContainerName &&
		pending.PortalToken == candidate.PortalToken &&
		pending.HomeVolumeName == candidate.HomeVolumeName &&
		pending.RootVolumeName == candidate.RootVolumeName &&
		pending.MultiplayerTTLSeconds == candidate.MultiplayerTTLSeconds &&
		pending.Phase == candidate.Phase &&
		pending.CreatedAt == candidate.CreatedAt
}

func cloneNeoOrbLifecycleState(state neoOrbLifecycleState) neoOrbLifecycleState {
	cloned := state
	cloned.GenerationHighWater = make(map[string]uint64, len(state.GenerationHighWater))
	for threadID, generation := range state.GenerationHighWater {
		cloned.GenerationHighWater[threadID] = generation
	}
	cloned.Threads = make(map[string]neoOrbLifecycleThreadRecord, len(state.Threads))
	for threadID, record := range state.Threads {
		record.Active = cloneNeoOrbLifecycleGeneration(record.Active)
		record.Pending = cloneNeoOrbLifecycleGeneration(record.Pending)
		record.Retained = cloneNeoOrbLifecycleGenerations(record.Retained)
		cloned.Threads[threadID] = record
	}
	cloned.CleanupTombstones = make(map[string]neoOrbLifecycleCleanupTombstone, len(state.CleanupTombstones))
	for threadID, tombstone := range state.CleanupTombstones {
		tombstone.Active = cloneNeoOrbLifecycleGeneration(tombstone.Active)
		tombstone.Pending = cloneNeoOrbLifecycleGeneration(tombstone.Pending)
		tombstone.Retained = cloneNeoOrbLifecycleGenerations(tombstone.Retained)
		cloned.CleanupTombstones[threadID] = tombstone
	}
	return cloned
}

func cloneNeoOrbLifecycleCleanupTombstone(tombstone neoOrbLifecycleCleanupTombstone) neoOrbLifecycleCleanupTombstone {
	tombstone.Active = cloneNeoOrbLifecycleGeneration(tombstone.Active)
	tombstone.Pending = cloneNeoOrbLifecycleGeneration(tombstone.Pending)
	tombstone.Retained = cloneNeoOrbLifecycleGenerations(tombstone.Retained)
	return tombstone
}

func cloneNeoOrbLifecycleGenerations(generations []neoOrbLifecycleGeneration) []neoOrbLifecycleGeneration {
	if generations == nil {
		return nil
	}
	return append(make([]neoOrbLifecycleGeneration, 0, len(generations)), generations...)
}

func cloneNeoOrbLifecycleGeneration(generation *neoOrbLifecycleGeneration) *neoOrbLifecycleGeneration {
	if generation == nil {
		return nil
	}
	cloned := *generation
	return &cloned
}

func neoOrbLifecycleOwnerIDValid(ownerID string) bool {
	if len(ownerID) != 32 {
		return false
	}
	for _, char := range ownerID {
		if char < '0' || char > '9' && char < 'a' || char > 'f' {
			return false
		}
	}
	return true
}

func neoOrbLifecycleSafeValue(value string, limit int) bool {
	if value == "" || len(value) > limit || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func neoOrbLifecycleOperationKindValid(kind string) bool {
	return kind == neoOrbLifecycleOperationStart || kind == neoOrbLifecycleOperationUnpause || kind == neoOrbLifecycleOperationExecDetached
}

func neoOrbLifecycleGenerationHasActivation(generation neoOrbLifecycleGeneration) bool {
	return generation.AuthenticatedOwnerID != "" || generation.Revision != 0 || generation.ActivationState != "" || generation.OperationID != "" || generation.OperationKind != ""
}

func neoOrbLifecycleStateHasActivation(state neoOrbLifecycleState) bool {
	has := func(active, pending *neoOrbLifecycleGeneration, retained []neoOrbLifecycleGeneration) bool {
		if active != nil && neoOrbLifecycleGenerationHasActivation(*active) || pending != nil && neoOrbLifecycleGenerationHasActivation(*pending) {
			return true
		}
		for _, generation := range retained {
			if neoOrbLifecycleGenerationHasActivation(generation) {
				return true
			}
		}
		return false
	}
	for _, record := range state.Threads {
		if has(record.Active, record.Pending, record.Retained) {
			return true
		}
	}
	for _, record := range state.CleanupTombstones {
		if has(record.Active, record.Pending, record.Retained) {
			return true
		}
	}
	return false
}

func validateNeoOrbLifecycleActivation(generation neoOrbLifecycleGeneration) error {
	if !neoOrbLifecycleGenerationHasActivation(generation) {
		return nil
	}
	if !neoOrbLifecycleSafeValue(generation.AuthenticatedOwnerID, 256) || generation.Revision == 0 || generation.Revision != generation.Generation || !neoOrbPortalTokenValid(generation.PortalToken) {
		return errors.New("orb lifecycle authenticated binding is invalid")
	}
	switch generation.ActivationState {
	case neoOrbLifecycleActivationBinding, neoOrbLifecycleActivationActionable:
		if generation.OperationID != "" || generation.OperationKind != "" {
			return errors.New("orb lifecycle activation operation is invalid")
		}
	case neoOrbLifecycleActivationLaunching:
		if !neoOrbLifecycleSafeValue(generation.OperationID, 128) || !neoOrbLifecycleOperationKindValid(generation.OperationKind) {
			return errors.New("orb lifecycle activation operation is invalid")
		}
	default:
		return errors.New("orb lifecycle activation state is invalid")
	}
	return nil
}

func neoOrbLifecycleActionable(generation *neoOrbLifecycleGeneration) bool {
	return generation != nil && generation.Phase == neoOrbLifecyclePhaseActive && generation.ActivationState == neoOrbLifecycleActivationActionable && generation.AuthenticatedOwnerID != "" && generation.Revision != 0 && generation.OperationID == "" && generation.OperationKind == "" && validateNeoOrbLifecycleActivation(*generation) == nil
}
