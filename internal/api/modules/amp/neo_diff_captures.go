package amp

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

const (
	neoDiffCaptureAllocationMaxBytes   = 64 * 1024
	neoDiffCapturePublishMaxBytes      = 8 * 1024 * 1024
	neoDiffCaptureTextBlobMaxBytes     = 4 * 1024 * 1024
	neoDiffCaptureDeltaMaxBytes        = 4 * 1024 * 1024
	neoDiffCaptureBlobIndexMaxBytes    = 8 * 1024 * 1024
	neoDiffCaptureBlobIndexMaxThreads  = 64
	neoDiffCaptureBlobIndexCacheBytes  = 32 * 1024 * 1024
	neoDiffCaptureGitArgsMaxBytes      = 64 * 1024
	neoDiffCaptureLatestMaxBytes       = 4 * 1024
	neoDiffCapturePendingMaxBytes      = 4 * 1024
	neoDiffCapturePendingPruneMaxBytes = 128 * 1024
	neoDiffCaptureRecordMaxBytes       = neoDiffCapturePublishMaxBytes + 64*1024
	neoDiffCaptureScanMaxRecords       = 1024
	neoDiffCaptureScanMaxBytes         = 32 * 1024 * 1024
	neoDiffCaptureManifestMaxFileSets  = 256
	neoDiffCaptureManifestMaxFiles     = 10000
	neoDiffCaptureManifestMaxCommits   = 10000
	neoDiffCaptureManifestMaxPathBytes = 4 * 1024 * 1024
	neoDiffCaptureRevisionMaxBytes     = 4 * 1024 * 1024
	neoDiffCaptureRetainedPublished    = 4
	neoDiffCaptureRetainedAllocations  = 4
	neoDiffCaptureAllocationTTL        = time.Hour
	neoDiffCaptureLockStaleAfter       = time.Minute
	neoDiffCaptureLockHeartbeat        = neoDiffCaptureLockStaleAfter / 3
	neoDiffCaptureReceiveMaxBytes      = 64 * 1024 * 1024
	neoDiffCaptureRepositoryMaxBytes   = 512 * 1024 * 1024
	neoDiffCaptureMaxTreeQueries       = 32
	neoDiffCaptureMaxRangeQueries      = 32
)

var (
	neoDiffCaptureIDPattern        = regexp.MustCompile(`^[0-9A-Za-z]{16,64}$`)
	neoDiffCaptureSHApattern       = regexp.MustCompile(`^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$`)
	errNeoDiffCaptureReceiveActive = errors.New("diff capture receive is active")
	errNeoDiffCaptureStorageLimit  = errors.New("diff capture storage exceeds local limit")
	neoDiffCaptureLocks            neoDiffCaptureLockRegistry
	neoDiffCaptureBlobIndexes      neoDiffCaptureBlobIndexCache
	neoDiffCaptureReceiveSequence  atomic.Uint64
)

type neoDiffCaptureRoute struct {
	threadID  string
	captureID string
	sha       string
	action    string
	valid     bool
}

type neoDiffCaptureRecord struct {
	StorageVersion int             `json:"storageVersion"`
	CaptureID      string          `json:"captureID"`
	CaptureRef     string          `json:"captureRef"`
	PublishedRef   string          `json:"publishedRef,omitempty"`
	CreatedAt      string          `json:"createdAt"`
	PublishedAt    string          `json:"publishedAt,omitempty"`
	State          string          `json:"state"`
	ManifestSHA256 string          `json:"manifestSHA256,omitempty"`
	Manifest       json.RawMessage `json:"manifest,omitempty"`
}

type neoDiffCaptureLatestRecord struct {
	StorageVersion int    `json:"storageVersion"`
	CaptureID      string `json:"captureID"`
	PublishedAt    string `json:"publishedAt"`
	ManifestSHA256 string `json:"manifestSHA256"`
}

type neoDiffCapturePendingPublication struct {
	StorageVersion int    `json:"storageVersion"`
	CaptureID      string `json:"captureID"`
}

type neoDiffCapturePendingPrune struct {
	StorageVersion int      `json:"storageVersion"`
	CaptureIDs     []string `json:"captureIDs"`
}

type neoDiffCaptureRetentionRecord struct {
	record    neoDiffCaptureRecord
	timestamp time.Time
}

type neoDiffCaptureLockRegistry struct {
	mu      sync.Mutex
	entries map[string]*neoDiffCaptureLockEntry
}

type neoDiffCaptureLockEntry struct {
	token chan struct{}
	refs  int
}

type neoDiffCaptureBlobIndexCache struct {
	marshalMu  sync.Mutex
	mu         sync.Mutex
	entries    map[string]neoDiffCaptureBlobIndexCacheEntry
	clock      uint64
	totalBytes int
}

type neoDiffCaptureBlobIndexCacheEntry struct {
	index *neoDiffCaptureBlobIndex
	used  uint64
	bytes int
}

type neoDiffCaptureBlobIndex struct {
	captures map[string]neoDiffCaptureBlobIndexEntry
	bySHA    map[string][]string
}

type neoDiffCaptureBlobIndexEntry struct {
	manifestSHA256 string
	shas           map[string]struct{}
}

type neoDiffCaptureStoredBlobIndex struct {
	StorageVersion int                                      `json:"storageVersion"`
	Captures       map[string]neoDiffCaptureStoredBlobEntry `json:"captures"`
}

type neoDiffCaptureStoredBlobEntry struct {
	ManifestSHA256 string   `json:"manifestSHA256"`
	SHAs           []string `json:"shas"`
}

type neoDiffCaptureManifest struct {
	Version    int             `json:"version"`
	CaptureID  string          `json:"captureID"`
	CapturedAt json.RawMessage `json:"capturedAt"`
	CaptureRef string          `json:"captureRef"`
	Source     struct {
		RepositoryRoot string `json:"repositoryRoot"`
		Branch         string `json:"branch"`
		HeadSHA        string `json:"headSHA"`
		MergeBaseSHA   string `json:"mergeBaseSHA"`
	} `json:"source"`
	Revisions struct {
		ProjectedHeadSHA     string `json:"projectedHeadSHA"`
		ProjectedIndexSHA    string `json:"projectedIndexSHA"`
		ProjectedWorktreeSHA string `json:"projectedWorktreeSHA"`
	} `json:"revisions"`
	ProjectedCommits []neoDiffCaptureProjectedCommit `json:"projectedCommits"`
	Sections         []neoDiffCaptureManifestSection `json:"sections"`
	RangeSections    []neoDiffCaptureRangeSection    `json:"rangeSections"`
}

type neoDiffCaptureProjectedCommit struct {
	SourceCommitSHA    string `json:"sourceCommitSHA"`
	ProjectedCommitSHA string `json:"projectedCommitSHA"`
	Subject            string `json:"subject"`
}

type neoDiffCaptureManifestSection struct {
	ID            string                       `json:"id"`
	Label         string                       `json:"label"`
	BaseCommitSHA string                       `json:"baseCommitSHA"`
	HeadCommitSHA string                       `json:"headCommitSHA"`
	Files         []neoDiffCaptureManifestFile `json:"files"`
}

type neoDiffCaptureRangeSection struct {
	RangeID string                       `json:"rangeID"`
	Label   string                       `json:"label"`
	Files   []neoDiffCaptureManifestFile `json:"files"`
}

type neoDiffCaptureManifestFile struct {
	ID           string                 `json:"id"`
	Path         string                 `json:"path"`
	PreviousPath string                 `json:"previousPath,omitempty"`
	ChangeType   string                 `json:"changeType"`
	Old          neoDiffCaptureFileSide `json:"old"`
	New          neoDiffCaptureFileSide `json:"new"`
	IsBinary     bool                   `json:"isBinary"`
	DiffStat     neoDiffCaptureDiffStat `json:"diffStat"`
	OldBlobSHA   string                 `json:"oldBlobSHA,omitempty"`
	NewBlobSHA   string                 `json:"newBlobSHA,omitempty"`
}

type neoDiffCaptureFileSide struct {
	Kind      string `json:"kind"`
	Path      string `json:"path"`
	BlobSHA   string `json:"blobSHA,omitempty"`
	SizeBytes int64  `json:"sizeBytes,omitempty"`
}

type neoDiffCaptureDiffStat struct {
	Added   int `json:"added"`
	Deleted int `json:"deleted"`
	Changed int `json:"changed"`
}

type neoGitDiffStatWriter struct {
	prefix    [16]byte
	prefixLen int
	added     int
	deleted   int
	binary    bool
	inHunk    bool
}

func (w *neoGitDiffStatWriter) Write(data []byte) (int, error) {
	for _, value := range data {
		if value == '\n' {
			w.finishLine()
			continue
		}
		if w.prefixLen < len(w.prefix) {
			w.prefix[w.prefixLen] = value
			w.prefixLen++
		}
	}
	return len(data), nil
}

func (w *neoGitDiffStatWriter) finishLine() {
	prefix := w.prefix[:w.prefixLen]
	if bytes.HasPrefix(prefix, []byte("diff --git ")) {
		w.inHunk = false
	} else if bytes.HasPrefix(prefix, []byte("Binary files ")) || bytes.Equal(prefix, []byte("GIT binary patch")) {
		w.binary = true
	} else if bytes.HasPrefix(prefix, []byte("@@")) {
		w.inHunk = true
	} else if w.inHunk && len(prefix) > 0 && prefix[0] == '+' {
		w.added++
	} else if w.inHunk && len(prefix) > 0 && prefix[0] == '-' {
		w.deleted++
	}
	w.prefixLen = 0
}

func (w *neoGitDiffStatWriter) result() map[string]any {
	if w.prefixLen > 0 {
		w.finishLine()
	}
	changed := max(w.added, w.deleted)
	if w.binary && changed == 0 {
		changed = 1
	}
	return map[string]any{"added": w.added, "deleted": w.deleted, "changed": changed}
}

type neoDiffCaptureManifestFileSet struct {
	baseSHA string
	headSHA string
	files   []neoDiffCaptureManifestFile
}

type neoDiffCapturePublished struct {
	record      neoDiffCaptureRecord
	manifest    neoDiffCaptureManifest
	publishedAt time.Time
}

type neoDiffCaptureTreeFile struct {
	mode string
	sha  string
}

type neoDiffCaptureNumStatEntry struct {
	oldPath  string
	newPath  string
	isBinary bool
	diffStat neoDiffCaptureDiffStat
}

type neoDiffCaptureNumStats struct {
	entries   []neoDiffCaptureNumStatEntry
	pathBytes int
}

type neoDiffCaptureRecordUsage struct {
	recordCount int
	recordBytes int64
	fileBytes   map[string]int64
	overLimit   bool
}

func neoDiffCaptureRequestPath(path string) (neoDiffCaptureRoute, bool) {
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(path, "/"), "/"), "/")
	if len(parts) < 4 || parts[0] != "api" || parts[1] != "threads" || parts[3] != "diff-captures" {
		return neoDiffCaptureRoute{}, false
	}
	route := neoDiffCaptureRoute{threadID: parts[2]}
	if !neoThreadIDExactPattern.MatchString(route.threadID) {
		return route, true
	}
	switch {
	case len(parts) == 4:
		route.action = "allocate"
		route.valid = true
	case len(parts) == 5 && parts[4] == "latest":
		route.action = "latest"
		route.valid = true
	case len(parts) == 5 && parts[4] == "diff":
		route.action = "diff"
		route.valid = true
	case len(parts) == 6 && parts[4] == "blob" && neoDiffCaptureSHApattern.MatchString(parts[5]):
		route.action = "blob"
		route.sha = strings.ToLower(parts[5])
		route.valid = true
	case len(parts) == 6 && neoDiffCaptureIDPattern.MatchString(parts[4]) && parts[5] == "publish":
		route.action = "publish"
		route.captureID = parts[4]
		route.valid = true
	}
	return route, true
}

func neoDiffCaptureBrowserReadPath(path string) bool {
	route, matched := neoDiffCaptureRequestPath(path)
	return matched && route.valid && (route.action == "latest" || route.action == "blob" || route.action == "diff")
}

func (m *AmpModule) tryServeNeoLocalDiffCapture(c *gin.Context) bool {
	if m == nil || m.neoRuntime == nil || c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	route, matched := neoDiffCaptureRequestPath(c.Request.URL.Path)
	if !matched {
		return false
	}
	cfg := m.neoThreadConfigSnapshot()
	if cfg == nil || !neoRuntimeEnabled(cfg) {
		return false
	}
	if !neoThreadIDExactPattern.MatchString(route.threadID) {
		c.JSON(http.StatusNotFound, gin.H{"error": "diff capture thread not found"})
		return true
	}
	if m.neoRuntime.neoLocalThreadActorForOwner(c.Request.Context(), route.threadID, false) == nil {
		if m.neoRuntime.store == nil {
			return false
		}
		actor := m.neoRuntime.store.lookupThreadActor(route.threadID)
		if actor == nil || !actor.hasLocalThreadState() {
			return false
		}
		c.JSON(http.StatusNotFound, gin.H{"error": "diff capture thread not found"})
		return true
	}
	if !route.valid {
		c.JSON(http.StatusNotFound, gin.H{"error": "diff capture route not found"})
		return true
	}
	c.Header("Cache-Control", "private, no-store")
	switch route.action {
	case "allocate":
		if !neoDiffCaptureRequireMethod(c, http.MethodPost) {
			return true
		}
		m.serveNeoDiffCaptureAllocate(c, route.threadID)
	case "publish":
		switch c.Request.Method {
		case http.MethodPost:
			m.serveNeoDiffCapturePublish(c, route.threadID, route.captureID)
		case http.MethodDelete:
			m.serveNeoDiffCaptureDelete(c, route.threadID, route.captureID)
		default:
			c.Header("Allow", http.MethodPost+", "+http.MethodDelete)
			c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "method_not_allowed"})
		}
	case "latest":
		if !neoDiffCaptureRequireMethod(c, http.MethodGet) {
			return true
		}
		m.serveNeoDiffCaptureLatest(c, route.threadID)
	case "blob":
		if !neoDiffCaptureRequireMethod(c, http.MethodGet) {
			return true
		}
		m.serveNeoDiffCaptureBlob(c, route.threadID, route.sha)
	case "diff":
		if !neoDiffCaptureRequireMethod(c, http.MethodGet) {
			return true
		}
		m.serveNeoDiffCaptureDelta(c, route.threadID)
	}
	return true
}

func neoDiffCaptureRequireMethod(c *gin.Context, method string) bool {
	if c.Request.Method == method {
		return true
	}
	c.Header("Allow", method)
	c.JSON(http.StatusMethodNotAllowed, gin.H{"error": "method_not_allowed"})
	return false
}

func (m *AmpModule) serveNeoDiffCaptureAllocate(c *gin.Context, threadID string) {
	if _, err := neoDiffCaptureReadBody(c, neoDiffCaptureAllocationMaxBytes); err != nil {
		neoDiffCaptureWriteBodyError(c, err)
		return
	}
	ctx := c.Request.Context()
	unlock, err := neoDiffCaptureLock(ctx, threadID)
	if err != nil {
		return
	}
	release := neoDiffCaptureLockRelease(unlock)
	defer release()
	if ctx.Err() != nil {
		return
	}
	objectFormat, err := m.neoDiffCaptureObjectFormat(ctx, threadID)
	if err != nil {
		logNeoDiffCaptureError("allocation object format", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
		return
	}
	repositoryDir, err := neoEnsureDiffCaptureRepository(ctx, threadID, objectFormat)
	if err != nil {
		logNeoDiffCaptureError("allocation", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
		return
	}
	receiveRelease, errReceive := neoAcquireDiffCaptureReceiveLock(threadID)
	if errReceive != nil {
		if errors.Is(errReceive, errNeoDiffCaptureReceiveActive) {
			neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture receive is active"})
		} else {
			logNeoDiffCaptureError("allocation receive lock", threadID, "", errReceive)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
		}
		return
	}
	receiveRelease = neoDiffCaptureLockRelease(receiveRelease)
	threadRelease := release
	release = neoDiffCaptureLockRelease(func() {
		receiveRelease()
		threadRelease()
	})
	defer receiveRelease()
	if err := neoPruneDiffCaptureStorage(ctx, threadID, "", false); err != nil {
		logNeoDiffCaptureError("allocation storage cleanup", threadID, "", err)
		if errors.Is(err, errNeoDiffCaptureStorageLimit) {
			neoDiffCaptureWriteJSON(c, release, http.StatusInsufficientStorage, gin.H{"error": "diff capture storage exceeds local limit"})
		} else {
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
		}
		return
	}
	usage, err := neoDiffCaptureRecordStorageUsage(threadID)
	if err != nil {
		logNeoDiffCaptureError("allocation metadata", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
		return
	}
	if usage.overLimit || usage.recordCount >= neoDiffCaptureScanMaxRecords {
		neoDiffCaptureWriteJSON(c, release, http.StatusInsufficientStorage, gin.H{"error": "diff capture storage exceeds local limit"})
		return
	}
	for attempt := 0; attempt < 4; attempt++ {
		captureID := randomBase62(24)
		captureRef := "refs/heads/amp/captures/" + captureID
		record := neoDiffCaptureRecord{
			StorageVersion: 1,
			CaptureID:      captureID,
			CaptureRef:     captureRef,
			CreatedAt:      time.Now().UTC().Format(time.RFC3339Nano),
			State:          "allocated",
		}
		raw, err := marshalNeoDiffCaptureRecord(record)
		if err != nil {
			logNeoDiffCaptureError("allocation metadata", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
			return
		}
		if usage.recordBytes+int64(len(raw)) > neoDiffCaptureScanMaxBytes {
			neoDiffCaptureWriteJSON(c, release, http.StatusInsufficientStorage, gin.H{"error": "diff capture storage exceeds local limit"})
			return
		}
		path := neoDiffCaptureRecordPath(threadID, captureID)
		if _, statErr := os.Stat(path); statErr == nil {
			continue
		} else if !errors.Is(statErr, os.ErrNotExist) {
			logNeoDiffCaptureError("allocation", threadID, captureID, statErr)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
			return
		}
		if err := writeNeoDiffCaptureRecordRaw(path, raw); err != nil {
			logNeoDiffCaptureError("allocation", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
			return
		}
		neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{
			"captureID":  captureID,
			"captureRef": captureRef,
			"gitURL":     neoDiffCaptureFileURL(repositoryDir),
		})
		return
	}
	neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to allocate diff capture"})
}

func (m *AmpModule) neoDiffCaptureObjectFormat(ctx context.Context, threadID string) (string, error) {
	if m == nil || m.neoRuntime == nil || m.neoRuntime.store == nil {
		return "sha1", nil
	}
	actor := m.neoRuntime.store.lookupThreadActor(threadID)
	if actor == nil {
		return "sha1", nil
	}
	actor.mu.Lock()
	workingDirectory := neoWorkingDirectoryFromEnvironment(actor.environment)
	actor.mu.Unlock()
	if workingDirectory == "" {
		return "sha1", nil
	}
	value, err := neoDiffCaptureGit(ctx, workingDirectory, []string{"rev-parse", "--show-object-format"}, 128, nil)
	if err != nil {
		return "", fmt.Errorf("resolve diff capture Git object format: %w", err)
	}
	objectFormat := strings.TrimSpace(value)
	if objectFormat != "sha1" && objectFormat != "sha256" {
		return "", fmt.Errorf("unsupported diff capture Git object format %q", objectFormat)
	}
	return objectFormat, nil
}

func (m *AmpModule) serveNeoDiffCapturePublish(c *gin.Context, threadID, captureID string) {
	raw, err := neoDiffCaptureReadBody(c, neoDiffCapturePublishMaxBytes)
	if err != nil {
		neoDiffCaptureWriteBodyError(c, err)
		return
	}
	ctx := c.Request.Context()
	unlock, err := neoDiffCaptureLock(ctx, threadID)
	if err != nil {
		return
	}
	release := neoDiffCaptureLockRelease(unlock)
	defer release()
	if ctx.Err() != nil {
		return
	}
	receiveRelease, errReceive := neoAcquireDiffCaptureReceiveLock(threadID)
	if errReceive != nil {
		if errors.Is(errReceive, errNeoDiffCaptureReceiveActive) {
			neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture receive is active"})
		} else {
			logNeoDiffCaptureError("publish receive lock", threadID, captureID, errReceive)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		}
		return
	}
	receiveRelease = neoDiffCaptureLockRelease(receiveRelease)
	threadRelease := release
	release = neoDiffCaptureLockRelease(func() {
		receiveRelease()
		threadRelease()
	})
	defer receiveRelease()
	record, err := readNeoDiffCaptureRecord(threadID, captureID)
	if errors.Is(err, os.ErrNotExist) {
		neoDiffCaptureWriteJSON(c, release, http.StatusNotFound, gin.H{"error": "diff capture not found"})
		return
	}
	if err != nil {
		logNeoDiffCaptureError("publish read", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	manifestSHA256 := neoDiffCaptureManifestSHA256(raw)
	if record.State == "published" {
		if !bytes.Equal(raw, record.Manifest) {
			neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "published diff capture manifest is immutable"})
			return
		}
		_, manifest, err := validateNeoDiffCapturePublishedRecord(record)
		if err != nil {
			logNeoDiffCaptureError("republish metadata", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
			return
		}
		if err := neoVerifyDiffCapturePublishedRef(ctx, neoDiffCaptureRepositoryDir(threadID), record.PublishedRef, manifest.Revisions.ProjectedWorktreeSHA); err != nil {
			logNeoDiffCaptureError("republish ref validation", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture ref does not match projected worktree"})
			return
		}
		if err := writeNeoDiffCapturePendingPublication(threadID, captureID); err != nil {
			logNeoDiffCaptureError("republish pending", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
			return
		}
		if err := ensureNeoDiffCapturePublishedIndex(threadID, record, raw); err != nil {
			logNeoDiffCaptureError("republish index", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
			return
		}
		if err := updateNeoDiffCaptureLatest(threadID, record); err != nil {
			neoDiffCaptureBlobIndexes.delete(threadID)
			logNeoDiffCaptureError("republish latest", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
			return
		}
		if err := removeNeoDiffCapturePendingPublication(threadID); err != nil {
			logNeoDiffCaptureError("republish pending removal", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
			return
		}
		if err := neoPruneDiffCaptureStorage(ctx, threadID, captureID, false); err != nil {
			logNeoDiffCaptureError("republish storage cleanup", threadID, captureID, err)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
			return
		}
		neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{"captureID": captureID, "published": true})
		return
	}
	if record.State != "allocated" {
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture is not publishable"})
		return
	}
	var manifest neoDiffCaptureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		neoDiffCaptureWriteJSON(c, release, http.StatusBadRequest, gin.H{"error": "invalid diff capture manifest"})
		return
	}
	if err := validateNeoDiffCaptureManifest(record, manifest); err != nil {
		neoDiffCaptureWriteJSON(c, release, http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	if err := validateNeoDiffCaptureManifestRevisions(ctx, repositoryDir, manifest); err != nil {
		logNeoDiffCaptureError("publish revision validation", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	shas, err := validateNeoDiffCaptureManifestBlobs(ctx, repositoryDir, manifest)
	if err != nil {
		logNeoDiffCaptureError("publish blob validation", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	if err := validateNeoDiffCaptureManifestFileData(ctx, repositoryDir, manifest); err != nil {
		logNeoDiffCaptureError("publish file validation", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	publishedRef := neoDiffCapturePublishedRef(captureID)
	record.PublishedRef = publishedRef
	record.PublishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	record.State = "published"
	record.ManifestSHA256 = manifestSHA256
	record.Manifest = append(json.RawMessage(nil), raw...)
	publishedRaw, err := marshalNeoDiffCaptureRecord(record)
	if err != nil {
		logNeoDiffCaptureError("publish metadata", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	usage, err := neoDiffCaptureRecordStorageUsage(threadID)
	if err != nil {
		logNeoDiffCaptureError("publish metadata", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	allocatedBytes, ok := usage.fileBytes[captureID]
	if !ok {
		logNeoDiffCaptureError("publish metadata", threadID, captureID, errors.New("allocated diff capture metadata is missing from storage usage"))
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	if usage.overLimit || usage.recordBytes-allocatedBytes+int64(len(publishedRaw)) > neoDiffCaptureScanMaxBytes {
		neoDiffCaptureWriteJSON(c, release, http.StatusInsufficientStorage, gin.H{"error": "diff capture storage exceeds local limit"})
		return
	}
	index, err := neoDiffCaptureBlobIndexForPublish(threadID)
	if err != nil {
		logNeoDiffCaptureError("publish index", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	index = index.withCapture(captureID, manifestSHA256, shas)
	indexRaw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil {
		neoDiffCaptureWriteJSON(c, release, http.StatusInsufficientStorage, gin.H{"error": "diff capture blob index exceeds local limit"})
		return
	}
	if err := writeNeoDiffCapturePendingPublication(threadID, captureID); err != nil {
		logNeoDiffCaptureError("publish pending", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	promoted, err := neoPromoteDiffCaptureRef(ctx, repositoryDir, record.CaptureRef, publishedRef, manifest.Revisions.ProjectedWorktreeSHA)
	if err != nil {
		if !promoted {
			if removeErr := removeNeoDiffCapturePendingPublication(threadID); removeErr != nil {
				logNeoDiffCaptureError("publish pending rollback", threadID, captureID, removeErr)
				neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
				return
			}
		}
		logNeoDiffCaptureError("publish ref promotion", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture ref does not match projected worktree"})
		return
	}
	if err := writeNeoDiffCaptureRecordRaw(neoDiffCaptureRecordPath(threadID, captureID), publishedRaw); err != nil {
		neoDiffCaptureBlobIndexes.delete(threadID)
		logNeoDiffCaptureError("publish write", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, indexRaw); err != nil {
		neoDiffCaptureBlobIndexes.delete(threadID)
		logNeoDiffCaptureError("publish index write", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	if err := updateNeoDiffCaptureLatest(threadID, record); err != nil {
		neoDiffCaptureBlobIndexes.delete(threadID)
		logNeoDiffCaptureError("publish latest", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	neoDiffCaptureBlobIndexes.put(threadID, index)
	if err := removeNeoDiffCapturePendingPublication(threadID); err != nil {
		logNeoDiffCaptureError("publish pending removal", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	if err := neoPruneDiffCaptureStorage(ctx, threadID, captureID, false); err != nil {
		logNeoDiffCaptureError("publish storage cleanup", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to publish diff capture"})
		return
	}
	neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{"captureID": captureID, "published": true})
}

func (m *AmpModule) serveNeoDiffCaptureDelete(c *gin.Context, threadID, captureID string) {
	ctx := c.Request.Context()
	unlock, err := neoDiffCaptureLock(ctx, threadID)
	if err != nil {
		return
	}
	release := neoDiffCaptureLockRelease(unlock)
	defer release()
	if ctx.Err() != nil {
		return
	}
	receiveRelease, errReceive := neoAcquireDiffCaptureReceiveLock(threadID)
	if errReceive != nil {
		if errors.Is(errReceive, errNeoDiffCaptureReceiveActive) {
			neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture receive is active"})
		} else {
			logNeoDiffCaptureError("delete receive lock", threadID, captureID, errReceive)
			neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to delete diff capture"})
		}
		return
	}
	receiveRelease = neoDiffCaptureLockRelease(receiveRelease)
	threadRelease := release
	release = neoDiffCaptureLockRelease(func() {
		receiveRelease()
		threadRelease()
	})
	defer receiveRelease()
	pending, err := readNeoDiffCapturePendingPublication(threadID)
	if err == nil && pending.CaptureID == captureID {
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture publication is pending"})
		return
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logNeoDiffCaptureError("delete pending publication", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to delete diff capture"})
		return
	}
	record, err := readNeoDiffCaptureRecord(threadID, captureID)
	if errors.Is(err, os.ErrNotExist) {
		neoDiffCaptureWriteStatus(c, release, http.StatusNoContent)
		return
	}
	if err != nil {
		logNeoDiffCaptureError("delete read", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to delete diff capture"})
		return
	}
	if record.CaptureRef != "refs/heads/amp/captures/"+captureID {
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "invalid stored diff capture ref"})
		return
	}
	if record.State == "published" {
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "published diff capture cannot be discarded"})
		return
	}
	if record.State != "allocated" {
		neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "invalid stored diff capture state"})
		return
	}
	if _, err := neoDiffCaptureGit(ctx, neoDiffCaptureRepositoryDir(threadID), []string{"update-ref", "-d", record.CaptureRef}, 4096, nil); err != nil {
		logNeoDiffCaptureError("delete ref", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to delete diff capture"})
		return
	}
	if err := os.Remove(neoDiffCaptureRecordPath(threadID, captureID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		logNeoDiffCaptureError("delete metadata", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to delete diff capture"})
		return
	}
	if err := neoPruneDiffCaptureStorage(ctx, threadID, "", true); err != nil {
		logNeoDiffCaptureError("delete storage cleanup", threadID, captureID, err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to delete diff capture"})
		return
	}
	neoDiffCaptureWriteStatus(c, release, http.StatusNoContent)
}

func (m *AmpModule) serveNeoDiffCaptureLatest(c *gin.Context, threadID string) {
	ctx := c.Request.Context()
	unlock, err := neoDiffCaptureLock(ctx, threadID)
	if err != nil {
		return
	}
	release := neoDiffCaptureLockRelease(unlock)
	defer release()
	if ctx.Err() != nil {
		return
	}
	_, errPendingPublication := os.Stat(neoDiffCapturePendingPublicationPath(threadID))
	_, errPendingPrune := os.Stat(neoDiffCapturePendingPrunePath(threadID))
	if errPendingPublication != nil && !errors.Is(errPendingPublication, os.ErrNotExist) {
		logNeoDiffCaptureError("latest pending publication", threadID, "", errPendingPublication)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read latest diff capture"})
		return
	}
	if errPendingPrune != nil && !errors.Is(errPendingPrune, os.ErrNotExist) {
		logNeoDiffCaptureError("latest pending prune", threadID, "", errPendingPrune)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read latest diff capture"})
		return
	}
	if errPendingPublication == nil || errPendingPrune == nil {
		receiveRelease, errReceive := neoAcquireDiffCaptureReceiveLock(threadID)
		if errReceive != nil {
			if errors.Is(errReceive, errNeoDiffCaptureReceiveActive) {
				neoDiffCaptureWriteJSON(c, release, http.StatusConflict, gin.H{"error": "diff capture receive is active"})
			} else {
				logNeoDiffCaptureError("latest receive lock", threadID, "", errReceive)
				neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read latest diff capture"})
			}
			return
		}
		receiveRelease = neoDiffCaptureLockRelease(receiveRelease)
		threadRelease := release
		release = neoDiffCaptureLockRelease(func() {
			receiveRelease()
			threadRelease()
		})
		defer receiveRelease()
		if errPendingPrune == nil {
			if _, err := completeNeoDiffCapturePendingPrune(ctx, threadID, false); err != nil {
				logNeoDiffCaptureError("latest pending prune recovery", threadID, "", err)
				neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read latest diff capture"})
				return
			}
		}
	}
	record, manifest, ok, err := latestNeoDiffCapture(threadID)
	if err != nil {
		logNeoDiffCaptureError("latest", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read latest diff capture"})
		return
	}
	if !ok {
		neoDiffCaptureWriteStatus(c, release, http.StatusNoContent)
		return
	}
	neoDiffCaptureWriteJSON(c, release, http.StatusOK, neoDiffCaptureSafeSubset(record, manifest))
}

func (m *AmpModule) serveNeoDiffCaptureBlob(c *gin.Context, threadID, sha string) {
	ctx := c.Request.Context()
	unlock, err := neoDiffCaptureLock(ctx, threadID)
	if err != nil {
		return
	}
	release := neoDiffCaptureLockRelease(unlock)
	defer release()
	if ctx.Err() != nil {
		return
	}
	index, err := neoDiffCaptureBlobIndexForThread(threadID)
	if err != nil {
		logNeoDiffCaptureError("blob index", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	authorized, err := neoDiffCaptureIndexAuthorizes(threadID, index, sha)
	if err != nil {
		logNeoDiffCaptureError("blob authorization", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	if !authorized {
		neoDiffCaptureWriteJSON(c, release, http.StatusNotFound, gin.H{"error": "diff capture blob not found"})
		return
	}
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	objectType, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"cat-file", "-t", sha}, 128, nil)
	if err != nil {
		logNeoDiffCaptureError("blob type", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	if strings.TrimSpace(objectType) != "blob" {
		err = fmt.Errorf("authorized object %s has type %q", sha, strings.TrimSpace(objectType))
		logNeoDiffCaptureError("blob type", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	sizeRaw, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"cat-file", "-s", sha}, 128, nil)
	if err != nil {
		logNeoDiffCaptureError("blob size", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	sizeBytes, err := strconv.ParseInt(strings.TrimSpace(sizeRaw), 10, 64)
	if err != nil || sizeBytes < 0 {
		logNeoDiffCaptureError("blob size", threadID, "", fmt.Errorf("invalid blob size %q", strings.TrimSpace(sizeRaw)))
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	if sizeBytes > neoDiffCaptureTextBlobMaxBytes {
		neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{"kind": "omitted", "sizeBytes": sizeBytes, "reason": "file exceeds local diff capture limit"})
		return
	}
	content, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"cat-file", "blob", sha}, neoDiffCaptureTextBlobMaxBytes+1, nil)
	release()
	if err != nil || int64(len(content)) != sizeBytes {
		if err == nil {
			err = fmt.Errorf("blob size changed from %d to %d bytes", sizeBytes, len(content))
		}
		logNeoDiffCaptureError("blob content", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to read diff capture blob"})
		return
	}
	if strings.IndexByte(content, 0) >= 0 || !utf8.ValidString(content) {
		neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{"kind": "binary", "sizeBytes": sizeBytes})
		return
	}
	neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{"kind": "text", "content": content})
}

func (m *AmpModule) serveNeoDiffCaptureDelta(c *gin.Context, threadID string) {
	fromSHA := strings.ToLower(strings.TrimSpace(c.Query("from")))
	toSHA := strings.ToLower(strings.TrimSpace(c.Query("to")))
	if !neoDiffCaptureSHApattern.MatchString(fromSHA) || !neoDiffCaptureSHApattern.MatchString(toSHA) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid diff capture blob sha"})
		return
	}
	ctx := c.Request.Context()
	unlock, err := neoDiffCaptureLock(ctx, threadID)
	if err != nil {
		return
	}
	release := neoDiffCaptureLockRelease(unlock)
	defer release()
	if ctx.Err() != nil {
		return
	}
	index, err := neoDiffCaptureBlobIndexForThread(threadID)
	if err != nil {
		logNeoDiffCaptureError("delta index", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to build diff capture delta"})
		return
	}
	authorized, err := neoDiffCaptureIndexAuthorizes(threadID, index, fromSHA, toSHA)
	if err != nil {
		logNeoDiffCaptureError("delta authorization", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to build diff capture delta"})
		return
	}
	if !authorized {
		neoDiffCaptureWriteJSON(c, release, http.StatusNotFound, gin.H{"error": "diff capture blobs not found"})
		return
	}
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	if err := neoDiffCaptureBatchCheckBlobs(ctx, repositoryDir, []string{fromSHA, toSHA}); err != nil {
		logNeoDiffCaptureError("delta blobs", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to build diff capture delta"})
		return
	}
	diff, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"diff", "--no-ext-diff", "--no-color", "--no-textconv", "--no-renames", fromSHA, toSHA}, neoDiffCaptureDeltaMaxBytes+1, nil)
	release()
	if err != nil {
		logNeoDiffCaptureError("delta", threadID, "", err)
		neoDiffCaptureWriteJSON(c, release, http.StatusInternalServerError, gin.H{"error": "failed to build diff capture delta"})
		return
	}
	if len(diff) > neoDiffCaptureDeltaMaxBytes {
		neoDiffCaptureWriteJSON(c, release, http.StatusRequestEntityTooLarge, gin.H{"error": "diff capture delta exceeds local limit"})
		return
	}
	neoDiffCaptureWriteJSON(c, release, http.StatusOK, gin.H{"diff": diff})
}

func validateNeoDiffCaptureManifest(record neoDiffCaptureRecord, manifest neoDiffCaptureManifest) error {
	if manifest.Version != 1 {
		return errors.New("unsupported diff capture manifest version")
	}
	if manifest.CaptureID != record.CaptureID {
		return errors.New("diff capture manifest id mismatch")
	}
	if manifest.CaptureRef != record.CaptureRef || record.CaptureRef != "refs/heads/amp/captures/"+record.CaptureID {
		return errors.New("diff capture manifest ref mismatch")
	}
	if !neoDiffCaptureSHApattern.MatchString(manifest.Revisions.ProjectedWorktreeSHA) {
		return errors.New("invalid projected worktree sha")
	}
	if _, err := neoDiffCaptureSafeCapturedAt(manifest.CapturedAt); err != nil {
		return err
	}
	if _, err := neoDiffCaptureManifestBlobSHAs(manifest); err != nil {
		return err
	}
	return nil
}

func neoDiffCaptureSafeCapturedAt(raw json.RawMessage) (any, error) {
	value := strings.TrimSpace(string(raw))
	if strings.HasPrefix(value, `"`) {
		var timestamp string
		if err := json.Unmarshal(raw, &timestamp); err != nil {
			return nil, errors.New("invalid diff capture timestamp")
		}
		if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
			return nil, errors.New("invalid diff capture timestamp")
		}
		return timestamp, nil
	}
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || milliseconds < 0 || milliseconds > 1<<53-1 {
		return nil, errors.New("invalid diff capture timestamp")
	}
	return milliseconds, nil
}

func validateNeoDiffCaptureManifestRevisions(ctx context.Context, repositoryDir string, manifest neoDiffCaptureManifest) error {
	revisions := neoDiffCaptureManifestRevisionSHAs(manifest)
	for _, revision := range revisions {
		if !neoDiffCaptureSHApattern.MatchString(revision) {
			return errors.New("invalid diff capture revision sha")
		}
	}
	if len(manifest.ProjectedCommits) > neoDiffCaptureManifestMaxCommits {
		return errors.New("too many diff capture projected commits")
	}
	mergeBaseSHA := strings.ToLower(manifest.Source.MergeBaseSHA)
	sourceHeadSHA := strings.ToLower(manifest.Source.HeadSHA)
	if _, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"merge-base", "--is-ancestor", mergeBaseSHA, sourceHeadSHA}, 1, nil); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("diff capture source merge base is not an ancestor of source head")
	}
	maxSourceOutput := (neoDiffCaptureManifestMaxCommits+1)*(64+1) + 1
	sourceOutput, err := neoDiffCaptureGit(ctx, repositoryDir, []string{
		"rev-list",
		"--reverse",
		"--topo-order",
		"--max-count=" + strconv.Itoa(neoDiffCaptureManifestMaxCommits+1),
		mergeBaseSHA + ".." + sourceHeadSHA,
		"--",
	}, maxSourceOutput, nil)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("failed to validate diff capture source commit lineage")
	}
	sourceCommits, err := neoDiffCaptureRevisionList(sourceOutput)
	if err != nil {
		return err
	}
	if len(sourceCommits) > neoDiffCaptureManifestMaxCommits {
		return errors.New("too many diff capture source commits")
	}
	if len(sourceCommits) != len(manifest.ProjectedCommits) {
		return errors.New("diff capture projected source commits do not match source lineage")
	}
	projectedSHAs := make([]string, len(manifest.ProjectedCommits))
	treeRevisions := make([]string, 0, len(manifest.ProjectedCommits)*2)
	for index, commit := range manifest.ProjectedCommits {
		if !strings.EqualFold(commit.SourceCommitSHA, sourceCommits[index]) {
			return errors.New("diff capture projected source commits do not match source lineage")
		}
		projectedSHAs[index] = strings.ToLower(commit.ProjectedCommitSHA)
		treeRevisions = append(treeRevisions, sourceCommits[index], projectedSHAs[index])
	}
	trees, err := neoDiffCaptureRevisionTrees(ctx, repositoryDir, treeRevisions)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("failed to validate diff capture projected commit contents")
	}
	for index := range manifest.ProjectedCommits {
		if trees[index*2] != trees[index*2+1] {
			return errors.New("diff capture projected commit contents do not match source commit")
		}
	}
	expectedProjectedHead := mergeBaseSHA
	if len(projectedSHAs) != 0 {
		expectedProjectedHead = projectedSHAs[len(projectedSHAs)-1]
	}
	projectedHeadSHA := strings.ToLower(manifest.Revisions.ProjectedHeadSHA)
	if projectedHeadSHA != expectedProjectedHead {
		return errors.New("diff capture projected head does not match projected commit lineage")
	}
	parentQueries := append([]string(nil), projectedSHAs...)
	projectedIndexSHA := strings.ToLower(manifest.Revisions.ProjectedIndexSHA)
	projectedWorktreeSHA := strings.ToLower(manifest.Revisions.ProjectedWorktreeSHA)
	if projectedIndexSHA != projectedHeadSHA {
		parentQueries = append(parentQueries, projectedIndexSHA)
	}
	if projectedWorktreeSHA != projectedIndexSHA {
		parentQueries = append(parentQueries, projectedWorktreeSHA)
	}
	parents, err := neoDiffCaptureRevisionParents(ctx, repositoryDir, parentQueries)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("failed to validate diff capture projected commit lineage")
	}
	previousSHA := mergeBaseSHA
	for _, projectedSHA := range projectedSHAs {
		if len(parents[projectedSHA]) == 0 || parents[projectedSHA][0] != previousSHA {
			return errors.New("diff capture projected commits do not form the required first-parent chain")
		}
		previousSHA = projectedSHA
	}
	if projectedIndexSHA != projectedHeadSHA && (len(parents[projectedIndexSHA]) == 0 || parents[projectedIndexSHA][0] != projectedHeadSHA) {
		return errors.New("diff capture projected index does not extend projected head")
	}
	if projectedWorktreeSHA != projectedIndexSHA && (len(parents[projectedWorktreeSHA]) == 0 || parents[projectedWorktreeSHA][0] != projectedIndexSHA) {
		return errors.New("diff capture projected worktree does not extend projected index")
	}
	return nil
}

func neoDiffCaptureRevisionList(output string) ([]string, error) {
	if output == "" {
		return nil, nil
	}
	if !strings.HasSuffix(output, "\n") {
		return nil, errors.New("invalid diff capture source commit traversal output")
	}
	lines := strings.Split(strings.TrimSuffix(output, "\n"), "\n")
	commits := make([]string, 0, len(lines))
	for _, line := range lines {
		if !neoDiffCaptureSHApattern.MatchString(line) {
			return nil, errors.New("invalid diff capture source commit traversal output")
		}
		commits = append(commits, strings.ToLower(line))
	}
	return commits, nil
}

func neoDiffCaptureRevisionParents(ctx context.Context, repositoryDir string, revisions []string) (map[string][]string, error) {
	unique := make([]string, 0, len(revisions))
	seen := make(map[string]struct{}, len(revisions))
	for _, revision := range revisions {
		if !neoDiffCaptureSHApattern.MatchString(revision) {
			return nil, errors.New("invalid diff capture projected revision")
		}
		revision = strings.ToLower(revision)
		if _, duplicate := seen[revision]; duplicate {
			continue
		}
		seen[revision] = struct{}{}
		unique = append(unique, revision)
	}
	parents := make(map[string][]string, len(unique))
	remainingOutputBytes := neoDiffCaptureRevisionMaxBytes
	for start := 0; start < len(unique); {
		args := []string{"rev-list", "--parents", "--no-walk=unsorted"}
		argumentBytes := 64
		end := start
		for end < len(unique) {
			revisionBytes := len(unique[end]) + 1
			if end > start && argumentBytes+revisionBytes > neoDiffCaptureGitArgsMaxBytes {
				break
			}
			args = append(args, unique[end])
			argumentBytes += revisionBytes
			end++
		}
		output, err := neoDiffCaptureGit(ctx, repositoryDir, args, remainingOutputBytes+1, nil)
		if err != nil {
			return nil, err
		}
		if len(output) > remainingOutputBytes {
			return nil, errors.New("diff capture projected revision output exceeds local limit")
		}
		remainingOutputBytes -= len(output)
		batch := make(map[string]struct{}, end-start)
		for _, revision := range unique[start:end] {
			batch[revision] = struct{}{}
		}
		for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 0 || !neoDiffCaptureSHApattern.MatchString(fields[0]) {
				return nil, errors.New("invalid diff capture projected revision output")
			}
			revision := strings.ToLower(fields[0])
			if _, requested := batch[revision]; !requested {
				return nil, errors.New("unexpected diff capture projected revision output")
			}
			if _, duplicate := parents[revision]; duplicate {
				return nil, errors.New("duplicate diff capture projected revision output")
			}
			lineParents := make([]string, 0, len(fields)-1)
			for _, parent := range fields[1:] {
				if !neoDiffCaptureSHApattern.MatchString(parent) {
					return nil, errors.New("invalid diff capture projected revision parent")
				}
				lineParents = append(lineParents, strings.ToLower(parent))
			}
			parents[revision] = lineParents
		}
		for _, revision := range unique[start:end] {
			if _, found := parents[revision]; !found {
				return nil, errors.New("incomplete diff capture projected revision output")
			}
		}
		start = end
	}
	return parents, nil
}

func neoDiffCaptureRevisionTrees(ctx context.Context, repositoryDir string, revisions []string) ([]string, error) {
	if len(revisions) == 0 {
		return nil, nil
	}
	var input strings.Builder
	for _, revision := range revisions {
		if !neoDiffCaptureSHApattern.MatchString(revision) {
			return nil, errors.New("invalid diff capture tree revision")
		}
		input.WriteString(revision)
		input.WriteString("^{tree}\n")
	}
	maxOutputBytes := len(revisions)*(64+len(" tree\n")) + 1
	output, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"cat-file", "--batch-check=%(objectname) %(objecttype)"}, maxOutputBytes, strings.NewReader(input.String()))
	if err != nil {
		return nil, err
	}
	trees := make([]string, 0, len(revisions))
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || !neoDiffCaptureSHApattern.MatchString(fields[0]) || fields[1] != "tree" {
			return nil, errors.New("invalid diff capture tree revision output")
		}
		trees = append(trees, strings.ToLower(fields[0]))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(trees) != len(revisions) {
		return nil, errors.New("incomplete diff capture tree revision output")
	}
	return trees, nil
}

func validateNeoDiffCaptureManifestBlobs(ctx context.Context, repositoryDir string, manifest neoDiffCaptureManifest) ([]string, error) {
	shas, err := neoDiffCaptureManifestBlobSHAs(manifest)
	if err != nil {
		return nil, err
	}
	if err := neoDiffCaptureBatchCheckObjects(ctx, repositoryDir, shas, "blob"); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("diff capture manifest references unavailable blob")
	}
	return shas, nil
}

func neoDiffCaptureBatchCheckBlobs(ctx context.Context, repositoryDir string, shas []string) error {
	return neoDiffCaptureBatchCheckObjects(ctx, repositoryDir, shas, "blob")
}

func neoDiffCaptureBatchCheckObjects(ctx context.Context, repositoryDir string, shas []string, objectType string) error {
	if len(shas) == 0 {
		return nil
	}
	var input strings.Builder
	for _, sha := range shas {
		input.WriteString(sha)
		input.WriteByte('\n')
	}
	maxOutputBytes := len(shas)*(64+len(" missing\n")) + 1
	output, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"cat-file", "--batch-check=%(objectname) %(objecttype)"}, maxOutputBytes, strings.NewReader(input.String()))
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(strings.NewReader(output))
	index := 0
	for scanner.Scan() {
		if index >= len(shas) {
			return errors.New("unexpected diff capture blob validation output")
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || !strings.EqualFold(fields[0], shas[index]) || fields[1] != objectType {
			return errors.New("diff capture manifest references unavailable object")
		}
		index++
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if index != len(shas) {
		return errors.New("incomplete diff capture blob validation output")
	}
	return nil
}

func validateNeoDiffCaptureManifestFiles(ctx context.Context, repositoryDir string, manifest neoDiffCaptureManifest) error {
	if err := validateNeoDiffCaptureManifestRevisions(ctx, repositoryDir, manifest); err != nil {
		return err
	}
	return validateNeoDiffCaptureManifestFileData(ctx, repositoryDir, manifest)
}

func validateNeoDiffCaptureManifestFileData(ctx context.Context, repositoryDir string, manifest neoDiffCaptureManifest) error {
	fileSets, err := neoDiffCaptureManifestFileSets(manifest)
	if err != nil {
		return err
	}
	queries := make(map[string]map[string]struct{})
	addQuery := func(revision, filename string) {
		if queries[revision] == nil {
			queries[revision] = make(map[string]struct{})
		}
		queries[revision][filename] = struct{}{}
	}
	for _, fileSet := range fileSets {
		for _, file := range fileSet.files {
			oldPath := file.Path
			if file.PreviousPath != "" {
				oldPath = file.PreviousPath
				addQuery(fileSet.baseSHA, file.Path)
				addQuery(fileSet.headSHA, oldPath)
			}
			addQuery(fileSet.baseSHA, oldPath)
			addQuery(fileSet.headSHA, file.Path)
		}
	}
	trees := make(map[string]map[string]neoDiffCaptureTreeFile, len(queries))
	treeRevisions := make([]string, 0, len(queries))
	for revision := range queries {
		treeRevisions = append(treeRevisions, revision)
	}
	if len(treeRevisions) > neoDiffCaptureMaxTreeQueries {
		return errors.New("diff capture manifest tree queries exceed local limit")
	}
	sort.Strings(treeRevisions)
	for _, revision := range treeRevisions {
		paths := make([]string, 0, len(queries[revision]))
		for filename := range queries[revision] {
			paths = append(paths, filename)
		}
		sort.Strings(paths)
		trees[revision], err = neoDiffCaptureTreeFiles(ctx, repositoryDir, revision, paths)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return errors.New("diff capture manifest file metadata does not match captured revisions")
		}
	}
	for _, fileSet := range fileSets {
		replacedPaths := make(map[string]struct{}, len(fileSet.files))
		for _, file := range fileSet.files {
			if file.PreviousPath == "" {
				replacedPaths[file.Path] = struct{}{}
			}
		}
		for _, file := range fileSet.files {
			if err := neoDiffCaptureValidateManifestFile(file, replacedPaths, trees[fileSet.baseSHA], trees[fileSet.headSHA]); err != nil {
				return err
			}
		}
	}
	changedFileCount := 0
	changedPathBytes := 0
	numStatsByRange := make(map[[2]string]neoDiffCaptureNumStats)
	for _, fileSet := range fileSets {
		numStatsByRange[[2]string{fileSet.baseSHA, fileSet.headSHA}] = neoDiffCaptureNumStats{}
	}
	if len(numStatsByRange) > neoDiffCaptureMaxRangeQueries {
		return errors.New("diff capture manifest range queries exceed local limit")
	}
	clear(numStatsByRange)
	for _, fileSet := range fileSets {
		key := [2]string{fileSet.baseSHA, fileSet.headSHA}
		numStats, ok := numStatsByRange[key]
		if !ok {
			remainingBytes := neoDiffCaptureManifestMaxPathBytes - changedPathBytes
			numStats, err = neoDiffCaptureReadNumStats(ctx, repositoryDir, fileSet.baseSHA, fileSet.headSHA, remainingBytes)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("diff capture manifest display metadata does not match captured revisions")
			}
			changedFileCount += len(numStats.entries)
			changedPathBytes += numStats.pathBytes
			if changedFileCount > neoDiffCaptureManifestMaxFiles || changedPathBytes > neoDiffCaptureManifestMaxPathBytes {
				return errors.New("diff capture manifest file comparison exceeds local limit")
			}
			numStatsByRange[key] = numStats
		}
		if err := neoDiffCaptureValidateManifestFileStats(fileSet.files, numStats.entries, trees[fileSet.baseSHA], trees[fileSet.headSHA]); err != nil {
			return err
		}
	}
	return nil
}

func neoDiffCaptureManifestFileSets(manifest neoDiffCaptureManifest) ([]neoDiffCaptureManifestFileSet, error) {
	for _, revision := range neoDiffCaptureManifestRevisionSHAs(manifest) {
		if !neoDiffCaptureSHApattern.MatchString(revision) {
			return nil, errors.New("invalid diff capture revision sha")
		}
	}
	knownRevisions := make(map[string]bool)
	for _, revision := range []string{
		manifest.Source.HeadSHA,
		manifest.Source.MergeBaseSHA,
		manifest.Revisions.ProjectedHeadSHA,
		manifest.Revisions.ProjectedIndexSHA,
		manifest.Revisions.ProjectedWorktreeSHA,
	} {
		knownRevisions[strings.ToLower(revision)] = true
	}
	projectedBySource := make(map[string]int, len(manifest.ProjectedCommits))
	projectedSHAs := make(map[string]struct{}, len(manifest.ProjectedCommits))
	for index, commit := range manifest.ProjectedCommits {
		sourceSHA := strings.ToLower(commit.SourceCommitSHA)
		if _, duplicate := projectedBySource[sourceSHA]; duplicate {
			return nil, errors.New("duplicate diff capture projected source commit")
		}
		projectedSHA := strings.ToLower(commit.ProjectedCommitSHA)
		if _, duplicate := projectedSHAs[projectedSHA]; duplicate {
			return nil, errors.New("duplicate diff capture projected commit")
		}
		projectedBySource[sourceSHA] = index
		projectedSHAs[projectedSHA] = struct{}{}
		knownRevisions[projectedSHA] = true
	}
	if len(manifest.Sections)+len(manifest.RangeSections) > neoDiffCaptureManifestMaxFileSets {
		return nil, errors.New("too many diff capture manifest file sets")
	}
	fileCount := 0
	fileSets := make([]neoDiffCaptureManifestFileSet, 0, len(manifest.Sections)+len(manifest.RangeSections))
	sectionIDs := make(map[string]struct{}, len(manifest.Sections))
	for _, section := range manifest.Sections {
		if section.ID == "" || !utf8.ValidString(section.ID) {
			return nil, errors.New("invalid diff capture section id")
		}
		if _, duplicate := sectionIDs[section.ID]; duplicate {
			return nil, errors.New("duplicate diff capture section id")
		}
		sectionIDs[section.ID] = struct{}{}
		baseSHA := strings.ToLower(section.BaseCommitSHA)
		headSHA := strings.ToLower(section.HeadCommitSHA)
		if !knownRevisions[baseSHA] || !knownRevisions[headSHA] {
			return nil, errors.New("diff capture section references an unknown revision")
		}
		if err := validateNeoDiffCaptureManifestFileList(section.Files); err != nil {
			return nil, err
		}
		fileCount += len(section.Files)
		if fileCount > neoDiffCaptureManifestMaxFiles {
			return nil, errors.New("too many diff capture manifest files")
		}
		fileSets = append(fileSets, neoDiffCaptureManifestFileSet{baseSHA: baseSHA, headSHA: headSHA, files: section.Files})
	}
	rangeIDs := make(map[string]struct{}, len(manifest.RangeSections))
	hasFullRange := false
	for _, section := range manifest.RangeSections {
		if section.RangeID == "" || !utf8.ValidString(section.RangeID) {
			return nil, errors.New("invalid diff capture range id")
		}
		if _, duplicate := rangeIDs[section.RangeID]; duplicate {
			return nil, errors.New("duplicate diff capture range id")
		}
		rangeIDs[section.RangeID] = struct{}{}
		var baseSHA, headSHA string
		switch {
		case strings.HasPrefix(section.RangeID, "merge-base:"):
			baseSHA = strings.ToLower(strings.TrimPrefix(section.RangeID, "merge-base:"))
			if baseSHA != strings.ToLower(manifest.Source.MergeBaseSHA) {
				return nil, errors.New("diff capture range references an unknown merge base")
			}
			headSHA = strings.ToLower(manifest.Revisions.ProjectedWorktreeSHA)
			hasFullRange = true
		case strings.HasPrefix(section.RangeID, "commit:"):
			sourceSHA := strings.ToLower(strings.TrimPrefix(section.RangeID, "commit:"))
			index, ok := projectedBySource[sourceSHA]
			if !ok {
				return nil, errors.New("diff capture range references an unknown commit")
			}
			headSHA = strings.ToLower(manifest.ProjectedCommits[index].ProjectedCommitSHA)
			baseSHA = strings.ToLower(manifest.Source.MergeBaseSHA)
			if index > 0 {
				baseSHA = strings.ToLower(manifest.ProjectedCommits[index-1].ProjectedCommitSHA)
			}
		default:
			return nil, errors.New("invalid diff capture range id")
		}
		if err := validateNeoDiffCaptureManifestFileList(section.Files); err != nil {
			return nil, err
		}
		fileCount += len(section.Files)
		if fileCount > neoDiffCaptureManifestMaxFiles {
			return nil, errors.New("too many diff capture manifest files")
		}
		fileSets = append(fileSets, neoDiffCaptureManifestFileSet{baseSHA: baseSHA, headSHA: headSHA, files: section.Files})
	}
	if !hasFullRange {
		return nil, errors.New("diff capture manifest is missing the full projected worktree range")
	}
	return fileSets, nil
}

func neoDiffCaptureManifestRevisionSHAs(manifest neoDiffCaptureManifest) []string {
	values := []string{
		manifest.Source.HeadSHA,
		manifest.Source.MergeBaseSHA,
		manifest.Revisions.ProjectedHeadSHA,
		manifest.Revisions.ProjectedIndexSHA,
		manifest.Revisions.ProjectedWorktreeSHA,
	}
	for _, commit := range manifest.ProjectedCommits {
		values = append(values, commit.SourceCommitSHA, commit.ProjectedCommitSHA)
	}
	revisions := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, revision := range values {
		normalized := strings.ToLower(revision)
		if _, duplicate := seen[normalized]; duplicate {
			continue
		}
		seen[normalized] = struct{}{}
		revisions = append(revisions, revision)
	}
	return revisions
}

func validateNeoDiffCaptureManifestFileList(files []neoDiffCaptureManifestFile) error {
	ids := make(map[string]struct{}, len(files))
	paths := make(map[string]struct{}, len(files))
	for _, file := range files {
		if file.ID == "" || !utf8.ValidString(file.ID) {
			return errors.New("invalid diff capture file id")
		}
		if _, duplicate := ids[file.ID]; duplicate {
			return errors.New("duplicate diff capture file id")
		}
		ids[file.ID] = struct{}{}
		if !neoReviewPathValid(file.Path) || len(file.Path)+1 >= neoDiffCaptureGitArgsMaxBytes {
			return fmt.Errorf("invalid diff capture file path %q", file.Path)
		}
		if _, duplicate := paths[file.Path]; duplicate {
			return fmt.Errorf("duplicate diff capture file path %q", file.Path)
		}
		paths[file.Path] = struct{}{}
		if file.PreviousPath != "" && (!neoReviewPathValid(file.PreviousPath) || file.PreviousPath == file.Path || len(file.PreviousPath)+1 >= neoDiffCaptureGitArgsMaxBytes) {
			return fmt.Errorf("invalid diff capture previous path %q", file.PreviousPath)
		}
		switch file.ChangeType {
		case "added", "modified", "deleted", "renamed", "copied", "type_changed", "untracked":
		default:
			return fmt.Errorf("invalid diff capture change type %q", file.ChangeType)
		}
	}
	return nil
}

func neoDiffCaptureTreeFiles(ctx context.Context, repositoryDir, revision string, paths []string) (map[string]neoDiffCaptureTreeFile, error) {
	if !neoDiffCaptureSHApattern.MatchString(revision) {
		return nil, errors.New("invalid diff capture tree revision")
	}
	files := make(map[string]neoDiffCaptureTreeFile, len(paths))
	for start := 0; start < len(paths); {
		args := []string{"--literal-pathspecs", "ls-tree", "-z", "--full-tree", revision, "--"}
		argumentBytes := len(revision) + 64
		end := start
		for end < len(paths) {
			pathBytes := len(paths[end]) + 1
			if argumentBytes+pathBytes > neoDiffCaptureGitArgsMaxBytes && end == start {
				return nil, errors.New("diff capture file path exceeds local limit")
			}
			if end > start && argumentBytes+pathBytes > neoDiffCaptureGitArgsMaxBytes {
				break
			}
			args = append(args, paths[end])
			argumentBytes += pathBytes
			end++
		}
		output, err := neoDiffCaptureGit(ctx, repositoryDir, args, argumentBytes+(end-start)*128+1, nil)
		if err != nil {
			return nil, err
		}
		queriedPaths := make(map[string]struct{}, end-start)
		for _, filename := range paths[start:end] {
			queriedPaths[filename] = struct{}{}
		}
		for _, record := range strings.Split(output, "\x00") {
			if record == "" {
				continue
			}
			tab := strings.IndexByte(record, '\t')
			if tab <= 0 || tab == len(record)-1 {
				return nil, errors.New("invalid diff capture tree output")
			}
			metadata := strings.Fields(record[:tab])
			filename := record[tab+1:]
			if _, queried := queriedPaths[filename]; len(metadata) != 3 || metadata[1] != "blob" || !neoDiffCaptureSHApattern.MatchString(metadata[2]) || !queried {
				return nil, errors.New("invalid diff capture tree entry")
			}
			if _, duplicate := files[filename]; duplicate {
				return nil, errors.New("duplicate diff capture tree entry")
			}
			files[filename] = neoDiffCaptureTreeFile{mode: metadata[0], sha: strings.ToLower(metadata[2])}
		}
		start = end
	}
	return files, nil
}

func neoDiffCaptureReadNumStats(ctx context.Context, repositoryDir, baseSHA, headSHA string, maxPathBytes int) (neoDiffCaptureNumStats, error) {
	if maxPathBytes < 0 {
		return neoDiffCaptureNumStats{}, errors.New("diff capture manifest file comparison exceeds local limit")
	}
	if !neoDiffCaptureSHApattern.MatchString(baseSHA) || !neoDiffCaptureSHApattern.MatchString(headSHA) {
		return neoDiffCaptureNumStats{}, errors.New("invalid diff capture numstat revision")
	}
	maxOutputBytes := maxPathBytes + neoDiffCaptureManifestMaxFiles*48
	args := []string{"--literal-pathspecs", "-c", "core.quotepath=false", "diff", "--numstat", "-z", "--find-renames", "--find-copies-harder", baseSHA, headSHA, "--"}
	output, err := neoDiffCaptureGit(ctx, repositoryDir, args, maxOutputBytes+1, nil)
	if err != nil {
		return neoDiffCaptureNumStats{}, err
	}
	if len(output) > maxOutputBytes {
		return neoDiffCaptureNumStats{}, errors.New("diff capture manifest file comparison exceeds local limit")
	}
	result := neoDiffCaptureNumStats{entries: make([]neoDiffCaptureNumStatEntry, 0)}
	seen := make(map[[2]string]struct{})
	offset := 0
	nextRecord := func() (string, error) {
		if offset >= len(output) {
			return "", errors.New("incomplete diff capture numstat output")
		}
		end := strings.IndexByte(output[offset:], 0)
		if end < 0 {
			return "", errors.New("invalid diff capture numstat output")
		}
		record := output[offset : offset+end]
		offset += end + 1
		return record, nil
	}
	for offset < len(output) {
		header, err := nextRecord()
		if err != nil {
			return neoDiffCaptureNumStats{}, err
		}
		firstTab := strings.IndexByte(header, '\t')
		secondTab := -1
		if firstTab >= 0 {
			if nextTab := strings.IndexByte(header[firstTab+1:], '\t'); nextTab >= 0 {
				secondTab = firstTab + 1 + nextTab
			}
		}
		if firstTab <= 0 || secondTab <= firstTab+1 {
			return neoDiffCaptureNumStats{}, errors.New("invalid diff capture numstat entry")
		}
		isBinary, diffStat, err := neoDiffCaptureParseNumStat(header[:firstTab], header[firstTab+1:secondTab])
		if err != nil {
			return neoDiffCaptureNumStats{}, err
		}
		entry := neoDiffCaptureNumStatEntry{isBinary: isBinary, diffStat: diffStat}
		entry.newPath = header[secondTab+1:]
		if entry.newPath == "" {
			entry.oldPath, err = nextRecord()
			if err != nil {
				return neoDiffCaptureNumStats{}, err
			}
			entry.newPath, err = nextRecord()
			if err != nil {
				return neoDiffCaptureNumStats{}, err
			}
		}
		if entry.newPath == "" || !neoReviewPathValid(entry.newPath) || entry.oldPath != "" && !neoReviewPathValid(entry.oldPath) {
			return neoDiffCaptureNumStats{}, errors.New("invalid diff capture numstat path")
		}
		key := [2]string{entry.oldPath, entry.newPath}
		if _, duplicate := seen[key]; duplicate {
			return neoDiffCaptureNumStats{}, errors.New("duplicate diff capture numstat entry")
		}
		seen[key] = struct{}{}
		result.entries = append(result.entries, entry)
		result.pathBytes += len(entry.newPath) + 1
		if entry.oldPath != "" {
			result.pathBytes += len(entry.oldPath) + 1
		}
		if len(result.entries) > neoDiffCaptureManifestMaxFiles || result.pathBytes > maxPathBytes {
			return neoDiffCaptureNumStats{}, errors.New("diff capture manifest file comparison exceeds local limit")
		}
	}
	return result, nil
}

func neoDiffCaptureParseNumStat(addedRaw, deletedRaw string) (bool, neoDiffCaptureDiffStat, error) {
	if addedRaw == "-" || deletedRaw == "-" {
		if addedRaw != "-" || deletedRaw != "-" {
			return false, neoDiffCaptureDiffStat{}, errors.New("invalid diff capture binary numstat")
		}
		return true, neoDiffCaptureDiffStat{Changed: 1}, nil
	}
	added, err := strconv.Atoi(addedRaw)
	if err != nil || added < 0 {
		return false, neoDiffCaptureDiffStat{}, errors.New("invalid diff capture added line count")
	}
	deleted, err := strconv.Atoi(deletedRaw)
	if err != nil || deleted < 0 {
		return false, neoDiffCaptureDiffStat{}, errors.New("invalid diff capture deleted line count")
	}
	return false, neoDiffCaptureDiffStat{Added: added, Deleted: deleted, Changed: max(added, deleted)}, nil
}

func neoDiffCaptureValidateManifestFileStats(files []neoDiffCaptureManifestFile, entries []neoDiffCaptureNumStatEntry, base, head map[string]neoDiffCaptureTreeFile) error {
	ordinary := make(map[string]int)
	moved := make(map[[2]string]int)
	for index, entry := range entries {
		if entry.oldPath == "" {
			ordinary[entry.newPath] = index
		} else {
			moved[[2]string{entry.oldPath, entry.newPath}] = index
		}
	}
	used := make([]bool, len(entries))
	for _, file := range files {
		if file.DiffStat.Added < 0 || file.DiffStat.Deleted < 0 || file.DiffStat.Changed < 0 {
			return fmt.Errorf("diff capture file %q has a negative diff stat", file.Path)
		}
		expected := neoDiffCaptureNumStatEntry{}
		if file.PreviousPath == "" {
			index, ok := ordinary[file.Path]
			if !ok || used[index] {
				return errors.New("diff capture manifest file list does not match captured revisions")
			}
			used[index] = true
			expected = entries[index]
		} else if index, ok := moved[[2]string{file.PreviousPath, file.Path}]; ok && !used[index] {
			used[index] = true
			expected = entries[index]
		} else if file.ChangeType == "copied" {
			index, ok := ordinary[file.Path]
			if !ok || used[index] || !entries[index].isBinary && entries[index].diffStat.Deleted != 0 {
				return errors.New("diff capture manifest file list does not match captured revisions")
			}
			used[index] = true
			expected = entries[index]
		} else {
			oldIndex, oldOK := ordinary[file.PreviousPath]
			newIndex, newOK := ordinary[file.Path]
			if !oldOK || !newOK || oldIndex == newIndex || used[oldIndex] || used[newIndex] {
				return errors.New("diff capture manifest file list does not match captured revisions")
			}
			oldEntry := entries[oldIndex]
			newEntry := entries[newIndex]
			if !oldEntry.isBinary && oldEntry.diffStat.Added != 0 || !newEntry.isBinary && newEntry.diffStat.Deleted != 0 {
				return errors.New("diff capture structural rename does not match delete and add numstat entries")
			}
			used[oldIndex] = true
			used[newIndex] = true
			combined, err := neoDiffCaptureCombineNumStats(oldEntry, newEntry)
			if err != nil {
				return err
			}
			expected = combined
		}
		if file.PreviousPath != "" && base[file.PreviousPath] == head[file.Path] {
			expected.diffStat = neoDiffCaptureDiffStat{}
		}
		if file.IsBinary != expected.isBinary || file.DiffStat != expected.diffStat {
			return fmt.Errorf("diff capture file %q has incorrect display metadata", file.Path)
		}
	}
	for _, entryUsed := range used {
		if !entryUsed {
			return errors.New("diff capture manifest file list does not match captured revisions")
		}
	}
	return nil
}

func neoDiffCaptureCombineNumStats(oldEntry, newEntry neoDiffCaptureNumStatEntry) (neoDiffCaptureNumStatEntry, error) {
	combined := neoDiffCaptureNumStatEntry{isBinary: oldEntry.isBinary || newEntry.isBinary}
	if combined.isBinary {
		combined.diffStat.Changed = 1
		return combined, nil
	}
	maxInt := int(^uint(0) >> 1)
	if oldEntry.diffStat.Added > maxInt-newEntry.diffStat.Added || oldEntry.diffStat.Deleted > maxInt-newEntry.diffStat.Deleted {
		return neoDiffCaptureNumStatEntry{}, errors.New("diff capture combined numstat exceeds local limit")
	}
	combined.diffStat.Added = oldEntry.diffStat.Added + newEntry.diffStat.Added
	combined.diffStat.Deleted = oldEntry.diffStat.Deleted + newEntry.diffStat.Deleted
	combined.diffStat.Changed = max(combined.diffStat.Added, combined.diffStat.Deleted)
	return combined, nil
}

func neoDiffCaptureValidateManifestFile(file neoDiffCaptureManifestFile, replacedPaths map[string]struct{}, base, head map[string]neoDiffCaptureTreeFile) error {
	oldPath := file.Path
	if file.PreviousPath != "" {
		oldPath = file.PreviousPath
	}
	oldFile, oldExists := base[oldPath]
	newFile, newExists := head[file.Path]
	if err := neoDiffCaptureValidateManifestFileSide(file.Old, file.OldBlobSHA, oldPath, oldFile, oldExists); err != nil {
		return err
	}
	if err := neoDiffCaptureValidateManifestFileSide(file.New, file.NewBlobSHA, file.Path, newFile, newExists); err != nil {
		return err
	}
	expectedChangeType := ""
	if file.PreviousPath != "" {
		if !oldExists || !newExists {
			return fmt.Errorf("diff capture file %q does not exist at its claimed paths", file.Path)
		}
		if _, destinationExisted := base[file.Path]; destinationExisted {
			return fmt.Errorf("diff capture file %q existed before its claimed move", file.Path)
		}
		expectedChangeType = "renamed"
		if sourceInHead, sourceStillExists := head[file.PreviousPath]; sourceStillExists {
			if sourceInHead != oldFile {
				if _, replaced := replacedPaths[file.PreviousPath]; !replaced {
					return fmt.Errorf("diff capture file %q has an unrepresented changed source", file.Path)
				}
			} else {
				if newFile != oldFile {
					return fmt.Errorf("diff capture file %q does not preserve its copied source", file.Path)
				}
				expectedChangeType = "copied"
			}
		}
	} else {
		switch {
		case !oldExists && newExists:
			expectedChangeType = "added"
		case oldExists && !newExists:
			expectedChangeType = "deleted"
		case oldExists && newExists && (oldFile.sha != newFile.sha || oldFile.mode != newFile.mode):
			expectedChangeType = "modified"
			if len(oldFile.mode) >= 3 && len(newFile.mode) >= 3 && oldFile.mode[:3] != newFile.mode[:3] {
				expectedChangeType = "type_changed"
			}
		default:
			return fmt.Errorf("diff capture file %q is not changed", file.Path)
		}
	}
	if file.ChangeType != expectedChangeType && !(expectedChangeType == "added" && file.ChangeType == "untracked") {
		return fmt.Errorf("diff capture file %q has change type %q, want %q", file.Path, file.ChangeType, expectedChangeType)
	}
	return nil
}

func neoDiffCaptureValidateManifestFileSide(side neoDiffCaptureFileSide, legacySHA, expectedPath string, actual neoDiffCaptureTreeFile, exists bool) error {
	if side.Path != "" && side.Path != expectedPath {
		return fmt.Errorf("diff capture file side path %q does not match %q", side.Path, expectedPath)
	}
	legacySHA = strings.ToLower(strings.TrimSpace(legacySHA))
	sideSHA := strings.ToLower(strings.TrimSpace(side.BlobSHA))
	if legacySHA != "" && sideSHA != "" && legacySHA != sideSHA {
		return errors.New("diff capture file side blob shas disagree")
	}
	claimedSHA := firstNonEmptyString(legacySHA, sideSHA)
	if exists {
		if side.Kind != "" && side.Kind != "file" {
			return fmt.Errorf("diff capture file side %q is not a file", expectedPath)
		}
		if claimedSHA != actual.sha {
			return fmt.Errorf("diff capture file side %q references the wrong blob", expectedPath)
		}
		return nil
	}
	if side.Kind != "" && side.Kind != "absent" {
		return fmt.Errorf("diff capture file side %q is not absent", expectedPath)
	}
	if claimedSHA != "" {
		return fmt.Errorf("diff capture absent file side %q references a blob", expectedPath)
	}
	return nil
}

func neoDiffCaptureSafeSubset(record neoDiffCaptureRecord, manifest neoDiffCaptureManifest) map[string]any {
	capturedAt, _ := neoDiffCaptureSafeCapturedAt(manifest.CapturedAt)
	branch := any(nil)
	if strings.TrimSpace(manifest.Source.Branch) != "" {
		branch = manifest.Source.Branch
	}
	ranges := make([]any, 0, len(manifest.RangeSections)+1)
	headSections := make([]any, 0, len(manifest.Sections))
	for _, section := range manifest.Sections {
		headSections = append(headSections, map[string]any{
			"id":    section.ID,
			"label": section.Label,
			"files": neoDiffCaptureSafeFiles(section.Files),
		})
	}
	ranges = append(ranges, map[string]any{
		"kind":     "head",
		"id":       "head",
		"label":    "Working tree",
		"sections": headSections,
	})
	commitIndex := make(map[string]int, len(manifest.ProjectedCommits))
	for index, commit := range manifest.ProjectedCommits {
		commitIndex[strings.ToLower(commit.SourceCommitSHA)] = index
	}
	for _, section := range manifest.RangeSections {
		rangeSections := []any{map[string]any{
			"id":    section.RangeID,
			"label": section.Label,
			"files": neoDiffCaptureSafeFiles(section.Files),
		}}
		switch {
		case strings.HasPrefix(section.RangeID, "merge-base:"):
			baseSHA := strings.TrimPrefix(section.RangeID, "merge-base:")
			if !neoDiffCaptureSHApattern.MatchString(baseSHA) {
				continue
			}
			ranges = append(ranges, map[string]any{
				"kind":          "all",
				"id":            section.RangeID,
				"label":         section.Label,
				"baseSourceSHA": baseSHA,
				"sections":      rangeSections,
			})
		case strings.HasPrefix(section.RangeID, "commit:"):
			headSHA := strings.TrimPrefix(section.RangeID, "commit:")
			if !neoDiffCaptureSHApattern.MatchString(headSHA) {
				continue
			}
			baseSHA := manifest.Source.MergeBaseSHA
			subject := ""
			if index, ok := commitIndex[strings.ToLower(headSHA)]; ok {
				subject = manifest.ProjectedCommits[index].Subject
				if index > 0 {
					baseSHA = manifest.ProjectedCommits[index-1].SourceCommitSHA
				}
			}
			if !neoDiffCaptureSHApattern.MatchString(baseSHA) {
				continue
			}
			ranges = append(ranges, map[string]any{
				"kind":          "commit",
				"id":            section.RangeID,
				"label":         section.Label,
				"baseSourceSHA": baseSHA,
				"headSourceSHA": headSHA,
				"shortSHA":      firstN(headSHA, 7),
				"subject":       subject,
				"isHead":        strings.EqualFold(headSHA, manifest.Source.HeadSHA),
				"sections":      rangeSections,
			})
		}
	}
	return map[string]any{
		"version":    3,
		"captureID":  record.CaptureID,
		"capturedAt": capturedAt,
		"branch":     branch,
		"ranges":     ranges,
	}
}

func neoDiffCaptureSafeFiles(files []neoDiffCaptureManifestFile) []any {
	out := make([]any, 0, len(files))
	for _, file := range files {
		value := map[string]any{
			"id":         file.ID,
			"path":       file.Path,
			"changeType": file.ChangeType,
			"isBinary":   file.IsBinary,
			"diffStat": map[string]any{
				"added":   file.DiffStat.Added,
				"deleted": file.DiffStat.Deleted,
				"changed": file.DiffStat.Changed,
			},
		}
		if file.PreviousPath != "" {
			value["previousPath"] = file.PreviousPath
		}
		oldBlobSHA := firstNonEmptyString(file.OldBlobSHA, file.Old.BlobSHA)
		newBlobSHA := firstNonEmptyString(file.NewBlobSHA, file.New.BlobSHA)
		if neoDiffCaptureSHApattern.MatchString(oldBlobSHA) {
			value["oldBlobSHA"] = strings.ToLower(oldBlobSHA)
		}
		if neoDiffCaptureSHApattern.MatchString(newBlobSHA) {
			value["newBlobSHA"] = strings.ToLower(newBlobSHA)
		}
		out = append(out, value)
	}
	return out
}

func neoDiffCaptureLock(ctx context.Context, threadID string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	neoDiffCaptureLocks.mu.Lock()
	if neoDiffCaptureLocks.entries == nil {
		neoDiffCaptureLocks.entries = make(map[string]*neoDiffCaptureLockEntry)
	}
	entry := neoDiffCaptureLocks.entries[threadID]
	if entry == nil {
		entry = &neoDiffCaptureLockEntry{token: make(chan struct{}, 1)}
		entry.token <- struct{}{}
		neoDiffCaptureLocks.entries[threadID] = entry
	}
	entry.refs++
	neoDiffCaptureLocks.mu.Unlock()
	select {
	case <-ctx.Done():
		neoDiffCaptureReleaseLockRef(threadID, entry)
		return nil, ctx.Err()
	case <-entry.token:
	}
	if err := ctx.Err(); err != nil {
		entry.token <- struct{}{}
		neoDiffCaptureReleaseLockRef(threadID, entry)
		return nil, err
	}
	return func() {
		entry.token <- struct{}{}
		neoDiffCaptureReleaseLockRef(threadID, entry)
	}, nil
}

func neoDiffCaptureLockRelease(unlock func()) func() {
	var once sync.Once
	return func() {
		once.Do(unlock)
	}
}

func neoDiffCaptureWriteJSON(c *gin.Context, release func(), status int, body any) {
	release()
	c.JSON(status, body)
}

func neoDiffCaptureWriteStatus(c *gin.Context, release func(), status int) {
	release()
	c.Status(status)
}

func neoDiffCaptureReleaseLockRef(threadID string, entry *neoDiffCaptureLockEntry) {
	neoDiffCaptureLocks.mu.Lock()
	entry.refs--
	if entry.refs == 0 && neoDiffCaptureLocks.entries[threadID] == entry {
		delete(neoDiffCaptureLocks.entries, threadID)
	}
	neoDiffCaptureLocks.mu.Unlock()
}

func neoDiffCaptureRootDir() string {
	dataDir := strings.TrimSpace(neoAmpDataDir())
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, "diff-captures")
}

func neoDiffCaptureThreadDir(threadID string) string {
	if !neoThreadIDExactPattern.MatchString(threadID) || neoDiffCaptureRootDir() == "" {
		return ""
	}
	return filepath.Join(neoDiffCaptureRootDir(), threadID)
}

func neoDiffCaptureRepositoryDir(threadID string) string {
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(threadDir, "repository.git")
}

func neoDiffCaptureRecordPath(threadID, captureID string) string {
	capturesDir := neoDiffCaptureCapturesDir(threadID)
	if capturesDir == "" || !neoDiffCaptureIDPattern.MatchString(captureID) {
		return ""
	}
	return filepath.Join(capturesDir, captureID+".json")
}

func neoDiffCaptureCapturesDir(threadID string) string {
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(threadDir, "captures")
}

func neoDiffCaptureBlobIndexPath(threadID string) string {
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(threadDir, "blob-index.json")
}

func neoDiffCaptureLatestPath(threadID string) string {
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(threadDir, "latest.json")
}

func neoDiffCapturePendingPublicationPath(threadID string) string {
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(threadDir, "pending-publication.json")
}

func neoDiffCapturePendingPrunePath(threadID string) string {
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return ""
	}
	return filepath.Join(threadDir, "pending-prune.json")
}

func neoEnsureDiffCaptureRepository(ctx context.Context, threadID, objectFormat string) (string, error) {
	if objectFormat != "sha1" && objectFormat != "sha256" {
		return "", fmt.Errorf("unsupported diff capture Git object format %q", objectFormat)
	}
	threadDir := neoDiffCaptureThreadDir(threadID)
	if threadDir == "" {
		return "", errors.New("diff capture data directory unavailable")
	}
	capturesDir := neoDiffCaptureCapturesDir(threadID)
	for _, dir := range []string{neoDiffCaptureRootDir(), threadDir, capturesDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return "", err
		}
	}
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	if _, err := os.Stat(repositoryDir); errors.Is(err, os.ErrNotExist) {
		if _, err := neoDiffCaptureGit(ctx, threadDir, []string{"init", "--bare", "--object-format=" + objectFormat, filepath.Base(repositoryDir)}, 4096, nil); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	if err := os.Chmod(repositoryDir, 0o700); err != nil {
		return "", err
	}
	isBare, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"rev-parse", "--is-bare-repository"}, 128, nil)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(isBare) != "true" {
		return "", errors.New("invalid diff capture Git repository")
	}
	repositoryObjectFormat, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"rev-parse", "--show-object-format"}, 128, nil)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(repositoryObjectFormat) != objectFormat {
		return "", errors.New("diff capture Git object format mismatch")
	}
	for key, value := range map[string]string{
		"gc.auto":                     "0",
		"receive.hideRefs":            "refs/amp/diff-captures",
		"receive.maxInputSize":        strconv.Itoa(neoDiffCaptureReceiveMaxBytes),
		"receive.denyDeletes":         "true",
		"receive.denyNonFastForwards": "true",
	} {
		if _, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"config", "--local", key, value}, 128, nil); err != nil {
			return "", err
		}
	}
	if err := neoInstallDiffCaptureReceiveHook(repositoryDir); err != nil {
		return "", err
	}
	return repositoryDir, nil
}

func neoInstallDiffCaptureReceiveHook(repositoryDir string) error {
	hooksDir := filepath.Join(repositoryDir, "hooks")
	if err := os.MkdirAll(hooksDir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(hooksDir, 0o700); err != nil {
		return err
	}
	script := fmt.Sprintf(`#!/bin/sh
set -eu
git_dir=$(git rev-parse --absolute-git-dir)
set -- $(du -sk "$git_dir")
repository_kib=$1
quarantine=${GIT_QUARANTINE_PATH:-}
if [ -n "$quarantine" ]; then
	case "$quarantine" in
		"$git_dir"/*) ;;
		*) set -- $(du -sk "$quarantine"); repository_kib=$((repository_kib + $1)) ;;
	esac
fi
if [ "$repository_kib" -gt %d ]; then
	echo "diff capture repository exceeds local limit" >&2
	exit 1
fi
while read -r old_sha new_sha ref
do
	case "$ref" in
		refs/heads/amp/captures/*) capture_id=${ref#refs/heads/amp/captures/} ;;
		*) echo "unsupported diff capture ref" >&2; exit 1 ;;
	esac
	case "$capture_id" in
		''|*[!0-9A-Za-z]*) echo "invalid diff capture ref" >&2; exit 1 ;;
	esac
	if [ ${#capture_id} -lt 16 ] || [ ${#capture_id} -gt 64 ]; then
		echo "invalid diff capture ref" >&2
		exit 1
	fi
	record="$git_dir/../captures/$capture_id.json"
	if [ ! -f "$record" ] || ! grep -q '"state":"allocated"}$' "$record"; then
		echo "diff capture ref is not allocated" >&2
		exit 1
	fi
done
for receive_owner in "$git_dir"/amp-receive-owner-"$PPID"-*
do
	[ -f "$receive_owner" ] || continue
	owner_token=$(cat "$receive_owner" 2>/dev/null || true)
	case "$owner_token" in
		''|*[!0-9A-Za-z-]*) ;;
		*) rm -f "$git_dir/amp-receive-active-$owner_token" ;;
	esac
	rm -f "$receive_owner"
done
rm -f "$git_dir"/amp-receive-pending-"$PPID"-*
token="$PPID-$$-$(date +%%s)"
pending="$git_dir/amp-receive-pending-$token"
pending_temp="$pending.tmp-$$"
trap 'rm -f "$pending_temp"' 0 1 2 3 15
printf '%%s\n' "$token" > "$pending_temp"
mv -f "$pending_temp" "$pending"
trap - 0 1 2 3 15
`, neoDiffCaptureRepositoryMaxBytes/1024)
	path := filepath.Join(hooksDir, "pre-receive")
	if err := writeNeoDurableAtomicFile(path, []byte(script), 0o700); err != nil {
		return err
	}
	transactionScript := `#!/bin/sh
set -eu
git_dir=$(git rev-parse --absolute-git-dir)
pending=
receive_owner=
receive_active=
owner_temp=
active_temp=
cleanup_receive() {
	if [ -z "$receive_active" ] && [ -n "$receive_owner" ]; then
		token=$(cat "$receive_owner" 2>/dev/null || true)
		case "$token" in
			''|*[!0-9A-Za-z-]*) ;;
			*) receive_active="$git_dir/amp-receive-active-$token" ;;
		esac
	fi
	if [ -n "$receive_active" ]; then
		rm -f "$receive_active"
	fi
	if [ -n "$owner_temp" ]; then
		rm -f "$owner_temp"
	fi
	if [ -n "$active_temp" ]; then
		rm -f "$active_temp"
	fi
	if [ -n "$receive_owner" ]; then
		rm -f "$receive_owner"
	fi
	if [ -n "$pending" ]; then
		rm -f "$pending"
	fi
}
marker_is_fresh() {
	marker=$1
	if [ ! -e "$marker" ]; then
		return 1
	fi
	if fresh=$(find "$marker" -prune -mmin -1 -print 2>/dev/null); then
		[ -n "$fresh" ]
		return
	fi
	return 0
}
case "$1" in
	prepared)
		for pending_candidate in "$git_dir"/amp-receive-pending-"$PPID"-*
		do
			[ -f "$pending_candidate" ] || continue
			pending=$pending_candidate
			break
		done
		if [ -z "$pending" ]; then
			exit 0
		fi
		token=$(cat "$pending" 2>/dev/null || true)
		case "$token" in
			''|*[!0-9A-Za-z-]*) echo "invalid diff capture receive state" >&2; exit 1 ;;
		esac
		receive_owner="$git_dir/amp-receive-owner-$token"
		receive_active="$git_dir/amp-receive-active-$token"
		release_lock=1
		trap 'if [ "$release_lock" -eq 1 ]; then cleanup_receive; fi' 0 1 2 3 15
		owner_temp="$receive_owner.tmp-$$"
		printf '%s\n' "$token" > "$owner_temp"
		mv -f "$owner_temp" "$receive_owner"
		active_temp="$receive_active.tmp-$$"
		printf '%s\n' "$PPID" > "$active_temp"
		mv -f "$active_temp" "$receive_active"
		for api_owner in "$git_dir"/amp-api-active-*
		do
			[ -e "$api_owner" ] || continue
			if marker_is_fresh "$api_owner"; then
				echo "diff capture operation is active" >&2
				exit 1
			fi
		done
		while read -r old_sha new_sha ref
		do
			case "$ref" in
				refs/heads/amp/captures/*) capture_id=${ref#refs/heads/amp/captures/} ;;
				*) echo "unsupported diff capture ref" >&2; exit 1 ;;
			esac
			record="$git_dir/../captures/$capture_id.json"
			if [ ! -f "$record" ] || ! grep -q '"state":"allocated"}$' "$record"; then
				echo "diff capture ref is not allocated" >&2
				exit 1
			fi
		done
		rm -f "$pending"
		release_lock=0
		trap - 0 1 2 3 15
		;;
	aborted)
		for receive_owner in "$git_dir"/amp-receive-owner-"$PPID"-*
		do
			[ -f "$receive_owner" ] || continue
			token=$(cat "$receive_owner" 2>/dev/null || true)
			case "$token" in
				''|*[!0-9A-Za-z-]*) ;;
				*) rm -f "$git_dir/amp-receive-active-$token" ;;
			esac
			rm -f "$receive_owner"
		done
		rm -f "$git_dir"/amp-receive-pending-"$PPID"-*
		;;
esac
`
	path = filepath.Join(hooksDir, "reference-transaction")
	if err := writeNeoDurableAtomicFile(path, []byte(transactionScript), 0o700); err != nil {
		return err
	}
	postReceiveScript := `#!/bin/sh
set -eu
git_dir=$(git rev-parse --absolute-git-dir)
for receive_owner in "$git_dir"/amp-receive-owner-"$PPID"-*
do
	[ -f "$receive_owner" ] || continue
	token=$(cat "$receive_owner" 2>/dev/null || true)
	case "$token" in
		''|*[!0-9A-Za-z-]*) ;;
		*) rm -f "$git_dir/amp-receive-active-$token" ;;
	esac
	rm -f "$receive_owner"
done
rm -f "$git_dir"/amp-receive-pending-"$PPID"-*
`
	path = filepath.Join(hooksDir, "post-receive")
	if err := writeNeoDurableAtomicFile(path, []byte(postReceiveScript), 0o700); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func neoAcquireDiffCaptureReceiveLock(threadID string) (func(), error) {
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	if repositoryDir == "" {
		return nil, errors.New("diff capture Git directory unavailable")
	}
	mutexPath := filepath.Join(repositoryDir, "amp-api-mutex")
	for attempt := 0; attempt < 8; attempt++ {
		mutex, err := neoOpenDiffCaptureLockFile(mutexPath)
		if err != nil {
			return nil, err
		}
		locked, err := neoTryLockDiffCaptureFile(mutex)
		if err != nil {
			_ = mutex.Close()
			return nil, err
		}
		if !locked {
			_ = mutex.Close()
			return nil, errNeoDiffCaptureReceiveActive
		}
		mutexInfo, errMutexInfo := mutex.Stat()
		pathInfo, errPathInfo := os.Stat(mutexPath)
		if errMutexInfo != nil || errPathInfo != nil || !os.SameFile(mutexInfo, pathInfo) {
			errRelease := neoReleaseDiffCaptureFile(mutex)
			if errMutexInfo != nil {
				return nil, errors.Join(errMutexInfo, errRelease)
			}
			if errPathInfo != nil && !errors.Is(errPathInfo, os.ErrNotExist) {
				return nil, errors.Join(errPathInfo, errRelease)
			}
			if errRelease != nil {
				return nil, errRelease
			}
			continue
		}
		if err := neoRemoveDiffCaptureAPIMarkers(repositoryDir); err != nil {
			return nil, errors.Join(err, neoReleaseDiffCaptureFile(mutex))
		}
		if err := neoRemoveStaleDiffCaptureReceiveMetadata(repositoryDir, time.Now()); err != nil {
			return nil, errors.Join(err, neoReleaseDiffCaptureFile(mutex))
		}
		owner := fmt.Sprintf("%d-%d-%d", os.Getpid(), time.Now().UnixNano(), neoDiffCaptureReceiveSequence.Add(1))
		ownerPath := filepath.Join(repositoryDir, "amp-api-active-"+owner)
		if err := neoWriteDiffCaptureOwner(ownerPath, owner); err != nil {
			return nil, errors.Join(err, neoReleaseDiffCaptureFile(mutex))
		}
		active, err := neoDiffCaptureReceiveIsActive(repositoryDir, time.Now())
		if err != nil || active {
			errRemove := neoRemoveDiffCaptureOwner(ownerPath, owner)
			errRelease := neoReleaseDiffCaptureFile(mutex)
			if err != nil {
				return nil, errors.Join(err, errRemove, errRelease)
			}
			return nil, errors.Join(errNeoDiffCaptureReceiveActive, errRemove, errRelease)
		}
		stopHeartbeat := make(chan struct{})
		heartbeatDone := make(chan struct{})
		go func() {
			defer close(heartbeatDone)
			ticker := time.NewTicker(neoDiffCaptureLockHeartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					matches, errMatch := neoDiffCaptureOwnerMatches(ownerPath, owner)
					if errMatch != nil {
						logNeoDiffCaptureError("API receive lock heartbeat", threadID, "", errMatch)
						continue
					}
					if !matches {
						return
					}
					now := time.Now()
					if errChtimes := os.Chtimes(ownerPath, now, now); errChtimes != nil {
						logNeoDiffCaptureError("API receive lock heartbeat", threadID, "", errChtimes)
						continue
					}
				case <-stopHeartbeat:
					return
				}
			}
		}()
		var once sync.Once
		return func() {
			once.Do(func() {
				close(stopHeartbeat)
				<-heartbeatDone
				if errRemove := neoRemoveDiffCaptureOwner(ownerPath, owner); errRemove != nil {
					logNeoDiffCaptureError("API receive lock owner release", threadID, "", errRemove)
				}
				if errRelease := neoReleaseDiffCaptureFile(mutex); errRelease != nil {
					logNeoDiffCaptureError("API receive lock release", threadID, "", errRelease)
				}
			})
		}, nil
	}
	return nil, errors.New("diff capture API lock path changed during acquisition")
}

func neoReleaseDiffCaptureFile(file *os.File) error {
	return errors.Join(neoUnlockDiffCaptureFile(file), file.Close())
}

func neoWriteDiffCaptureOwner(path, owner string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, errWrite := file.WriteString(owner + "\n")
	errClose := file.Close()
	if errWrite != nil || errClose != nil {
		errRemove := os.Remove(path)
		if errors.Is(errRemove, os.ErrNotExist) {
			errRemove = nil
		}
		return errors.Join(errWrite, errClose, errRemove)
	}
	return nil
}

func neoDiffCaptureOwnerMatches(path, owner string) (bool, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(string(raw)) == owner, nil
}

func neoRemoveDiffCaptureOwner(path, owner string) error {
	matches, err := neoDiffCaptureOwnerMatches(path, owner)
	if err != nil || !matches {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func neoRemoveDiffCaptureAPIMarkers(repositoryDir string) error {
	entries, err := os.ReadDir(repositoryDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "amp-api-active-") {
			continue
		}
		if err := os.Remove(filepath.Join(repositoryDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func neoRemoveStaleDiffCaptureReceiveMetadata(repositoryDir string, now time.Time) error {
	entries, err := os.ReadDir(repositoryDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "amp-receive-pending-") && !strings.HasPrefix(entry.Name(), "amp-receive-owner-") {
			continue
		}
		info, errInfo := entry.Info()
		if errors.Is(errInfo, os.ErrNotExist) {
			continue
		}
		if errInfo != nil {
			return errInfo
		}
		if now.Sub(info.ModTime()) < neoDiffCaptureLockStaleAfter {
			continue
		}
		if errRemove := os.Remove(filepath.Join(repositoryDir, entry.Name())); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return errRemove
		}
	}
	return nil
}

func neoDiffCaptureReceiveIsActive(repositoryDir string, now time.Time) (bool, error) {
	entries, err := os.ReadDir(repositoryDir)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "amp-receive-active-") {
			continue
		}
		path := filepath.Join(repositoryDir, entry.Name())
		info, errInfo := entry.Info()
		if errors.Is(errInfo, os.ErrNotExist) {
			continue
		}
		if errInfo != nil {
			return false, errInfo
		}
		fresh := now.Sub(info.ModTime()) < neoDiffCaptureLockStaleAfter
		if fresh && info.Mode().IsRegular() {
			raw, errRead := os.ReadFile(path)
			if errors.Is(errRead, os.ErrNotExist) {
				continue
			}
			if errRead != nil {
				return true, nil
			}
			pid, errPID := strconv.Atoi(strings.TrimSpace(string(raw)))
			if errPID != nil {
				return true, nil
			}
			alive, known := neoProcessStatus(pid)
			if !known || alive {
				return true, nil
			}
		} else if fresh {
			return true, nil
		}
		if errRemove := os.Remove(path); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
			return false, errRemove
		}
	}
	return false, nil
}

func neoDiffCaptureGit(ctx context.Context, cwd string, args []string, maxOutputBytes int, stdin io.Reader) (string, error) {
	if strings.TrimSpace(cwd) == "" {
		return "", errors.New("diff capture Git directory unavailable")
	}
	stdout := neoGitOutputBuffer{limit: maxOutputBytes}
	if err := neoChangesFileOrderGitWrite(ctx, cwd, args, false, &stdout, stdin, neoGitCommandEnv()); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return stdout.String(), nil
}

func writeNeoDiffCaptureRecord(path string, record neoDiffCaptureRecord) error {
	raw, err := marshalNeoDiffCaptureRecord(record)
	if err != nil {
		return err
	}
	return writeNeoDiffCaptureRecordRaw(path, raw)
}

func marshalNeoDiffCaptureRecord(record neoDiffCaptureRecord) ([]byte, error) {
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	if len(raw) > neoDiffCaptureRecordMaxBytes {
		return nil, errors.New("diff capture metadata exceeds local limit")
	}
	return raw, nil
}

func writeNeoDiffCaptureRecordRaw(path string, raw []byte) error {
	if path == "" {
		return errors.New("invalid diff capture metadata path")
	}
	if len(raw) > neoDiffCaptureRecordMaxBytes {
		return errors.New("diff capture metadata exceeds local limit")
	}
	if err := writeNeoDurableAtomicFile(path, raw, 0o600); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func readNeoDiffCaptureRecord(threadID, captureID string) (neoDiffCaptureRecord, error) {
	path := neoDiffCaptureRecordPath(threadID, captureID)
	if path == "" {
		return neoDiffCaptureRecord{}, os.ErrNotExist
	}
	file, err := os.Open(path)
	if err != nil {
		return neoDiffCaptureRecord{}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			logNeoDiffCaptureError("record close", threadID, captureID, err)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return neoDiffCaptureRecord{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > neoDiffCaptureRecordMaxBytes {
		return neoDiffCaptureRecord{}, errors.New("invalid diff capture metadata")
	}
	raw, err := io.ReadAll(io.LimitReader(file, neoDiffCaptureRecordMaxBytes+1))
	if err != nil {
		return neoDiffCaptureRecord{}, err
	}
	if len(raw) > neoDiffCaptureRecordMaxBytes {
		return neoDiffCaptureRecord{}, errors.New("invalid diff capture metadata")
	}
	var record neoDiffCaptureRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return neoDiffCaptureRecord{}, err
	}
	if record.StorageVersion != 1 || record.CaptureID != captureID || record.CaptureRef != "refs/heads/amp/captures/"+captureID || record.PublishedRef != "" && record.PublishedRef != neoDiffCapturePublishedRef(captureID) {
		return neoDiffCaptureRecord{}, errors.New("invalid diff capture metadata")
	}
	return record, nil
}

func neoDiffCaptureRecordStorageUsage(threadID string) (neoDiffCaptureRecordUsage, error) {
	usage := neoDiffCaptureRecordUsage{fileBytes: make(map[string]int64)}
	dir := neoDiffCaptureCapturesDir(threadID)
	if dir == "" {
		return usage, errors.New("invalid diff capture directory")
	}
	directory, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return usage, nil
	}
	if err != nil {
		return usage, err
	}
	defer func() {
		if err := directory.Close(); err != nil {
			logNeoDiffCaptureError("capture storage directory close", threadID, "", err)
		}
	}()
	for {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			captureID := strings.TrimSuffix(entry.Name(), ".json")
			if !neoDiffCaptureIDPattern.MatchString(captureID) {
				continue
			}
			usage.recordCount++
			if usage.recordCount > neoDiffCaptureScanMaxRecords {
				usage.overLimit = true
				return usage, nil
			}
			info, err := entry.Info()
			if err != nil {
				return usage, err
			}
			usage.recordBytes += info.Size()
			usage.fileBytes[captureID] = info.Size()
			if usage.recordBytes > neoDiffCaptureScanMaxBytes {
				usage.overLimit = true
				return usage, nil
			}
			if _, err := readNeoDiffCaptureRecord(threadID, captureID); err != nil {
				return usage, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return usage, readErr
		}
	}
	return usage, nil
}

func neoPruneDiffCaptureStorage(ctx context.Context, threadID, keepCaptureID string, forceGC bool) error {
	if _, err := completeNeoDiffCapturePendingPrune(ctx, threadID, forceGC); err != nil {
		return err
	}
	dir := neoDiffCaptureCapturesDir(threadID)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return neoBoundDiffCaptureRepository(ctx, threadID, forceGC)
	}
	if err != nil {
		return err
	}
	pendingCaptureID := ""
	pending, err := readNeoDiffCapturePendingPublication(threadID)
	if err == nil {
		pendingCaptureID = pending.CaptureID
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	records := make([]neoDiffCaptureRetentionRecord, 0, len(entries))
	published := make([]neoDiffCaptureRetentionRecord, 0, len(entries))
	allocated := make([]neoDiffCaptureRetentionRecord, 0, len(entries))
	var recordBytes int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		captureID := strings.TrimSuffix(entry.Name(), ".json")
		if !neoDiffCaptureIDPattern.MatchString(captureID) {
			continue
		}
		if len(records) >= neoDiffCaptureScanMaxRecords {
			return fmt.Errorf("%w: record limit", errNeoDiffCaptureStorageLimit)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		recordBytes += info.Size()
		if recordBytes > neoDiffCaptureScanMaxBytes {
			return fmt.Errorf("%w: byte limit", errNeoDiffCaptureStorageLimit)
		}
		record, err := readNeoDiffCaptureRecord(threadID, captureID)
		if err != nil {
			return err
		}
		var timestamp time.Time
		switch record.State {
		case "published":
			timestamp, _, err = validateNeoDiffCapturePublishedRecord(record)
		case "allocated":
			timestamp, err = time.Parse(time.RFC3339Nano, record.CreatedAt)
			if err == nil && (record.PublishedRef != "" || record.PublishedAt != "" || record.ManifestSHA256 != "" || len(record.Manifest) != 0) {
				err = errors.New("invalid allocated diff capture metadata")
			}
		default:
			err = errors.New("invalid diff capture state")
		}
		if err != nil {
			return err
		}
		candidate := neoDiffCaptureRetentionRecord{record: record, timestamp: timestamp}
		records = append(records, candidate)
		if record.State == "published" {
			published = append(published, candidate)
		} else {
			allocated = append(allocated, candidate)
		}
	}
	sort.Slice(published, func(i, j int) bool {
		if published[i].timestamp.Equal(published[j].timestamp) {
			return published[i].record.CaptureID > published[j].record.CaptureID
		}
		return published[i].timestamp.After(published[j].timestamp)
	})
	sort.Slice(allocated, func(i, j int) bool {
		if allocated[i].timestamp.Equal(allocated[j].timestamp) {
			return allocated[i].record.CaptureID > allocated[j].record.CaptureID
		}
		return allocated[i].timestamp.After(allocated[j].timestamp)
	})
	retained := make(map[string]struct{}, neoDiffCaptureRetainedPublished+neoDiffCaptureRetainedAllocations+2)
	for _, candidates := range [][]neoDiffCaptureRetentionRecord{published, allocated} {
		limit := neoDiffCaptureRetainedPublished
		if len(candidates) != 0 && candidates[0].record.State == "allocated" {
			limit = neoDiffCaptureRetainedAllocations
		}
		count := 0
		for _, candidate := range candidates {
			captureID := candidate.record.CaptureID
			protected := captureID == keepCaptureID || captureID == pendingCaptureID
			if candidate.record.State == "allocated" && !protected && time.Since(candidate.timestamp) >= neoDiffCaptureAllocationTTL {
				continue
			}
			if protected || count < limit {
				retained[captureID] = struct{}{}
				count++
			}
		}
	}
	prunedRecords := make([]neoDiffCaptureRecord, 0)
	for _, candidate := range records {
		if _, ok := retained[candidate.record.CaptureID]; ok {
			continue
		}
		prunedRecords = append(prunedRecords, candidate.record)
	}
	if len(prunedRecords) != 0 {
		captureIDs := make([]string, len(prunedRecords))
		for index, record := range prunedRecords {
			captureIDs[index] = record.CaptureID
		}
		if err := writeNeoDiffCapturePendingPrune(threadID, captureIDs); err != nil {
			return err
		}
		if _, err := completeNeoDiffCapturePendingPrune(ctx, threadID, forceGC); err != nil {
			return err
		}
		return nil
	}
	return neoBoundDiffCaptureRepository(ctx, threadID, forceGC)
}

func completeNeoDiffCapturePendingPrune(ctx context.Context, threadID string, forceGC bool) (bool, error) {
	pending, err := readNeoDiffCapturePendingPrune(threadID)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := removeNeoDiffCaptureLatest(threadID); err != nil {
		return false, err
	}
	if err := removeNeoDiffCaptureBlobIndex(threadID); err != nil {
		return false, err
	}
	neoDiffCaptureBlobIndexes.delete(threadID)
	for _, captureID := range pending.CaptureIDs {
		if err := os.Remove(neoDiffCaptureRecordPath(threadID, captureID)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if err := syncNeoDurableDirectory(neoDiffCaptureCapturesDir(threadID)); err != nil {
		return false, err
	}
	var updates strings.Builder
	for _, captureID := range pending.CaptureIDs {
		updates.WriteString("delete refs/heads/amp/captures/")
		updates.WriteString(captureID)
		updates.WriteByte('\n')
		updates.WriteString("delete ")
		updates.WriteString(neoDiffCapturePublishedRef(captureID))
		updates.WriteByte('\n')
	}
	if _, err := neoDiffCaptureGit(ctx, neoDiffCaptureRepositoryDir(threadID), []string{"update-ref", "--stdin"}, 4096, strings.NewReader(updates.String())); err != nil {
		return false, err
	}
	if _, _, _, err := rebuildNeoDiffCaptureLatest(threadID); err != nil {
		return false, err
	}
	index, err := rebuildNeoDiffCaptureBlobIndex(threadID)
	if err != nil {
		return false, err
	}
	raw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil {
		return false, err
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, raw); err != nil {
		neoDiffCaptureBlobIndexes.delete(threadID)
		return false, err
	}
	neoDiffCaptureBlobIndexes.put(threadID, index)
	if err := neoBoundDiffCaptureRepository(ctx, threadID, forceGC); err != nil {
		return false, err
	}
	if err := removeNeoDiffCapturePendingPrune(threadID); err != nil {
		return false, err
	}
	return true, nil
}

func neoDiffCaptureFileURL(dir string) string {
	path := strings.ReplaceAll(strings.TrimSpace(dir), `\`, "/")
	if len(path) >= 2 && path[1] == ':' {
		path = "/" + path
	}
	if strings.HasPrefix(path, "//") {
		parts := strings.SplitN(strings.TrimPrefix(path, "//"), "/", 2)
		fileURL := &url.URL{Scheme: "file", Host: parts[0]}
		if len(parts) == 2 {
			fileURL.Path = "/" + parts[1]
		}
		return fileURL.String()
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}

func neoDiffCapturePublishedRef(captureID string) string {
	return "refs/amp/diff-captures/" + captureID
}

func neoPromoteDiffCaptureRef(ctx context.Context, repositoryDir, captureRef, publishedRef, expectedSHA string) (bool, error) {
	if err := neoVerifyDiffCapturePublishedRef(ctx, repositoryDir, publishedRef, expectedSHA); err == nil {
		_, err = neoDiffCaptureGit(ctx, repositoryDir, []string{"update-ref", "-d", captureRef}, 4096, nil)
		return true, err
	}
	var updates strings.Builder
	updates.WriteString("start\ncreate ")
	updates.WriteString(publishedRef)
	updates.WriteByte(' ')
	updates.WriteString(expectedSHA)
	updates.WriteString("\ndelete ")
	updates.WriteString(captureRef)
	updates.WriteByte(' ')
	updates.WriteString(expectedSHA)
	updates.WriteString("\nprepare\ncommit\n")
	_, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"update-ref", "--stdin"}, 4096, strings.NewReader(updates.String()))
	return err == nil, err
}

func neoVerifyDiffCapturePublishedRef(ctx context.Context, repositoryDir, publishedRef, expectedSHA string) error {
	resolved, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"rev-parse", "--verify", publishedRef + "^{commit}"}, 256, nil)
	if err != nil {
		return err
	}
	if !strings.EqualFold(strings.TrimSpace(resolved), expectedSHA) {
		return errors.New("published diff capture ref does not match projected worktree")
	}
	return nil
}

func neoDiffCaptureRefSHA(ctx context.Context, repositoryDir, ref string) (string, bool, error) {
	stdout := neoGitOutputBuffer{limit: 256}
	if err := neoChangesFileOrderGitWrite(ctx, repositoryDir, []string{"rev-parse", "--verify", "--quiet", ref}, true, &stdout, nil, neoGitCommandEnv()); err != nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	sha := strings.TrimSpace(stdout.String())
	if sha == "" {
		return "", false, nil
	}
	if !neoDiffCaptureSHApattern.MatchString(sha) {
		return "", false, errors.New("invalid diff capture ref SHA")
	}
	return sha, true, nil
}

func neoRollbackDiffCaptureRefPromotion(ctx context.Context, repositoryDir, captureRef, publishedRef string) error {
	publishedSHA, found, err := neoDiffCaptureRefSHA(ctx, repositoryDir, publishedRef)
	if err != nil || !found {
		return err
	}
	captureSHA, captureFound, err := neoDiffCaptureRefSHA(ctx, repositoryDir, captureRef)
	if err != nil {
		return err
	}
	if captureFound && !strings.EqualFold(captureSHA, publishedSHA) {
		return errors.New("diff capture upload ref changed during publication recovery")
	}
	var updates strings.Builder
	updates.WriteString("start\n")
	if !captureFound {
		updates.WriteString("create ")
		updates.WriteString(captureRef)
		updates.WriteByte(' ')
		updates.WriteString(publishedSHA)
		updates.WriteByte('\n')
	}
	updates.WriteString("delete ")
	updates.WriteString(publishedRef)
	updates.WriteByte(' ')
	updates.WriteString(publishedSHA)
	updates.WriteString("\nprepare\ncommit\n")
	_, err = neoDiffCaptureGit(ctx, repositoryDir, []string{"update-ref", "--stdin"}, 4096, strings.NewReader(updates.String()))
	return err
}

func neoBoundDiffCaptureRepository(ctx context.Context, threadID string, forceGC bool) error {
	repositoryDir := neoDiffCaptureRepositoryDir(threadID)
	bytesUsed, err := neoDiffCaptureRepositoryBytes(ctx, repositoryDir)
	if err != nil {
		return err
	}
	if forceGC || bytesUsed > neoDiffCaptureRepositoryMaxBytes {
		if _, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"gc", "--prune=now"}, 4096, nil); err != nil {
			return err
		}
		bytesUsed, err = neoDiffCaptureRepositoryBytes(ctx, repositoryDir)
		if err != nil {
			return err
		}
	}
	if bytesUsed > neoDiffCaptureRepositoryMaxBytes {
		return fmt.Errorf("%w: Git repository", errNeoDiffCaptureStorageLimit)
	}
	return nil
}

func neoDiffCaptureRepositoryBytes(ctx context.Context, repositoryDir string) (int64, error) {
	output, err := neoDiffCaptureGit(ctx, repositoryDir, []string{"count-objects", "-v"}, 4096, nil)
	if err != nil {
		return 0, err
	}
	values := make(map[string]int64, 3)
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		key := strings.TrimSuffix(fields[0], ":")
		if key != "size" && key != "size-pack" && key != "size-garbage" {
			continue
		}
		value, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil || value < 0 {
			return 0, errors.New("invalid diff capture Git object size")
		}
		values[key] = value
	}
	if _, ok := values["size"]; !ok {
		return 0, errors.New("missing diff capture loose object size")
	}
	if _, ok := values["size-pack"]; !ok {
		return 0, errors.New("missing diff capture packed object size")
	}
	maxKiB := int64(^uint64(0)>>1) / 1024
	totalKiB := values["size"] + values["size-pack"] + values["size-garbage"]
	if totalKiB < 0 || totalKiB > maxKiB {
		return 0, errors.New("invalid diff capture Git object size")
	}
	return totalKiB * 1024, nil
}

func publishedNeoDiffCaptures(threadID string) ([]neoDiffCapturePublished, error) {
	dir := neoDiffCaptureCapturesDir(threadID)
	if dir == "" {
		return nil, errors.New("invalid diff capture directory")
	}
	directory, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := directory.Close(); err != nil {
			logNeoDiffCaptureError("capture directory close", threadID, "", err)
		}
	}()
	records := make([]neoDiffCapturePublished, 0)
	recordCount := 0
	var recordBytes int64
	for {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			captureID := strings.TrimSuffix(entry.Name(), ".json")
			if !neoDiffCaptureIDPattern.MatchString(captureID) {
				continue
			}
			recordCount++
			if recordCount > neoDiffCaptureScanMaxRecords {
				return nil, errors.New("diff capture compatibility scan exceeds record limit")
			}
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			recordBytes += info.Size()
			if recordBytes > neoDiffCaptureScanMaxBytes {
				return nil, errors.New("diff capture compatibility scan exceeds byte limit")
			}
			record, err := readNeoDiffCaptureRecord(threadID, captureID)
			if err != nil {
				return nil, err
			}
			if record.State != "published" {
				continue
			}
			publishedAt, manifest, err := validateNeoDiffCapturePublishedRecord(record)
			if err != nil {
				return nil, err
			}
			records = append(records, neoDiffCapturePublished{record: record, manifest: manifest, publishedAt: publishedAt})
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, readErr
		}
	}
	return records, nil
}

func latestNeoDiffCapture(threadID string) (neoDiffCaptureRecord, neoDiffCaptureManifest, bool, error) {
	pending, err := readNeoDiffCapturePendingPublication(threadID)
	if err == nil {
		return recoverNeoDiffCapturePendingPublication(threadID, pending)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	record, manifest, ok, err := latestNeoDiffCapturePointer(threadID)
	if errors.Is(err, os.ErrNotExist) {
		return rebuildNeoDiffCaptureLatest(threadID)
	}
	return record, manifest, ok, err
}

func latestNeoDiffCapturePointer(threadID string) (neoDiffCaptureRecord, neoDiffCaptureManifest, bool, error) {
	pointer, err := readNeoDiffCaptureLatestRecord(threadID)
	if err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	if pointer.CaptureID == "" {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, nil
	}
	record, err := readNeoDiffCaptureRecord(threadID, pointer.CaptureID)
	if err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, fmt.Errorf("read latest diff capture record: %w", err)
	}
	_, manifest, err := validateNeoDiffCapturePublishedRecord(record)
	if err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	if record.PublishedAt != pointer.PublishedAt || record.ManifestSHA256 != pointer.ManifestSHA256 {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, errors.New("latest diff capture pointer does not match capture record")
	}
	return record, manifest, true, nil
}

func recoverNeoDiffCapturePendingPublication(threadID string, pending neoDiffCapturePendingPublication) (neoDiffCaptureRecord, neoDiffCaptureManifest, bool, error) {
	record, err := readNeoDiffCaptureRecord(threadID, pending.CaptureID)
	if err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, fmt.Errorf("read pending diff capture record: %w", err)
	}
	switch record.State {
	case "published":
		_, manifest, err := validateNeoDiffCapturePublishedRecord(record)
		if err != nil {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
		}
		if err := neoVerifyDiffCapturePublishedRef(context.Background(), neoDiffCaptureRepositoryDir(threadID), record.PublishedRef, manifest.Revisions.ProjectedWorktreeSHA); err != nil {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, fmt.Errorf("recover published diff capture ref: %w", err)
		}
		if err := ensureNeoDiffCapturePublishedIndex(threadID, record, record.Manifest); err != nil {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
		}
	case "allocated":
		if record.PublishedAt != "" || record.ManifestSHA256 != "" || len(record.Manifest) != 0 {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, errors.New("invalid allocated diff capture metadata")
		}
		if _, err := time.Parse(time.RFC3339Nano, record.CreatedAt); err != nil {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, errors.New("invalid allocated diff capture timestamp")
		}
		if err := neoRollbackDiffCaptureRefPromotion(context.Background(), neoDiffCaptureRepositoryDir(threadID), record.CaptureRef, neoDiffCapturePublishedRef(record.CaptureID)); err != nil {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, fmt.Errorf("recover allocated diff capture ref promotion: %w", err)
		}
	default:
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, errors.New("invalid pending diff capture state")
	}
	latest, manifest, found, err := rebuildNeoDiffCaptureLatest(threadID)
	if err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	if err := removeNeoDiffCapturePendingPublication(threadID); err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	return latest, manifest, found, nil
}

func rebuildNeoDiffCaptureLatest(threadID string) (neoDiffCaptureRecord, neoDiffCaptureManifest, bool, error) {
	var latest neoDiffCaptureRecord
	var latestManifest neoDiffCaptureManifest
	var latestTime time.Time
	found := false
	records, err := publishedNeoDiffCaptures(threadID)
	if err != nil {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	for _, published := range records {
		if !found || published.publishedAt.After(latestTime) || published.publishedAt.Equal(latestTime) && published.record.CaptureID > latest.CaptureID {
			latest = published.record
			latestManifest = published.manifest
			latestTime = published.publishedAt
			found = true
		}
	}
	pointer := neoDiffCaptureLatestRecord{StorageVersion: 1}
	if found {
		pointer.CaptureID = latest.CaptureID
		pointer.PublishedAt = latest.PublishedAt
		pointer.ManifestSHA256 = latest.ManifestSHA256
	}
	threadDir := neoDiffCaptureThreadDir(threadID)
	if _, err := os.Stat(threadDir); err == nil {
		if err := writeNeoDiffCaptureLatestRecord(threadID, pointer); err != nil {
			return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return neoDiffCaptureRecord{}, neoDiffCaptureManifest{}, false, err
	}
	return latest, latestManifest, found, nil
}

func updateNeoDiffCaptureLatest(threadID string, candidate neoDiffCaptureRecord) error {
	candidateTime, _, err := validateNeoDiffCapturePublishedRecord(candidate)
	if err != nil {
		return err
	}
	current, _, found, err := latestNeoDiffCapturePointer(threadID)
	if errors.Is(err, os.ErrNotExist) {
		current, _, found, err = rebuildNeoDiffCaptureLatest(threadID)
	}
	if err != nil {
		return err
	}
	if found {
		currentTime, _, err := validateNeoDiffCapturePublishedRecord(current)
		if err != nil {
			return err
		}
		if currentTime.After(candidateTime) || currentTime.Equal(candidateTime) && current.CaptureID >= candidate.CaptureID {
			return nil
		}
	}
	return writeNeoDiffCaptureLatestRecord(threadID, neoDiffCaptureLatestRecord{
		StorageVersion: 1,
		CaptureID:      candidate.CaptureID,
		PublishedAt:    candidate.PublishedAt,
		ManifestSHA256: candidate.ManifestSHA256,
	})
}

func validateNeoDiffCapturePublishedRecord(record neoDiffCaptureRecord) (time.Time, neoDiffCaptureManifest, error) {
	if record.State != "published" || record.PublishedRef != neoDiffCapturePublishedRef(record.CaptureID) || record.PublishedAt == "" || len(record.Manifest) == 0 {
		return time.Time{}, neoDiffCaptureManifest{}, errors.New("invalid published diff capture metadata")
	}
	publishedAt, err := time.Parse(time.RFC3339Nano, record.PublishedAt)
	if err != nil {
		return time.Time{}, neoDiffCaptureManifest{}, errors.New("invalid published diff capture timestamp")
	}
	manifestSHA256 := neoDiffCaptureManifestSHA256(record.Manifest)
	if record.ManifestSHA256 != manifestSHA256 {
		return time.Time{}, neoDiffCaptureManifest{}, errors.New("published diff capture manifest digest mismatch")
	}
	var manifest neoDiffCaptureManifest
	if err := json.Unmarshal(record.Manifest, &manifest); err != nil {
		return time.Time{}, neoDiffCaptureManifest{}, errors.New("invalid published diff capture manifest")
	}
	if err := validateNeoDiffCaptureManifest(record, manifest); err != nil {
		return time.Time{}, neoDiffCaptureManifest{}, err
	}
	return publishedAt, manifest, nil
}

func readNeoDiffCaptureLatestRecord(threadID string) (neoDiffCaptureLatestRecord, error) {
	path := neoDiffCaptureLatestPath(threadID)
	if path == "" {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture path")
	}
	file, err := os.Open(path)
	if err != nil {
		return neoDiffCaptureLatestRecord{}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			logNeoDiffCaptureError("latest close", threadID, "", err)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return neoDiffCaptureLatestRecord{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > neoDiffCaptureLatestMaxBytes {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	raw, err := io.ReadAll(io.LimitReader(file, neoDiffCaptureLatestMaxBytes+1))
	if err != nil {
		return neoDiffCaptureLatestRecord{}, err
	}
	if len(raw) > neoDiffCaptureLatestMaxBytes {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	var pointer neoDiffCaptureLatestRecord
	if err := json.Unmarshal(raw, &pointer); err != nil || pointer.StorageVersion != 1 {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	if pointer.CaptureID == "" {
		if pointer.PublishedAt != "" || pointer.ManifestSHA256 != "" {
			return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
		}
		return pointer, nil
	}
	if !neoDiffCaptureIDPattern.MatchString(pointer.CaptureID) {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	if _, err := time.Parse(time.RFC3339Nano, pointer.PublishedAt); err != nil {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	if len(pointer.ManifestSHA256) != sha256.Size*2 {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	if _, err := hex.DecodeString(pointer.ManifestSHA256); err != nil {
		return neoDiffCaptureLatestRecord{}, errors.New("invalid latest diff capture pointer")
	}
	return pointer, nil
}

func writeNeoDiffCaptureLatestRecord(threadID string, pointer neoDiffCaptureLatestRecord) error {
	path := neoDiffCaptureLatestPath(threadID)
	if path == "" {
		return errors.New("invalid latest diff capture path")
	}
	raw, err := json.Marshal(pointer)
	if err != nil {
		return err
	}
	if len(raw)+1 > neoDiffCaptureLatestMaxBytes {
		return errors.New("latest diff capture pointer exceeds local limit")
	}
	if err := writeNeoDurableAtomicFile(path, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func readNeoDiffCapturePendingPublication(threadID string) (neoDiffCapturePendingPublication, error) {
	path := neoDiffCapturePendingPublicationPath(threadID)
	if path == "" {
		return neoDiffCapturePendingPublication{}, errors.New("invalid pending diff capture path")
	}
	file, err := os.Open(path)
	if err != nil {
		return neoDiffCapturePendingPublication{}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			logNeoDiffCaptureError("pending publication close", threadID, "", err)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return neoDiffCapturePendingPublication{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > neoDiffCapturePendingMaxBytes {
		return neoDiffCapturePendingPublication{}, errors.New("invalid pending diff capture publication")
	}
	raw, err := io.ReadAll(io.LimitReader(file, neoDiffCapturePendingMaxBytes+1))
	if err != nil {
		return neoDiffCapturePendingPublication{}, err
	}
	if len(raw) > neoDiffCapturePendingMaxBytes {
		return neoDiffCapturePendingPublication{}, errors.New("pending diff capture publication exceeds local limit")
	}
	var pending neoDiffCapturePendingPublication
	if err := json.Unmarshal(raw, &pending); err != nil || pending.StorageVersion != 1 || !neoDiffCaptureIDPattern.MatchString(pending.CaptureID) {
		return neoDiffCapturePendingPublication{}, errors.New("invalid pending diff capture publication")
	}
	return pending, nil
}

func writeNeoDiffCapturePendingPublication(threadID, captureID string) error {
	path := neoDiffCapturePendingPublicationPath(threadID)
	if path == "" || !neoDiffCaptureIDPattern.MatchString(captureID) {
		return errors.New("invalid pending diff capture path")
	}
	pending, err := readNeoDiffCapturePendingPublication(threadID)
	if err == nil {
		if pending.CaptureID != captureID {
			return errors.New("another diff capture publication is pending")
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	raw, err := json.Marshal(neoDiffCapturePendingPublication{StorageVersion: 1, CaptureID: captureID})
	if err != nil {
		return err
	}
	if len(raw)+1 > neoDiffCapturePendingMaxBytes {
		return errors.New("pending diff capture publication exceeds local limit")
	}
	if err := writeNeoDurableAtomicFile(path, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func removeNeoDiffCapturePendingPublication(threadID string) error {
	path := neoDiffCapturePendingPublicationPath(threadID)
	if path == "" {
		return errors.New("invalid pending diff capture path")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func readNeoDiffCapturePendingPrune(threadID string) (neoDiffCapturePendingPrune, error) {
	path := neoDiffCapturePendingPrunePath(threadID)
	if path == "" {
		return neoDiffCapturePendingPrune{}, errors.New("invalid pending diff capture prune path")
	}
	file, err := os.Open(path)
	if err != nil {
		return neoDiffCapturePendingPrune{}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			logNeoDiffCaptureError("pending prune close", threadID, "", err)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return neoDiffCapturePendingPrune{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > neoDiffCapturePendingPruneMaxBytes {
		return neoDiffCapturePendingPrune{}, errors.New("invalid pending diff capture prune")
	}
	raw, err := io.ReadAll(io.LimitReader(file, neoDiffCapturePendingPruneMaxBytes+1))
	if err != nil {
		return neoDiffCapturePendingPrune{}, err
	}
	if len(raw) > neoDiffCapturePendingPruneMaxBytes {
		return neoDiffCapturePendingPrune{}, errors.New("pending diff capture prune exceeds local limit")
	}
	var pending neoDiffCapturePendingPrune
	if err := json.Unmarshal(raw, &pending); err != nil || pending.StorageVersion != 1 || len(pending.CaptureIDs) == 0 || len(pending.CaptureIDs) > neoDiffCaptureScanMaxRecords {
		return neoDiffCapturePendingPrune{}, errors.New("invalid pending diff capture prune")
	}
	seen := make(map[string]struct{}, len(pending.CaptureIDs))
	for _, captureID := range pending.CaptureIDs {
		if !neoDiffCaptureIDPattern.MatchString(captureID) {
			return neoDiffCapturePendingPrune{}, errors.New("invalid pending diff capture prune")
		}
		if _, duplicate := seen[captureID]; duplicate {
			return neoDiffCapturePendingPrune{}, errors.New("invalid pending diff capture prune")
		}
		seen[captureID] = struct{}{}
	}
	return pending, nil
}

func writeNeoDiffCapturePendingPrune(threadID string, captureIDs []string) error {
	path := neoDiffCapturePendingPrunePath(threadID)
	if path == "" || len(captureIDs) == 0 || len(captureIDs) > neoDiffCaptureScanMaxRecords {
		return errors.New("invalid pending diff capture prune path")
	}
	seen := make(map[string]struct{}, len(captureIDs))
	for _, captureID := range captureIDs {
		if !neoDiffCaptureIDPattern.MatchString(captureID) {
			return errors.New("invalid pending diff capture prune")
		}
		if _, duplicate := seen[captureID]; duplicate {
			return errors.New("invalid pending diff capture prune")
		}
		seen[captureID] = struct{}{}
	}
	if pending, err := readNeoDiffCapturePendingPrune(threadID); err == nil {
		if len(pending.CaptureIDs) != len(captureIDs) {
			return errors.New("another diff capture prune is pending")
		}
		for index := range captureIDs {
			if pending.CaptureIDs[index] != captureIDs[index] {
				return errors.New("another diff capture prune is pending")
			}
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	pending := neoDiffCapturePendingPrune{StorageVersion: 1, CaptureIDs: append([]string(nil), captureIDs...)}
	raw, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	if len(raw)+1 > neoDiffCapturePendingPruneMaxBytes {
		return errors.New("pending diff capture prune exceeds local limit")
	}
	if err := writeNeoDurableAtomicFile(path, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func removeNeoDiffCapturePendingPrune(threadID string) error {
	path := neoDiffCapturePendingPrunePath(threadID)
	if path == "" {
		return errors.New("invalid pending diff capture prune path")
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func neoDiffCaptureManifestBlobSHAs(manifest neoDiffCaptureManifest) ([]string, error) {
	shas := make([]string, 0)
	seen := make(map[string]struct{})
	addFiles := func(files []neoDiffCaptureManifestFile) error {
		for _, file := range files {
			for _, sha := range []string{file.OldBlobSHA, file.NewBlobSHA, file.Old.BlobSHA, file.New.BlobSHA} {
				sha = strings.TrimSpace(sha)
				if sha == "" {
					continue
				}
				if !neoDiffCaptureSHApattern.MatchString(sha) {
					return errors.New("invalid diff capture blob sha")
				}
				sha = strings.ToLower(sha)
				if _, ok := seen[sha]; !ok {
					seen[sha] = struct{}{}
					shas = append(shas, sha)
				}
			}
		}
		return nil
	}
	for _, section := range manifest.Sections {
		if err := addFiles(section.Files); err != nil {
			return nil, err
		}
	}
	for _, section := range manifest.RangeSections {
		if err := addFiles(section.Files); err != nil {
			return nil, err
		}
	}
	return shas, nil
}

func neoDiffCaptureManifestSHA256(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func neoDiffCaptureBlobIndexForThread(threadID string) (*neoDiffCaptureBlobIndex, error) {
	index, err := readNeoDiffCaptureBlobIndex(threadID)
	if errors.Is(err, os.ErrNotExist) {
		index, err = rebuildNeoDiffCaptureBlobIndex(threadID)
		if err == nil {
			if _, statErr := os.Stat(neoDiffCaptureThreadDir(threadID)); statErr == nil {
				var raw []byte
				raw, err = marshalNeoDiffCaptureBlobIndex(index)
				if err == nil {
					err = writeNeoDiffCaptureBlobIndex(threadID, raw)
				}
			} else if !errors.Is(statErr, os.ErrNotExist) {
				err = statErr
			}
		}
	}
	if err != nil {
		return nil, err
	}
	neoDiffCaptureBlobIndexes.put(threadID, index)
	return index, nil
}

func neoDiffCaptureBlobIndexForPublish(threadID string) (*neoDiffCaptureBlobIndex, error) {
	index, err := readNeoDiffCaptureBlobIndex(threadID)
	if err == nil {
		neoDiffCaptureBlobIndexes.put(threadID, index)
		return index, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return rebuildNeoDiffCaptureBlobIndex(threadID)
}

func rebuildNeoDiffCaptureBlobIndex(threadID string) (*neoDiffCaptureBlobIndex, error) {
	records, err := publishedNeoDiffCaptures(threadID)
	if err != nil {
		return nil, err
	}
	captures := make(map[string]neoDiffCaptureBlobIndexEntry, len(records))
	for _, published := range records {
		shas, err := neoDiffCaptureManifestBlobSHAs(published.manifest)
		if err != nil {
			return nil, err
		}
		manifestSHA256 := published.record.ManifestSHA256
		if manifestSHA256 == "" {
			manifestSHA256 = neoDiffCaptureManifestSHA256(published.record.Manifest)
		}
		shaSet := make(map[string]struct{}, len(shas))
		for _, sha := range shas {
			shaSet[strings.ToLower(sha)] = struct{}{}
		}
		captures[published.record.CaptureID] = neoDiffCaptureBlobIndexEntry{manifestSHA256: manifestSHA256, shas: shaSet}
	}
	return newNeoDiffCaptureBlobIndex(captures), nil
}

func readNeoDiffCaptureBlobIndex(threadID string) (*neoDiffCaptureBlobIndex, error) {
	path := neoDiffCaptureBlobIndexPath(threadID)
	if path == "" {
		return nil, errors.New("invalid diff capture blob index path")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			logNeoDiffCaptureError("blob index close", threadID, "", err)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > neoDiffCaptureBlobIndexMaxBytes {
		return nil, errors.New("invalid diff capture blob index")
	}
	raw, err := io.ReadAll(io.LimitReader(file, neoDiffCaptureBlobIndexMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > neoDiffCaptureBlobIndexMaxBytes {
		return nil, errors.New("diff capture blob index exceeds local limit")
	}
	var stored neoDiffCaptureStoredBlobIndex
	if err := json.Unmarshal(raw, &stored); err != nil || stored.StorageVersion != 1 || stored.Captures == nil {
		return nil, errors.New("invalid diff capture blob index")
	}
	captures := make(map[string]neoDiffCaptureBlobIndexEntry, len(stored.Captures))
	for captureID, entry := range stored.Captures {
		if !neoDiffCaptureIDPattern.MatchString(captureID) || len(entry.ManifestSHA256) != sha256.Size*2 {
			return nil, errors.New("invalid diff capture blob index")
		}
		if _, err := hex.DecodeString(entry.ManifestSHA256); err != nil {
			return nil, errors.New("invalid diff capture blob index")
		}
		seen := make(map[string]struct{}, len(entry.SHAs))
		for _, sha := range entry.SHAs {
			if !neoDiffCaptureSHApattern.MatchString(sha) {
				return nil, errors.New("invalid diff capture blob index")
			}
			sha = strings.ToLower(sha)
			if _, duplicate := seen[sha]; duplicate {
				return nil, errors.New("invalid diff capture blob index")
			}
			seen[sha] = struct{}{}
		}
		captures[captureID] = neoDiffCaptureBlobIndexEntry{manifestSHA256: strings.ToLower(entry.ManifestSHA256), shas: seen}
	}
	return newNeoDiffCaptureBlobIndex(captures), nil
}

func marshalNeoDiffCaptureBlobIndex(index *neoDiffCaptureBlobIndex) ([]byte, error) {
	if index == nil {
		return nil, errors.New("invalid diff capture blob index")
	}
	stored := neoDiffCaptureStoredBlobIndex{StorageVersion: 1, Captures: make(map[string]neoDiffCaptureStoredBlobEntry, len(index.captures))}
	for captureID, entry := range index.captures {
		if !neoDiffCaptureIDPattern.MatchString(captureID) || len(entry.manifestSHA256) != sha256.Size*2 {
			return nil, errors.New("invalid diff capture blob index")
		}
		if _, err := hex.DecodeString(entry.manifestSHA256); err != nil {
			return nil, errors.New("invalid diff capture blob index")
		}
		shas := make([]string, 0, len(entry.shas))
		for sha := range entry.shas {
			if !neoDiffCaptureSHApattern.MatchString(sha) {
				return nil, errors.New("invalid diff capture blob index")
			}
			shas = append(shas, strings.ToLower(sha))
		}
		sort.Strings(shas)
		stored.Captures[captureID] = neoDiffCaptureStoredBlobEntry{ManifestSHA256: strings.ToLower(entry.manifestSHA256), SHAs: shas}
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		return nil, err
	}
	if len(raw)+1 > neoDiffCaptureBlobIndexMaxBytes {
		return nil, errors.New("diff capture blob index exceeds local limit")
	}
	return append(raw, '\n'), nil
}

func ensureNeoDiffCapturePublishedIndex(threadID string, record neoDiffCaptureRecord, raw []byte) error {
	var manifest neoDiffCaptureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return err
	}
	if err := validateNeoDiffCaptureManifest(record, manifest); err != nil {
		return err
	}
	shas, err := neoDiffCaptureManifestBlobSHAs(manifest)
	if err != nil {
		return err
	}
	index, err := neoDiffCaptureBlobIndexForPublish(threadID)
	if err != nil {
		return err
	}
	index = index.withCapture(record.CaptureID, neoDiffCaptureManifestSHA256(raw), shas)
	indexRaw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil {
		return err
	}
	if err := writeNeoDiffCaptureBlobIndex(threadID, indexRaw); err != nil {
		neoDiffCaptureBlobIndexes.delete(threadID)
		return err
	}
	neoDiffCaptureBlobIndexes.put(threadID, index)
	return nil
}

func neoDiffCaptureIndexAuthorizes(threadID string, index *neoDiffCaptureBlobIndex, shas ...string) (bool, error) {
	if index == nil || len(shas) == 0 {
		return false, nil
	}
	var candidateErr error
	for _, captureID := range index.bySHA[strings.ToLower(shas[0])] {
		entry := index.captures[captureID]
		candidate := true
		for _, sha := range shas {
			if _, ok := entry.shas[strings.ToLower(sha)]; !ok {
				candidate = false
				break
			}
		}
		if !candidate {
			continue
		}
		record, err := readNeoDiffCaptureRecord(threadID, captureID)
		if err != nil {
			candidateErr = fmt.Errorf("read indexed diff capture %s: %w", captureID, err)
			continue
		}
		if record.State != "published" || len(record.Manifest) == 0 {
			candidateErr = fmt.Errorf("indexed diff capture %s is not published", captureID)
			continue
		}
		manifestSHA256 := neoDiffCaptureManifestSHA256(record.Manifest)
		if entry.manifestSHA256 != manifestSHA256 || record.ManifestSHA256 != "" && record.ManifestSHA256 != manifestSHA256 {
			candidateErr = fmt.Errorf("indexed diff capture %s manifest digest mismatch", captureID)
			continue
		}
		var manifest neoDiffCaptureManifest
		if err := json.Unmarshal(record.Manifest, &manifest); err != nil {
			candidateErr = fmt.Errorf("decode indexed diff capture %s: %w", captureID, err)
			continue
		}
		if err := validateNeoDiffCaptureManifest(record, manifest); err != nil {
			candidateErr = fmt.Errorf("validate indexed diff capture %s: %w", captureID, err)
			continue
		}
		manifestSHAs, err := neoDiffCaptureManifestBlobSHAs(manifest)
		if err != nil {
			candidateErr = fmt.Errorf("read indexed diff capture %s blobs: %w", captureID, err)
			continue
		}
		allowed := make(map[string]struct{}, len(manifestSHAs))
		for _, sha := range manifestSHAs {
			allowed[sha] = struct{}{}
		}
		for _, sha := range shas {
			if _, ok := allowed[strings.ToLower(sha)]; !ok {
				candidate = false
				break
			}
		}
		if candidate {
			return true, nil
		}
		candidateErr = fmt.Errorf("indexed diff capture %s does not authorize indexed blobs", captureID)
	}
	return false, candidateErr
}

func (i *neoDiffCaptureBlobIndex) withCapture(captureID, manifestSHA256 string, shas []string) *neoDiffCaptureBlobIndex {
	captures := make(map[string]neoDiffCaptureBlobIndexEntry, len(i.captures)+1)
	for existingID, entry := range i.captures {
		captures[existingID] = entry
	}
	shaSet := make(map[string]struct{}, len(shas))
	for _, sha := range shas {
		shaSet[strings.ToLower(sha)] = struct{}{}
	}
	captures[captureID] = neoDiffCaptureBlobIndexEntry{manifestSHA256: manifestSHA256, shas: shaSet}
	return newNeoDiffCaptureBlobIndex(captures)
}

func newNeoDiffCaptureBlobIndex(captures map[string]neoDiffCaptureBlobIndexEntry) *neoDiffCaptureBlobIndex {
	bySHA := make(map[string][]string)
	for captureID, entry := range captures {
		for sha := range entry.shas {
			bySHA[sha] = append(bySHA[sha], captureID)
		}
	}
	for sha := range bySHA {
		sort.Strings(bySHA[sha])
	}
	return &neoDiffCaptureBlobIndex{captures: captures, bySHA: bySHA}
}

func (i *neoDiffCaptureBlobIndex) allows(sha string) bool {
	return i != nil && len(i.bySHA[strings.ToLower(sha)]) != 0
}

func (i *neoDiffCaptureBlobIndex) allowsPair(fromSHA, toSHA string) bool {
	if i == nil {
		return false
	}
	fromSHA = strings.ToLower(fromSHA)
	toSHA = strings.ToLower(toSHA)
	for _, captureID := range i.bySHA[fromSHA] {
		_, hasFrom := i.captures[captureID].shas[fromSHA]
		entry := i.captures[captureID]
		_, hasTo := entry.shas[toSHA]
		if hasFrom && hasTo {
			return true
		}
	}
	return false
}

func (c *neoDiffCaptureBlobIndexCache) get(threadID string) (*neoDiffCaptureBlobIndex, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[threadID]
	if !ok {
		return nil, false
	}
	c.clock++
	entry.used = c.clock
	c.entries[threadID] = entry
	return entry.index, true
}

func (c *neoDiffCaptureBlobIndexCache) put(threadID string, index *neoDiffCaptureBlobIndex) {
	c.marshalMu.Lock()
	defer c.marshalMu.Unlock()
	raw, err := marshalNeoDiffCaptureBlobIndex(index)
	if err != nil || len(raw) > neoDiffCaptureBlobIndexCacheBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]neoDiffCaptureBlobIndexCacheEntry)
	}
	if existing, exists := c.entries[threadID]; exists {
		c.totalBytes -= existing.bytes
		delete(c.entries, threadID)
	}
	for len(c.entries) >= neoDiffCaptureBlobIndexMaxThreads || len(c.entries) > 0 && c.totalBytes+len(raw) > neoDiffCaptureBlobIndexCacheBytes {
		oldestThreadID := ""
		oldestUse := ^uint64(0)
		for candidateThreadID, entry := range c.entries {
			if entry.used < oldestUse {
				oldestThreadID = candidateThreadID
				oldestUse = entry.used
			}
		}
		c.totalBytes -= c.entries[oldestThreadID].bytes
		delete(c.entries, oldestThreadID)
	}
	c.clock++
	c.entries[threadID] = neoDiffCaptureBlobIndexCacheEntry{index: index, used: c.clock, bytes: len(raw)}
	c.totalBytes += len(raw)
}

func (c *neoDiffCaptureBlobIndexCache) delete(threadID string) {
	c.mu.Lock()
	if entry, ok := c.entries[threadID]; ok {
		c.totalBytes -= entry.bytes
	}
	delete(c.entries, threadID)
	c.mu.Unlock()
}

func writeNeoDiffCaptureBlobIndex(threadID string, raw []byte) error {
	path := neoDiffCaptureBlobIndexPath(threadID)
	if err := writeNeoDurableAtomicFile(path, raw, 0o600); err != nil {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func removeNeoDiffCaptureBlobIndex(threadID string) error {
	path := neoDiffCaptureBlobIndexPath(threadID)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func removeNeoDiffCaptureLatest(threadID string) error {
	path := neoDiffCaptureLatestPath(threadID)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncNeoDurablePath(path, false)
}

func neoDiffCaptureReadBody(c *gin.Context, maxBytes int64) ([]byte, error) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	raw, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return []byte("{}"), nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if _, ok := value.(map[string]any); !ok {
		return nil, errors.New("request body must be a JSON object")
	}
	return raw, nil
}

func neoDiffCaptureWriteBodyError(c *gin.Context, err error) {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "diff capture request body exceeds local limit"})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": "invalid diff capture request body"})
}

func removeNeoLocalDiffCaptures(threadID string) error {
	if !neoThreadIDExactPattern.MatchString(threadID) {
		return nil
	}
	unlock, err := neoDiffCaptureLock(context.Background(), threadID)
	if err != nil {
		return err
	}
	defer unlock()
	dir := neoDiffCaptureThreadDir(threadID)
	if dir == "" {
		return nil
	}
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	receiveRelease, errReceive := neoAcquireDiffCaptureReceiveLock(threadID)
	if errReceive != nil {
		return errReceive
	}
	defer receiveRelease()
	deletingDir := dir + ".deleting-" + randomBase62(16)
	if err := os.Rename(dir, deletingDir); err != nil {
		return err
	}
	receiveRelease()
	neoDiffCaptureBlobIndexes.delete(threadID)
	if err := os.RemoveAll(deletingDir); err != nil {
		logNeoDiffCaptureError("capture directory cleanup", threadID, "", err)
		return err
	}
	return nil
}

func logNeoDiffCaptureError(operation, threadID, captureID string, err error) {
	fields := log.Fields{"operation": operation, "thread": threadID}
	if captureID != "" {
		fields["capture"] = captureID
	}
	log.WithFields(fields).WithError(err).Warn("amp neo local diff capture failed")
}
