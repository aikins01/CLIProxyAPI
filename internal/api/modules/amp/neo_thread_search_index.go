package amp

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	neoLocalThreadSearchIndexDirName       = ".cliproxyapi-thread-search-v1"
	neoLocalThreadSearchIndexVersion       = 2
	neoLocalThreadSearchIndexBloomBytes    = 32 * 1024
	neoLocalThreadSearchIndexMagicBytes    = 8
	neoLocalThreadSearchIndexThreadIDBytes = sha256.Size
	neoLocalThreadSearchIndexIdentityBytes = 64
	neoLocalThreadSearchIndexHeaderBytes   = neoLocalThreadSearchIndexMagicBytes + 2 + neoLocalThreadSearchIndexThreadIDBytes + neoLocalThreadSearchIndexIdentityBytes
	neoLocalThreadSearchIndexEncodedBytes  = neoLocalThreadSearchIndexHeaderBytes + neoLocalThreadSearchIndexBloomBytes + sha256.Size
)

var (
	neoLocalThreadSearchIndexMagic                   = [neoLocalThreadSearchIndexMagicBytes]byte{'A', 'M', 'P', 'I', 'D', 'X', '0', '1'}
	neoLocalThreadSearchIndexGuards                  [64]sync.RWMutex
	neoLocalThreadSearchIndexReadLockObserverForTest atomic.Pointer[neoLocalThreadSearchIndexReadLockObserver]
)

type neoLocalThreadSearchIndexReadLockObserver struct {
	acquired func(string)
}

type neoLocalThreadSearchResult struct {
	threadID string
	thread   map[string]any
	updated  int
	score    int
}

type neoLocalThreadSearchResultHeap []neoLocalThreadSearchResult

func (h neoLocalThreadSearchResultHeap) Len() int {
	return len(h)
}

func (h neoLocalThreadSearchResultHeap) Less(i, j int) bool {
	return neoLocalThreadSearchResultBetter(h[j], h[i])
}

func (h neoLocalThreadSearchResultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *neoLocalThreadSearchResultHeap) Push(value any) {
	*h = append(*h, value.(neoLocalThreadSearchResult))
}

func (h *neoLocalThreadSearchResultHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = neoLocalThreadSearchResult{}
	*h = old[:last]
	return value
}

func neoLocalThreadSearchResultBetter(left, right neoLocalThreadSearchResult) bool {
	if left.score != right.score {
		return left.score > right.score
	}
	if left.updated != right.updated {
		return left.updated > right.updated
	}
	return left.threadID < right.threadID
}

type neoLocalThreadSearchResultCollector struct {
	results neoLocalThreadSearchResultHeap
	limit   int
	total   int
}

type neoLocalThreadSearchMetrics struct {
	filesConsidered           atomic.Uint64
	validIndexes              atomic.Uint64
	unknownIndexes            atomic.Uint64
	corruptIndexes            atomic.Uint64
	staleIndexes              atomic.Uint64
	predictedNegativesChecked atomic.Uint64
	mismatches                atomic.Uint64
	rebuildSuccesses          atomic.Uint64
	rebuildFailures           atomic.Uint64
	sidecarBytesRead          atomic.Uint64
	sidecarBytesWritten       atomic.Uint64
	scannerBytesRead          atomic.Uint64
	potentiallyPrunedBytes    atomic.Uint64
	rankedSearches            atomic.Uint64
	rankedCandidates          atomic.Uint64
	rankedDecodes             atomic.Uint64
	rankFallbacks             atomic.Uint64
	orphanCleanups            atomic.Uint64
	orphanCleanupFailures     atomic.Uint64
}

type neoLocalThreadSearchMetricsSnapshot struct {
	filesConsidered           uint64
	validIndexes              uint64
	unknownIndexes            uint64
	corruptIndexes            uint64
	staleIndexes              uint64
	predictedNegativesChecked uint64
	mismatches                uint64
	rebuildSuccesses          uint64
	rebuildFailures           uint64
	sidecarBytesRead          uint64
	sidecarBytesWritten       uint64
	scannerBytesRead          uint64
	potentiallyPrunedBytes    uint64
	rankedSearches            uint64
	rankedCandidates          uint64
	rankedDecodes             uint64
	rankFallbacks             uint64
	orphanCleanups            uint64
	orphanCleanupFailures     uint64
}

func (m *neoLocalThreadSearchMetrics) snapshot() neoLocalThreadSearchMetricsSnapshot {
	return neoLocalThreadSearchMetricsSnapshot{
		filesConsidered:           m.filesConsidered.Load(),
		validIndexes:              m.validIndexes.Load(),
		unknownIndexes:            m.unknownIndexes.Load(),
		corruptIndexes:            m.corruptIndexes.Load(),
		staleIndexes:              m.staleIndexes.Load(),
		predictedNegativesChecked: m.predictedNegativesChecked.Load(),
		mismatches:                m.mismatches.Load(),
		rebuildSuccesses:          m.rebuildSuccesses.Load(),
		rebuildFailures:           m.rebuildFailures.Load(),
		sidecarBytesRead:          m.sidecarBytesRead.Load(),
		sidecarBytesWritten:       m.sidecarBytesWritten.Load(),
		scannerBytesRead:          m.scannerBytesRead.Load(),
		potentiallyPrunedBytes:    m.potentiallyPrunedBytes.Load(),
		rankedSearches:            m.rankedSearches.Load(),
		rankedCandidates:          m.rankedCandidates.Load(),
		rankedDecodes:             m.rankedDecodes.Load(),
		rankFallbacks:             m.rankFallbacks.Load(),
		orphanCleanups:            m.orphanCleanups.Load(),
		orphanCleanupFailures:     m.orphanCleanupFailures.Load(),
	}
}

func (m *neoLocalThreadSearchMetrics) log() {
	metrics := m.snapshot()
	if metrics.filesConsidered == 0 {
		return
	}
	log.WithFields(log.Fields{
		"files_considered":            metrics.filesConsidered,
		"valid_indexes":               metrics.validIndexes,
		"unknown_indexes":             metrics.unknownIndexes,
		"corrupt_indexes":             metrics.corruptIndexes,
		"stale_indexes":               metrics.staleIndexes,
		"predicted_negatives_checked": metrics.predictedNegativesChecked,
		"mismatches":                  metrics.mismatches,
		"rebuild_successes":           metrics.rebuildSuccesses,
		"rebuild_failures":            metrics.rebuildFailures,
		"sidecar_bytes_read":          metrics.sidecarBytesRead,
		"sidecar_bytes_written":       metrics.sidecarBytesWritten,
		"scanner_bytes_read":          metrics.scannerBytesRead,
		"potentially_pruned_bytes":    metrics.potentiallyPrunedBytes,
		"ranked_searches":             metrics.rankedSearches,
		"ranked_candidates":           metrics.rankedCandidates,
		"ranked_decodes":              metrics.rankedDecodes,
		"rank_fallbacks":              metrics.rankFallbacks,
		"orphan_cleanups":             metrics.orphanCleanups,
		"orphan_cleanup_failures":     metrics.orphanCleanupFailures,
	}).Debug("amp neo local thread search index shadow metrics")
}

func newNeoLocalThreadSearchResultCollector(limit, capacity int) *neoLocalThreadSearchResultCollector {
	return &neoLocalThreadSearchResultCollector{
		results: make(neoLocalThreadSearchResultHeap, 0, min(limit, capacity)),
		limit:   limit,
	}
}

func (c *neoLocalThreadSearchResultCollector) add(result neoLocalThreadSearchResult) {
	c.total++
	if len(c.results) < c.limit {
		heap.Push(&c.results, result)
		return
	}
	if neoLocalThreadSearchResultBetter(result, c.results[0]) {
		c.results[0] = result
		heap.Fix(&c.results, 0)
	}
}

func (c *neoLocalThreadSearchResultCollector) page(offset, limit int) ([]neoLocalThreadSearchResult, bool) {
	if c.total == 0 || offset >= c.total {
		return []neoLocalThreadSearchResult{}, false
	}
	sort.SliceStable(c.results, func(i, j int) bool {
		return neoLocalThreadSearchResultBetter(c.results[i], c.results[j])
	})
	end := min(offset+limit, c.total)
	return c.results[offset:end], end < c.total
}

type neoLocalThreadSearchFile struct {
	path     string
	threadID string
}

const (
	neoLocalThreadSearchRankVersion                = 3
	neoLocalThreadSearchRankBackfillWorkers        = 1
	neoLocalThreadSearchRankBackfillBytesPerSecond = 16 * 1024 * 1024
	neoLocalThreadSearchRankBackfillStartDelay     = 30 * time.Second
	neoLocalThreadSearchRankMaxBytes               = 4096
	neoLocalThreadSearchRankTitleMaxRunes          = 256
)

type neoLocalThreadSearchRank struct {
	Version              int    `json:"v"`
	ThreadID             string `json:"id"`
	SnapshotSize         int64  `json:"size"`
	SnapshotModifiedNano int64  `json:"modNano"`
	SnapshotChangeSec    int64  `json:"changeSec"`
	SnapshotChangeNano   int64  `json:"changeNano"`
	SnapshotChecksum     string `json:"snapshotChecksum,omitempty"`
	Title                string `json:"title"`
	Updated              int    `json:"updated"`
	Checksum             string `json:"checksum"`
}

func neoLocalThreadSearchMillis(value any) int {
	if millis := numberFrom(value); millis > 0 {
		return millis
	}
	return neoTimeStringMillis(stringValue(value))
}

func neoThreadSearchTitleFromMessages(messages []any) string {
	for _, rawMessage := range messages {
		message := mapValue(rawMessage)
		if stringValue(message["role"]) != "user" {
			continue
		}
		for _, rawBlock := range arrayValue(message["content"]) {
			block := mapValue(rawBlock)
			if stringValue(block["type"]) != "text" {
				continue
			}
			text := strings.TrimSpace(stringValue(block["text"]))
			if text == "" {
				continue
			}
			runes := []rune(text)
			if len(runes) > 80 {
				return string(runes[:80])
			}
			return text
		}
	}
	return ""
}

func neoLocalThreadSearchRankTitle(title string) (string, bool) {
	runes := 0
	for range title {
		if runes == neoLocalThreadSearchRankTitleMaxRunes {
			return "", false
		}
		runes++
	}
	return strings.Clone(title), true
}

func writeNeoLocalThreadSearchRank(threadDir, snapshotPath, fileThreadID string, snapshotInfo os.FileInfo, rank neoLocalThreadSearchRank, snapshotRaw []byte) error {
	if snapshotInfo == nil || !neoThreadIDExactPattern.MatchString(fileThreadID) || rank.ThreadID != fileThreadID || strings.TrimSpace(threadDir) == "" {
		return errors.New("amp local thread search rank source unavailable")
	}
	guard := neoLocalThreadSearchIndexGuard(fileThreadID)
	guard.Lock()
	defer guard.Unlock()
	return writeNeoLocalThreadSearchRankLocked(threadDir, snapshotPath, fileThreadID, snapshotInfo, rank, snapshotRaw)
}

func writeNeoLocalThreadSearchRankLocked(threadDir, snapshotPath, fileThreadID string, snapshotInfo os.FileInfo, rank neoLocalThreadSearchRank, snapshotRaw []byte) error {
	if snapshotInfo == nil || !neoThreadIDExactPattern.MatchString(fileThreadID) || rank.ThreadID != fileThreadID || strings.TrimSpace(threadDir) == "" {
		return errors.New("amp local thread search rank source unavailable")
	}
	dir := filepath.Join(threadDir, neoLocalThreadSearchIndexDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	currentInfo, err := os.Stat(snapshotPath)
	if err != nil || !neoLocalThreadSearchSnapshotStableIdentityMatches(snapshotInfo, currentInfo) {
		return errors.New("amp local thread search rank source changed")
	}
	if snapshotRaw == nil {
		var readErr error
		snapshotRaw, readErr = neoLocalThreadSearchReadBounded(snapshotPath, neoLocalSnapshotCacheMaxBytes)
		if readErr != nil {
			return readErr
		}
	} else if len(snapshotRaw) > neoLocalSnapshotCacheMaxBytes {
		return errors.New("amp local thread search snapshot exceeds size limit")
	}
	actualRank, actualRankOK := neoLocalThreadSearchRankFromJSON(gjson.ParseBytes(snapshotRaw), fileThreadID)
	publishedInfo, statErr := os.Stat(snapshotPath)
	if !actualRankOK || actualRank.ThreadID != fileThreadID || statErr != nil || !neoLocalThreadSearchSnapshotStableIdentityMatches(currentInfo, publishedInfo) {
		return errors.New("amp local thread search rank source changed during publication")
	}
	rank = actualRank
	rank.Version = neoLocalThreadSearchRankVersion
	rank.SnapshotSize = currentInfo.Size()
	rank.SnapshotModifiedNano = currentInfo.ModTime().UnixNano()
	rank.SnapshotChangeSec, rank.SnapshotChangeNano, _ = neoLocalThreadSearchSnapshotChangeTime(currentInfo)
	rank.SnapshotChecksum = ""
	if _, _, ok := neoLocalThreadSearchSnapshotChangeTime(currentInfo); !ok {
		snapshotChecksum := sha256.Sum256(snapshotRaw)
		rank.SnapshotChecksum = hex.EncodeToString(snapshotChecksum[:])
	}
	if rank.Title, actualRankOK = neoLocalThreadSearchRankTitle(rank.Title); !actualRankOK {
		return errors.New("amp local thread search rank title exceeds size limit")
	}
	rank.Checksum = neoLocalThreadSearchRankChecksum(rank)
	raw, err := json.Marshal(rank)
	if err != nil {
		return err
	}
	if len(raw) > neoLocalThreadSearchRankMaxBytes {
		return errors.New("amp local thread search rank exceeds size limit")
	}
	return writeNeoLocalThreadSearchSidecar(neoLocalThreadSearchRankPath(threadDir, fileThreadID), raw)
}

func readNeoLocalThreadSearchRank(threadDir, fileThreadID string, snapshotInfo os.FileInfo) (neoLocalThreadSearchRank, bool) {
	if snapshotInfo == nil || !neoThreadIDExactPattern.MatchString(fileThreadID) {
		return neoLocalThreadSearchRank{}, false
	}
	guard := neoLocalThreadSearchIndexGuard(fileThreadID)
	guard.RLock()
	defer guard.RUnlock()
	snapshotPath := filepath.Join(threadDir, fileThreadID+".json")
	currentInfo, err := os.Stat(snapshotPath)
	if err != nil || !neoLocalThreadSearchSnapshotStableIdentityMatches(snapshotInfo, currentInfo) {
		return neoLocalThreadSearchRank{}, false
	}
	rankFile, err := os.Open(neoLocalThreadSearchRankPath(threadDir, fileThreadID))
	if err != nil {
		return neoLocalThreadSearchRank{}, false
	}
	raw, readErr := io.ReadAll(io.LimitReader(rankFile, neoLocalThreadSearchRankMaxBytes+1))
	closeErr := rankFile.Close()
	if readErr != nil || closeErr != nil || len(raw) == 0 || len(raw) > neoLocalThreadSearchRankMaxBytes {
		return neoLocalThreadSearchRank{}, false
	}
	var rank neoLocalThreadSearchRank
	if err := json.Unmarshal(raw, &rank); err != nil {
		return neoLocalThreadSearchRank{}, false
	}
	rankTitle, rankTitleOK := neoLocalThreadSearchRankTitle(rank.Title)
	if !rankTitleOK || rank.Version != neoLocalThreadSearchRankVersion || rank.ThreadID != fileThreadID || rank.SnapshotSize != currentInfo.Size() || rank.SnapshotModifiedNano != currentInfo.ModTime().UnixNano() || rank.Title != rankTitle || rank.Checksum != neoLocalThreadSearchRankChecksum(rank) {
		return neoLocalThreadSearchRank{}, false
	}
	changeSec, changeNano, changeTimeOK := neoLocalThreadSearchSnapshotChangeTime(currentInfo)
	if changeTimeOK {
		if rank.SnapshotChecksum != "" || rank.SnapshotChangeSec != changeSec || rank.SnapshotChangeNano != changeNano {
			return neoLocalThreadSearchRank{}, false
		}
	} else {
		snapshotChecksum, checksumErr := neoLocalThreadSearchSnapshotChecksum(snapshotPath, neoLocalSnapshotCacheMaxBytes)
		if checksumErr != nil || rank.SnapshotChecksum == "" || rank.SnapshotChecksum != snapshotChecksum {
			return neoLocalThreadSearchRank{}, false
		}
	}
	publishedInfo, statErr := os.Stat(snapshotPath)
	if statErr != nil || !neoLocalThreadSearchSnapshotStableIdentityMatches(currentInfo, publishedInfo) {
		return neoLocalThreadSearchRank{}, false
	}
	return rank, true
}

func neoLocalThreadSearchRankChecksum(rank neoLocalThreadSearchRank) string {
	rank.Checksum = ""
	raw, _ := json.Marshal(rank)
	checksum := sha256.Sum256(raw)
	return hex.EncodeToString(checksum[:])
}

func neoLocalThreadSearchSnapshotStableIdentityMatches(left, right os.FileInfo) bool {
	if left == nil || right == nil || !os.SameFile(left, right) || left.Size() != right.Size() || !left.ModTime().Equal(right.ModTime()) {
		return false
	}
	leftSec, leftNano, leftOK := neoLocalThreadSearchSnapshotChangeTime(left)
	rightSec, rightNano, rightOK := neoLocalThreadSearchSnapshotChangeTime(right)
	return !leftOK && !rightOK || leftOK && rightOK && leftSec == rightSec && leftNano == rightNano
}

func readNeoLocalThreadSearchRankBackfillFile(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := handle.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > neoLocalSnapshotCacheMaxBytes {
		return nil, errors.Join(err, handle.Close(), errors.New("amp local thread search rank source exceeds size limit"))
	}
	raw := make([]byte, neoLocalSnapshotCacheMaxBytes)
	written := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, handle.Close())
		}
		if written == len(raw) {
			var extra [1]byte
			read, readErr := handle.Read(extra[:])
			if read > 0 {
				return nil, errors.Join(errors.New("amp local thread search rank source exceeds size limit"), handle.Close())
			}
			closeErr := handle.Close()
			if errors.Is(readErr, io.EOF) {
				return raw[:written], closeErr
			}
			return nil, errors.Join(readErr, closeErr)
		}
		started := time.Now()
		end := min(written+neoLocalThreadSearchBufferBytes, len(raw))
		read, readErr := handle.Read(raw[written:end])
		if read > 0 {
			if err := ctx.Err(); err != nil {
				return nil, errors.Join(err, handle.Close())
			}
			written += read
			delay := time.Duration(read)*time.Second/neoLocalThreadSearchRankBackfillBytesPerSecond - time.Since(started)
			if delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					return nil, errors.Join(ctx.Err(), handle.Close())
				}
			}
		}
		if readErr != nil {
			closeErr := handle.Close()
			if errors.Is(readErr, io.EOF) {
				return raw[:written], closeErr
			}
			return nil, errors.Join(readErr, closeErr)
		}
	}
}

func (rt *neoRuntime) backfillNeoLocalThreadSearchRanks(ctx context.Context) {
	if rt == nil || strings.TrimSpace(rt.threadDir) == "" {
		return
	}
	entries, err := os.ReadDir(rt.threadDir)
	if err != nil {
		return
	}
	started := time.Now()
	jobs := make(chan neoLocalThreadSearchFile)
	var workers sync.WaitGroup
	var generated atomic.Uint64
	var failures atomic.Uint64
	var bytesRead atomic.Uint64
	for range neoLocalThreadSearchRankBackfillWorkers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for file := range jobs {
				if ctx.Err() != nil {
					return
				}
				info, statErr := os.Stat(file.path)
				if statErr != nil {
					continue
				}
				if _, ok := readNeoLocalThreadSearchRank(rt.threadDir, file.threadID, info); ok {
					continue
				}
				if !neoAcquireLocalThreadSearchPermit(ctx, rt.localThreadSearchDecodes) {
					return
				}
				raw, readErr := readNeoLocalThreadSearchRankBackfillFile(ctx, file.path)
				if readErr != nil {
					neoReleaseLocalThreadSearchPermit(rt.localThreadSearchDecodes)
					if ctx.Err() != nil {
						return
					}
					failures.Add(1)
					continue
				}
				bytesRead.Add(uint64(len(raw)))
				currentInfo, statErr := os.Stat(file.path)
				if statErr != nil || !neoLocalThreadSearchSnapshotStableIdentityMatches(info, currentInfo) {
					neoReleaseLocalThreadSearchPermit(rt.localThreadSearchDecodes)
					continue
				}
				rank, ok := neoLocalThreadSearchRankFromJSON(gjson.ParseBytes(raw), file.threadID)
				neoReleaseLocalThreadSearchPermit(rt.localThreadSearchDecodes)
				if !ok {
					continue
				}
				if writeErr := writeNeoLocalThreadSearchRank(rt.threadDir, file.path, file.threadID, currentInfo, rank, raw); writeErr != nil {
					failures.Add(1)
					continue
				}
				generated.Add(1)
			}
		}()
	}
feed:
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		threadID := strings.TrimSuffix(entry.Name(), ".json")
		if !neoThreadIDExactPattern.MatchString(threadID) {
			continue
		}
		select {
		case jobs <- neoLocalThreadSearchFile{path: filepath.Join(rt.threadDir, entry.Name()), threadID: threadID}:
		case <-ctx.Done():
			break feed
		}
	}
	close(jobs)
	workers.Wait()
	if generated.Load() > 0 || failures.Load() > 0 {
		log.WithFields(log.Fields{
			"generated":  generated.Load(),
			"failures":   failures.Load(),
			"bytes_read": bytesRead.Load(),
			"elapsed":    time.Since(started).Round(time.Millisecond),
		}).Info("amp neo local thread search rank backfill complete")
	}
}

type neoLocalThreadSearchCandidate struct {
	file neoLocalThreadSearchFile
	info os.FileInfo
	rank neoLocalThreadSearchRank
}

func neoLocalThreadSearchRankPath(threadDir, threadID string) string {
	return filepath.Join(threadDir, neoLocalThreadSearchIndexDirName, threadID+".rank")
}

func neoLocalThreadSearchRankFromJSON(thread gjson.Result, fallbackID string) (neoLocalThreadSearchRank, bool) {
	var id, title, updatedAt, updated, interactedAt, createdAt, created, messages, data gjson.Result
	thread.ForEach(func(key, value gjson.Result) bool {
		switch key.String() {
		case "id":
			id = value
		case "title":
			title = value
		case "updatedAt":
			updatedAt = value
		case "updated":
			updated = value
		case "userLastInteractedAt":
			interactedAt = value
		case "createdAt":
			createdAt = value
		case "created":
			created = value
		case "messages":
			messages = value
		case "data":
			data = value
		}
		return true
	})
	var dataID, dataTitle, dataUpdatedAt, dataUpdated, dataInteractedAt, dataCreatedAt, dataCreated, dataMessages gjson.Result
	if data.Exists() && data.IsObject() {
		data.ForEach(func(key, value gjson.Result) bool {
			switch key.String() {
			case "id":
				dataID = value
			case "title":
				dataTitle = value
			case "updatedAt":
				dataUpdatedAt = value
			case "updated":
				dataUpdated = value
			case "userLastInteractedAt":
				dataInteractedAt = value
			case "createdAt":
				dataCreatedAt = value
			case "created":
				dataCreated = value
			case "messages":
				dataMessages = value
			}
			return true
		})
	}
	threadID, ok := neoLocalThreadSearchRankThreadID(fallbackID, id.Value(), dataID.Value())
	if !ok {
		return neoLocalThreadSearchRank{}, false
	}
	if !messages.Exists() || !messages.IsArray() {
		messages = dataMessages
	}
	titleValue := firstNonEmptyString(neoLocalThreadSearchJSONString(title), neoLocalThreadSearchJSONString(dataTitle))
	if titleValue == "" {
		titleValue = firstNonEmptyString(neoThreadSearchTitleFromJSONMessages(messages), "Untitled")
	}
	titleValue, ok = neoLocalThreadSearchRankTitle(titleValue)
	if !ok {
		return neoLocalThreadSearchRank{}, false
	}
	updatedValue := 0
	for _, value := range []gjson.Result{updatedAt, updated, interactedAt, createdAt, created, dataUpdatedAt, dataUpdated, dataInteractedAt, dataCreatedAt, dataCreated} {
		if updatedValue = neoJSONMillis(value); updatedValue > 0 {
			break
		}
	}
	if updatedValue == 0 && messages.Exists() && messages.IsArray() {
		messages.ForEach(func(_, message gjson.Result) bool {
			updatedValue = max(updatedValue, firstNonZero(neoJSONMillis(message.Get("created")), neoJSONMillis(message.Get("createdAt"))))
			return true
		})
	}
	return neoLocalThreadSearchRank{
		Version:  neoLocalThreadSearchRankVersion,
		ThreadID: threadID,
		Title:    titleValue,
		Updated:  updatedValue,
	}, true
}

func neoLocalThreadSearchRankFromThread(thread map[string]any, fallbackID string) (neoLocalThreadSearchRank, bool) {
	data := mapValue(thread["data"])
	threadID, ok := neoLocalThreadSearchRankThreadID(fallbackID, thread["id"], data["id"])
	if !ok {
		return neoLocalThreadSearchRank{}, false
	}
	messages, messagesOK := arrayValueOK(thread["messages"])
	if !messagesOK {
		messages = arrayValue(data["messages"])
	}
	title := neoLocalThreadSearchMapString(thread["title"], data["title"])
	if title == "" {
		title = firstNonEmptyString(neoThreadSearchTitleFromMessages(messages), "Untitled")
	}
	title, ok = neoLocalThreadSearchRankTitle(title)
	if !ok {
		return neoLocalThreadSearchRank{}, false
	}
	updated := 0
	for _, value := range []any{
		thread["updatedAt"], thread["updated"], thread["userLastInteractedAt"], thread["createdAt"], thread["created"],
		data["updatedAt"], data["updated"], data["userLastInteractedAt"], data["createdAt"], data["created"],
	} {
		if updated = neoLocalThreadSearchMillis(value); updated > 0 {
			break
		}
	}
	if updated == 0 {
		for _, rawMessage := range messages {
			message := mapValue(rawMessage)
			created := firstNonZero(neoLocalThreadSearchMillis(message["created"]), neoLocalThreadSearchMillis(message["createdAt"]))
			updated = max(updated, created)
		}
	}
	return neoLocalThreadSearchRank{
		Version:  neoLocalThreadSearchRankVersion,
		ThreadID: threadID,
		Title:    title,
		Updated:  updated,
	}, true
}

func neoLocalThreadSearchRankThreadID(fallbackID string, embeddedIDs ...any) (string, bool) {
	if !neoThreadIDExactPattern.MatchString(fallbackID) {
		return "", false
	}
	for _, rawID := range embeddedIDs {
		if rawID == nil {
			continue
		}
		embeddedID, ok := rawID.(string)
		if !ok || embeddedID != fallbackID {
			return "", false
		}
	}
	return strings.Clone(fallbackID), true
}

type neoLocalThreadSearchIndexBuilder struct {
	bloom          []byte
	normalizedTail [2]byte
	tailLen        int
	normalizer     *neoLocalThreadSearchWorkspace
	sourceHash     hash.Hash
	complete       bool
}

func neoLocalThreadSearchJSONString(value gjson.Result) string {
	if value.Type != gjson.String {
		return ""
	}
	return value.String()
}

func neoLocalThreadSearchMapString(values ...any) string {
	for _, value := range values {
		if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
			return text
		}
	}
	return ""
}

type neoLocalThreadSearchIndexWorkspace struct {
	encoded []byte
}

type neoLocalThreadSearchIndexReadStatus uint8

const (
	neoLocalThreadSearchIndexUnknown neoLocalThreadSearchIndexReadStatus = iota
	neoLocalThreadSearchIndexValid
	neoLocalThreadSearchIndexCorrupt
	neoLocalThreadSearchIndexStale
)

func newNeoLocalThreadSearchIndexBuilder() *neoLocalThreadSearchIndexBuilder {
	builder := &neoLocalThreadSearchIndexBuilder{bloom: make([]byte, neoLocalThreadSearchIndexBloomBytes), sourceHash: sha256.New()}
	builder.normalizer = (&neoLocalThreadSearchMatcher{}).newWorkspace(64 * 1024)
	builder.normalizer.indexBuilder = builder
	return builder
}

func newNeoLocalThreadSearchIndexWorkspace() *neoLocalThreadSearchIndexWorkspace {
	return &neoLocalThreadSearchIndexWorkspace{encoded: make([]byte, neoLocalThreadSearchIndexEncodedBytes)}
}

func (b *neoLocalThreadSearchIndexBuilder) reset() {
	clear(b.bloom)
	b.normalizedTail = [2]byte{}
	b.tailLen = 0
	b.complete = false
	if b.sourceHash == nil {
		b.sourceHash = sha256.New()
	} else {
		b.sourceHash.Reset()
	}
}

func (b *neoLocalThreadSearchIndexBuilder) Write(raw []byte) (int, error) {
	if b == nil || b.normalizer == nil {
		return 0, errors.New("amp local thread search index builder unavailable")
	}
	b.normalizer.write(raw, false)
	return len(raw), nil
}

func (b *neoLocalThreadSearchIndexBuilder) finish() {
	if b != nil && b.normalizer != nil {
		b.normalizer.write(nil, true)
	}
}

func (b *neoLocalThreadSearchIndexBuilder) build(raw []byte, chunkBytes int) {
	if b == nil || b.normalizer == nil {
		return
	}
	b.reset()
	b.normalizer.indexBuilder = b
	b.normalizer.reset()
	chunkBytes = max(1, chunkBytes)
	for len(raw) > 0 {
		size := min(chunkBytes, len(raw))
		b.normalizer.write(raw[:size], false)
		raw = raw[size:]
	}
	b.normalizer.write(nil, true)
}

func (b *neoLocalThreadSearchIndexBuilder) addNormalized(raw []byte) {
	for _, value := range raw {
		if b.tailLen < len(b.normalizedTail) {
			b.normalizedTail[b.tailLen] = value
			b.tailLen++
			continue
		}
		b.addTrigram(b.normalizedTail[0], b.normalizedTail[1], value)
		b.normalizedTail[0] = b.normalizedTail[1]
		b.normalizedTail[1] = value
	}
}

func (b *neoLocalThreadSearchIndexBuilder) addRaw(raw []byte) {
	if b != nil && b.sourceHash != nil && len(raw) > 0 {
		_, _ = b.sourceHash.Write(raw)
	}
}

func (b *neoLocalThreadSearchIndexBuilder) markComplete() {
	if b != nil {
		b.complete = true
	}
}

func (b *neoLocalThreadSearchIndexBuilder) addTrigram(first, second, third byte) {
	for _, bit := range neoLocalThreadSearchIndexTrigramBits(first, second, third) {
		b.bloom[bit>>3] |= byte(1 << (bit & 7))
	}
}

func (b *neoLocalThreadSearchIndexBuilder) mayContain(patterns [][]byte) bool {
	for _, pattern := range patterns {
		for index := 0; index+2 < len(pattern); index++ {
			for _, bit := range neoLocalThreadSearchIndexTrigramBits(pattern[index], pattern[index+1], pattern[index+2]) {
				if b.bloom[bit>>3]&byte(1<<(bit&7)) == 0 {
					return false
				}
			}
		}
	}
	return true
}

func neoLocalThreadSearchIndexTrigramBits(first, second, third byte) [4]uint32 {
	value := uint32(first)<<16 | uint32(second)<<8 | uint32(third)
	firstHash := value*0x9e3779b1 + 0x85ebca6b
	firstHash ^= firstHash >> 16
	firstHash *= 0x7feb352d
	firstHash ^= firstHash >> 15
	secondHash := bits.RotateLeft32(value^0xc2b2ae35, 13)*0x27d4eb2d | 1
	bitCount := uint32(neoLocalThreadSearchIndexBloomBytes * 8)
	return [4]uint32{
		firstHash % bitCount,
		(firstHash + secondHash) % bitCount,
		(firstHash + 2*secondHash) % bitCount,
		(firstHash + 3*secondHash) % bitCount,
	}
}

func neoLocalThreadSearchIndexGuard(threadID string) *sync.RWMutex {
	var hash uint32
	for index := range len(threadID) {
		hash = hash*33 + uint32(threadID[index])
	}
	return &neoLocalThreadSearchIndexGuards[hash%uint32(len(neoLocalThreadSearchIndexGuards))]
}

func neoLocalThreadSearchIndexPaths(threadDir, threadID string) (string, string) {
	dir := filepath.Join(threadDir, neoLocalThreadSearchIndexDirName)
	return filepath.Join(dir, threadID+".bloom"), filepath.Join(dir, threadID+".source")
}

func writeNeoLocalThreadSearchIndex(threadDir, snapshotPath, threadID string, snapshotInfo os.FileInfo, builder *neoLocalThreadSearchIndexBuilder) error {
	if builder == nil || !builder.complete || snapshotInfo == nil || !neoThreadIDExactPattern.MatchString(threadID) || threadDir == "" {
		return errors.New("amp local thread search index source unavailable")
	}
	indexPath, sourcePath := neoLocalThreadSearchIndexPaths(threadDir, threadID)
	dir := filepath.Dir(indexPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	guard := neoLocalThreadSearchIndexGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	currentInfo, err := os.Stat(snapshotPath)
	if err != nil || !os.SameFile(snapshotInfo, currentInfo) || !neoLocalThreadSearchSnapshotIdentityMatches(snapshotInfo, currentInfo) {
		return errors.New("amp local thread search index source changed")
	}
	if err := os.Remove(sourcePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	changeSec, changeNsec, _ := neoLocalThreadSearchSnapshotChangeTime(currentInfo)
	sourceChecksum := builder.sourceHash.Sum(nil)
	if len(sourceChecksum) != sha256.Size {
		return errors.New("amp local thread search index source checksum unavailable")
	}
	encoded := make([]byte, neoLocalThreadSearchIndexEncodedBytes)
	copy(encoded[:neoLocalThreadSearchIndexMagicBytes], neoLocalThreadSearchIndexMagic[:])
	binary.BigEndian.PutUint16(encoded[neoLocalThreadSearchIndexMagicBytes:], neoLocalThreadSearchIndexVersion)
	threadIDHash := sha256.Sum256([]byte(threadID))
	threadIDEnd := neoLocalThreadSearchIndexMagicBytes + 2 + neoLocalThreadSearchIndexThreadIDBytes
	copy(encoded[neoLocalThreadSearchIndexMagicBytes+2:threadIDEnd], threadIDHash[:])
	binary.BigEndian.PutUint64(encoded[threadIDEnd:], uint64(currentInfo.Size()))
	binary.BigEndian.PutUint64(encoded[threadIDEnd+8:], uint64(currentInfo.ModTime().UnixNano()))
	binary.BigEndian.PutUint64(encoded[threadIDEnd+16:], uint64(changeSec))
	binary.BigEndian.PutUint64(encoded[threadIDEnd+24:], uint64(changeNsec))
	copy(encoded[threadIDEnd+32:threadIDEnd+64], sourceChecksum)
	copy(encoded[neoLocalThreadSearchIndexHeaderBytes:neoLocalThreadSearchIndexHeaderBytes+neoLocalThreadSearchIndexBloomBytes], builder.bloom)
	checksum := sha256.Sum256(encoded[:neoLocalThreadSearchIndexEncodedBytes-sha256.Size])
	copy(encoded[neoLocalThreadSearchIndexEncodedBytes-sha256.Size:], checksum[:])
	if err := writeNeoLocalThreadSearchSidecar(indexPath, encoded); err != nil {
		return err
	}
	publishedInfo, err := os.Stat(snapshotPath)
	if err != nil || !os.SameFile(currentInfo, publishedInfo) || !neoLocalThreadSearchSnapshotIdentityMatches(currentInfo, publishedInfo) {
		return errors.New("amp local thread search index source changed during publication")
	}
	return nil
}

func readNeoLocalThreadSearchIndex(threadDir, threadID string, snapshotInfo os.FileInfo, patterns [][]byte, workspace *neoLocalThreadSearchIndexWorkspace) (bool, bool) {
	mayContain, status, _ := readNeoLocalThreadSearchIndexDetailed(threadDir, threadID, snapshotInfo, patterns, workspace)
	return mayContain, status == neoLocalThreadSearchIndexValid
}

func readNeoLocalThreadSearchIndexDetailed(threadDir, threadID string, snapshotInfo os.FileInfo, patterns [][]byte, workspace *neoLocalThreadSearchIndexWorkspace) (bool, neoLocalThreadSearchIndexReadStatus, int64) {
	if workspace == nil || len(workspace.encoded) != neoLocalThreadSearchIndexEncodedBytes || snapshotInfo == nil || !neoThreadIDExactPattern.MatchString(threadID) {
		return true, neoLocalThreadSearchIndexUnknown, 0
	}
	indexPath, _ := neoLocalThreadSearchIndexPaths(threadDir, threadID)
	guard := neoLocalThreadSearchIndexGuard(threadID)
	guard.RLock()
	defer guard.RUnlock()
	if observer := neoLocalThreadSearchIndexReadLockObserverForTest.Load(); observer != nil && observer.acquired != nil {
		observer.acquired(threadID)
	}
	snapshotPath := filepath.Join(threadDir, threadID+".json")
	currentInfo, err := os.Stat(snapshotPath)
	if err != nil || !os.SameFile(snapshotInfo, currentInfo) || !neoLocalThreadSearchSnapshotIdentityMatches(snapshotInfo, currentInfo) {
		return true, neoLocalThreadSearchIndexStale, 0
	}
	handle, err := os.Open(indexPath)
	if err != nil {
		return true, neoLocalThreadSearchIndexUnknown, 0
	}
	read, readErr := io.ReadFull(handle, workspace.encoded)
	var extra [1]byte
	extraRead, extraErr := handle.Read(extra[:])
	closeErr := handle.Close()
	readBytes := int64(read + extraRead)
	if readErr != nil || read != len(workspace.encoded) || extraRead != 0 || extraErr != io.EOF || closeErr != nil {
		return true, neoLocalThreadSearchIndexCorrupt, readBytes
	}
	encoded := workspace.encoded
	wantChecksum := encoded[neoLocalThreadSearchIndexEncodedBytes-sha256.Size:]
	gotChecksum := sha256.Sum256(encoded[:neoLocalThreadSearchIndexEncodedBytes-sha256.Size])
	if !bytes.Equal(wantChecksum, gotChecksum[:]) {
		return true, neoLocalThreadSearchIndexCorrupt, readBytes
	}
	if !bytes.Equal(encoded[:neoLocalThreadSearchIndexMagicBytes], neoLocalThreadSearchIndexMagic[:]) {
		return true, neoLocalThreadSearchIndexCorrupt, readBytes
	}
	if int(binary.BigEndian.Uint16(encoded[neoLocalThreadSearchIndexMagicBytes:])) != neoLocalThreadSearchIndexVersion {
		return true, neoLocalThreadSearchIndexUnknown, readBytes
	}
	threadIDHash := sha256.Sum256([]byte(threadID))
	threadIDEnd := neoLocalThreadSearchIndexMagicBytes + 2 + neoLocalThreadSearchIndexThreadIDBytes
	if !bytes.Equal(encoded[neoLocalThreadSearchIndexMagicBytes+2:threadIDEnd], threadIDHash[:]) {
		return true, neoLocalThreadSearchIndexCorrupt, readBytes
	}
	if int64(binary.BigEndian.Uint64(encoded[threadIDEnd:])) != snapshotInfo.Size() || int64(binary.BigEndian.Uint64(encoded[threadIDEnd+8:])) != snapshotInfo.ModTime().UnixNano() {
		return true, neoLocalThreadSearchIndexStale, readBytes
	}
	changeSec, changeNsec, changeTimeOK := neoLocalThreadSearchSnapshotChangeTime(snapshotInfo)
	if changeTimeOK {
		if int64(binary.BigEndian.Uint64(encoded[threadIDEnd+16:])) != changeSec || int64(binary.BigEndian.Uint64(encoded[threadIDEnd+24:])) != changeNsec {
			return true, neoLocalThreadSearchIndexStale, readBytes
		}
	} else {
		snapshotChecksum, checksumErr := neoLocalThreadSearchSnapshotChecksum(snapshotPath, neoLocalSnapshotCacheMaxBytes)
		if checksumErr != nil || hex.EncodeToString(encoded[threadIDEnd+32:threadIDEnd+64]) != snapshotChecksum {
			return true, neoLocalThreadSearchIndexStale, readBytes
		}
	}
	currentInfo, err = os.Stat(snapshotPath)
	if err != nil || !os.SameFile(snapshotInfo, currentInfo) || !neoLocalThreadSearchSnapshotIdentityMatches(snapshotInfo, currentInfo) {
		return true, neoLocalThreadSearchIndexStale, readBytes
	}
	builder := neoLocalThreadSearchIndexBuilder{bloom: encoded[neoLocalThreadSearchIndexHeaderBytes : neoLocalThreadSearchIndexHeaderBytes+neoLocalThreadSearchIndexBloomBytes]}
	return builder.mayContain(patterns), neoLocalThreadSearchIndexValid, readBytes
}

func neoLocalThreadSearchSnapshotIdentityMatches(left, right os.FileInfo) bool {
	if left == nil || right == nil || !os.SameFile(left, right) || left.Size() != right.Size() || !left.ModTime().Equal(right.ModTime()) {
		return false
	}
	leftSec, leftNsec, leftOK := neoLocalThreadSearchSnapshotChangeTime(left)
	rightSec, rightNsec, rightOK := neoLocalThreadSearchSnapshotChangeTime(right)
	return !leftOK && !rightOK || leftOK && rightOK && leftSec == rightSec && leftNsec == rightNsec
}

func neoLocalThreadSearchSnapshotChecksum(path string, maxBytes int64) (string, error) {
	handle, err := os.Open(path)
	if err != nil {
		return "", err
	}
	checksum := sha256.New()
	copied, copyErr := io.Copy(checksum, io.LimitReader(handle, maxBytes+1))
	closeErr := handle.Close()
	if copyErr != nil || closeErr != nil {
		return "", errors.Join(copyErr, closeErr)
	}
	if copied > maxBytes {
		return "", errors.New("amp local thread search snapshot exceeds size limit")
	}
	return hex.EncodeToString(checksum.Sum(nil)), nil
}

func neoLocalThreadSearchReadBounded(path string, maxBytes int64) ([]byte, error) {
	handle, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(handle, maxBytes+1))
	closeErr := handle.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	if int64(len(raw)) > maxBytes {
		return nil, errors.New("amp local thread search snapshot exceeds size limit")
	}
	return raw, nil
}

func writeNeoLocalThreadSearchSidecar(path string, raw []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	closed := false
	cleanup := true
	defer func() {
		if !closed {
			if errClose := tmp.Close(); errClose != nil {
				log.Errorf("amp neo local thread search sidecar close failed: %v", errClose)
			}
		}
		if cleanup {
			if errRemove := os.Remove(tmpPath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
				log.Errorf("amp neo local thread search sidecar cleanup failed: %v", errRemove)
			}
		}
	}()
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := replaceNeoLocalThreadSearchSidecar(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func neoLocalThreadSearchSnapshotChangeTime(info os.FileInfo) (int64, int64, bool) {
	if info == nil || info.Sys() == nil {
		return 0, 0, false
	}
	value := reflect.ValueOf(info.Sys())
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 0, 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, 0, false
	}
	for _, fieldName := range []string{"Ctim", "Ctimespec"} {
		field := value.FieldByName(fieldName)
		if !field.IsValid() || field.Kind() != reflect.Struct {
			continue
		}
		sec := field.FieldByName("Sec")
		nsec := field.FieldByName("Nsec")
		if sec.IsValid() && nsec.IsValid() && sec.CanInt() && nsec.CanInt() {
			return sec.Int(), nsec.Int(), true
		}
	}
	return 0, 0, false
}

func removeNeoLocalThreadSearchIndex(threadDir, threadID string) error {
	if threadDir == "" || !neoThreadIDExactPattern.MatchString(threadID) {
		return nil
	}
	guard := neoLocalThreadSearchIndexGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	return removeNeoLocalThreadSearchIndexLocked(threadDir, threadID)
}

func removeNeoLocalThreadSearchBloomIndex(threadDir, threadID string) error {
	if threadDir == "" || !neoThreadIDExactPattern.MatchString(threadID) {
		return nil
	}
	guard := neoLocalThreadSearchIndexGuard(threadID)
	guard.Lock()
	defer guard.Unlock()
	return errors.Join(removeNeoLocalThreadSearchIndexSourceLocked(threadDir, threadID), removeNeoLocalThreadSearchIndexBloomLocked(threadDir, threadID))
}

func removeNeoLocalThreadSearchIndexLocked(threadDir, threadID string) error {
	sourceErr := removeNeoLocalThreadSearchIndexSourceLocked(threadDir, threadID)
	bloomErr := removeNeoLocalThreadSearchIndexBloomLocked(threadDir, threadID)
	rankErr := removeNeoLocalThreadSearchRankLocked(threadDir, threadID)
	return errors.Join(sourceErr, bloomErr, rankErr)
}

func removeNeoLocalThreadSearchIndexSourceLocked(threadDir, threadID string) error {
	_, sourcePath := neoLocalThreadSearchIndexPaths(threadDir, threadID)
	if err := os.Remove(sourcePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", filepath.Base(sourcePath), err)
	}
	return nil
}

func removeNeoLocalThreadSearchIndexBloomLocked(threadDir, threadID string) error {
	indexPath, _ := neoLocalThreadSearchIndexPaths(threadDir, threadID)
	if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", filepath.Base(indexPath), err)
	}
	return nil
}

func removeNeoLocalThreadSearchRankLocked(threadDir, threadID string) error {
	path := neoLocalThreadSearchRankPath(threadDir, threadID)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove %s: %w", filepath.Base(path), err)
	}
	return nil
}

type neoLocalThreadSearchIndexReconcileStats struct {
	removed int
	failed  int
}

func reconcileNeoLocalThreadSearchIndexes(threadDir string) neoLocalThreadSearchIndexReconcileStats {
	var stats neoLocalThreadSearchIndexReconcileStats
	dir := filepath.Join(threadDir, neoLocalThreadSearchIndexDirName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			stats.failed++
			log.Debugf("amp neo local thread search index reconciliation read failed path=%s: %v", dir, err)
		}
		return stats
	}
	threadIDs := make(map[string]struct{})
	for _, entry := range entries {
		name := entry.Name()
		if tempThreadID, ok := neoLocalThreadSearchTempThreadID(name); ok {
			guard := neoLocalThreadSearchIndexGuard(tempThreadID)
			guard.Lock()
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				stats.failed++
				log.Debugf("amp neo local thread search index temp cleanup failed path=%s: %v", filepath.Join(dir, name), err)
			} else {
				stats.removed++
			}
			guard.Unlock()
			continue
		}
		var threadID string
		switch {
		case strings.HasSuffix(name, ".source"):
			threadID = strings.TrimSuffix(name, ".source")
		case strings.HasSuffix(name, ".bloom"):
			threadID = strings.TrimSuffix(name, ".bloom")
		case strings.HasSuffix(name, ".rank"):
			threadID = strings.TrimSuffix(name, ".rank")
		}
		if neoThreadIDExactPattern.MatchString(threadID) {
			threadIDs[threadID] = struct{}{}
		}
	}
	for threadID := range threadIDs {
		guard := neoLocalThreadSearchIndexGuard(threadID)
		guard.Lock()
		_, sourcePath := neoLocalThreadSearchIndexPaths(threadDir, threadID)
		_, snapshotErr := os.Stat(filepath.Join(threadDir, threadID+".json"))
		if snapshotErr != nil && !errors.Is(snapshotErr, os.ErrNotExist) {
			stats.failed++
			log.Debugf("amp neo local thread search snapshot reconciliation stat failed thread=%s: %v", threadID, snapshotErr)
			guard.Unlock()
			continue
		}
		if err := os.Remove(sourcePath); err == nil {
			stats.removed++
		} else if !errors.Is(err, os.ErrNotExist) {
			stats.failed++
		}
		if errors.Is(snapshotErr, os.ErrNotExist) {
			if err := removeNeoLocalThreadSearchIndexLocked(threadDir, threadID); err != nil {
				stats.failed++
				log.Debugf("amp neo local thread search index reconciliation failed thread=%s: %v", threadID, err)
			} else {
				stats.removed++
			}
		}
		guard.Unlock()
	}
	return stats
}

func neoLocalThreadSearchTempThreadID(name string) (string, bool) {
	if !strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".tmp") {
		return "", false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(name, "."), ".tmp")
	marker := strings.IndexByte(value, '.')
	if marker <= 0 || !neoThreadIDExactPattern.MatchString(value[:marker]) {
		return "", false
	}
	remainder := value[marker:]
	bloomToken := strings.TrimPrefix(remainder, ".bloom.")
	rankToken := strings.TrimPrefix(remainder, ".rank.")
	sourceToken := strings.TrimSuffix(remainder, ".source")
	bloomTemp := bloomToken != remainder && neoLocalThreadSearchTempToken(bloomToken)
	rankTemp := rankToken != remainder && neoLocalThreadSearchTempToken(rankToken)
	sourceTemp := sourceToken != remainder && strings.HasPrefix(sourceToken, ".") && neoLocalThreadSearchTempToken(strings.TrimPrefix(sourceToken, "."))
	if !bloomTemp && !rankTemp && !sourceTemp {
		return "", false
	}
	return value[:marker], true
}

func neoLocalThreadSearchTempToken(token string) bool {
	if token == "" {
		return false
	}
	for _, value := range token {
		if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' {
			continue
		}
		return false
	}
	return true
}
