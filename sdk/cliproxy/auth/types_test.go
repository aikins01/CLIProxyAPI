package auth

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type metadataCloneRecord struct {
	Values  []string
	Payload [1][]byte
	Count   *int
}

type metadataCloneMap map[string][]byte

type metadataLockedRecord struct {
	Lock   sync.Mutex
	Values []string
}

type metadataOpaqueLeafRecord struct {
	Channel chan int
	Values  []string
}

type metadataPrivateStateRecord struct {
	private string
	Values  []string
}

func TestToolPrefixDisabled(t *testing.T) {
	var a *Auth
	if a.ToolPrefixDisabled() {
		t.Error("nil auth should return false")
	}

	a = &Auth{}
	if a.ToolPrefixDisabled() {
		t.Error("empty auth should return false")
	}

	a = &Auth{Metadata: map[string]any{"tool_prefix_disabled": true}}
	if !a.ToolPrefixDisabled() {
		t.Error("should return true when set to true")
	}

	a = &Auth{Metadata: map[string]any{"tool_prefix_disabled": "true"}}
	if !a.ToolPrefixDisabled() {
		t.Error("should return true when set to string 'true'")
	}

	a = &Auth{Metadata: map[string]any{"tool-prefix-disabled": true}}
	if !a.ToolPrefixDisabled() {
		t.Error("should return true with kebab-case key")
	}

	a = &Auth{Metadata: map[string]any{"tool_prefix_disabled": false}}
	if a.ToolPrefixDisabled() {
		t.Error("should return false when set to false")
	}
}

func TestCloneMetadataDetachesReferenceValues(t *testing.T) {
	count := 3
	customMap := metadataCloneMap{"key": {1, 2}}
	record := &metadataCloneRecord{
		Values:  []string{"first"},
		Payload: [1][]byte{{3, 4}},
		Count:   &count,
	}
	nested := map[string]any{"items": []any{[]byte{5, 6}}}
	cycle := map[string]any{}
	cycle["self"] = cycle
	original := map[string]any{
		"bool":       true,
		"int":        7,
		"string":     "value",
		"time":       time.Unix(1_700_000_000, 0),
		"custom_map": customMap,
		"record":     record,
		"nested":     nested,
		"cycle":      cycle,
	}

	cloned := cloneMetadata(original)
	customMap["key"][0] = 9
	record.Values[0] = "changed"
	record.Payload[0][0] = 9
	*record.Count = 9
	nested["items"].([]any)[0].([]byte)[0] = 9
	cycle["original-only"] = true

	if cloned["bool"] != true || cloned["int"] != 7 || cloned["string"] != "value" || cloned["time"] != original["time"] {
		t.Fatalf("immutable clone values = %#v", cloned)
	}
	if got := cloned["custom_map"].(metadataCloneMap)["key"][0]; got != 1 {
		t.Fatalf("custom map clone byte = %d, want 1", got)
	}
	clonedRecord := cloned["record"].(*metadataCloneRecord)
	if clonedRecord == record || clonedRecord.Values[0] != "first" || clonedRecord.Payload[0][0] != 3 || *clonedRecord.Count != 3 {
		t.Fatalf("record clone = %#v", clonedRecord)
	}
	if got := cloned["nested"].(map[string]any)["items"].([]any)[0].([]byte)[0]; got != 5 {
		t.Fatalf("nested clone byte = %d, want 5", got)
	}
	clonedCycle := cloned["cycle"].(map[string]any)
	if _, ok := clonedCycle["original-only"]; ok {
		t.Fatal("cycle clone shares mutations with original")
	}
	clonedCycle["self"].(map[string]any)["clone-only"] = true
	if _, ok := clonedCycle["clone-only"]; !ok {
		t.Fatal("cycle clone did not preserve its self reference")
	}
}

func TestCloneMetadataPreservesOpaqueReferenceValues(t *testing.T) {
	channel := make(chan int)
	function := func() {}
	cloned := cloneMetadata(map[string]any{
		"channel":  channel,
		"function": function,
	})

	if cloned["channel"] != channel || reflect.ValueOf(cloned["function"]).Pointer() != reflect.ValueOf(function).Pointer() {
		t.Fatalf("clone = %#v", cloned)
	}
}

func TestCloneMetadataDetachesContainersWithOpaqueLeaves(t *testing.T) {
	channel := make(chan int)
	function := func() {}
	channels := map[string]chan int{"original": channel}
	functions := []func(){function}
	record := &metadataOpaqueLeafRecord{Channel: channel, Values: []string{"original"}}
	cloned := cloneMetadata(map[string]any{
		"channels":  channels,
		"functions": functions,
		"record":    record,
	})

	channels["added"] = make(chan int)
	functions[0] = func() {}
	record.Values[0] = "changed"

	clonedChannels := cloned["channels"].(map[string]chan int)
	if len(clonedChannels) != 1 || clonedChannels["original"] != channel {
		t.Fatalf("channel map clone = %#v", clonedChannels)
	}
	clonedFunctions := cloned["functions"].([]func())
	if len(clonedFunctions) != 1 || reflect.ValueOf(clonedFunctions[0]).Pointer() != reflect.ValueOf(function).Pointer() {
		t.Fatalf("function slice clone = %#v", clonedFunctions)
	}
	clonedRecord := cloned["record"].(*metadataOpaqueLeafRecord)
	if clonedRecord == record || clonedRecord.Channel != channel || clonedRecord.Values[0] != "original" {
		t.Fatalf("record clone = %#v", clonedRecord)
	}
}

func TestCloneMetadataPreservesStructsWithSynchronizationState(t *testing.T) {
	record := &metadataLockedRecord{Values: []string{"value"}}
	record.Lock.Lock()
	defer record.Lock.Unlock()
	cloned := cloneMetadata(map[string]any{"record": record})
	if cloned["record"] != record {
		t.Fatal("clone copied a struct containing synchronization state")
	}
}

func TestCloneMetadataDetachesExportedReferencesWithPrivateState(t *testing.T) {
	record := &metadataPrivateStateRecord{private: "state", Values: []string{"original"}}
	cloned := cloneMetadata(map[string]any{"record": record})["record"].(*metadataPrivateStateRecord)
	record.Values[0] = "changed"
	if cloned == record || cloned.private != "state" || cloned.Values[0] != "original" {
		t.Fatalf("record clone = %#v", cloned)
	}
}

func TestEnsureIndexUsesCredentialIdentity(t *testing.T) {
	t.Parallel()

	geminiAuth := &Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
			"source":  "config:gemini[abc123]",
		},
	}
	compatAuth := &Auth{
		Provider: "bohe",
		Attributes: map[string]string{
			"api_key":      "shared-key",
			"compat_name":  "bohe",
			"provider_key": "bohe",
			"source":       "config:bohe[def456]",
		},
	}
	geminiAltBase := &Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key":  "shared-key",
			"base_url": "https://alt.example.com",
			"source":   "config:gemini[ghi789]",
		},
	}
	geminiDuplicate := &Auth{
		Provider: "gemini",
		Attributes: map[string]string{
			"api_key": "shared-key",
			"source":  "config:gemini[abc123-1]",
		},
	}

	geminiIndex := geminiAuth.EnsureIndex()
	compatIndex := compatAuth.EnsureIndex()
	altBaseIndex := geminiAltBase.EnsureIndex()
	duplicateIndex := geminiDuplicate.EnsureIndex()

	if geminiIndex == "" {
		t.Fatal("gemini index should not be empty")
	}
	if compatIndex == "" {
		t.Fatal("compat index should not be empty")
	}
	if altBaseIndex == "" {
		t.Fatal("alt base index should not be empty")
	}
	if duplicateIndex == "" {
		t.Fatal("duplicate index should not be empty")
	}
	if geminiIndex == compatIndex {
		t.Fatalf("shared api key produced duplicate auth_index %q", geminiIndex)
	}
	if geminiIndex == altBaseIndex {
		t.Fatalf("same provider/key with different base_url produced duplicate auth_index %q", geminiIndex)
	}
	if geminiIndex != duplicateIndex {
		t.Fatalf("same provider/key with different source should share auth_index, got %q vs %q", geminiIndex, duplicateIndex)
	}
}

func TestEnsureIndexUsesOAuthTypeAndAbsolutePath(t *testing.T) {
	t.Parallel()

	wd, errWd := os.Getwd()
	if errWd != nil {
		t.Fatalf("os.Getwd returned error: %v", errWd)
	}

	relPath := "test-oauth.json"
	absPath := filepath.Join(wd, relPath)
	expectedSeed := "gemini:" + filepath.Clean(absPath)
	expectedIndex := stableAuthIndex(expectedSeed)

	a := &Auth{
		Provider: "gemini-cli",
		Attributes: map[string]string{
			"path": relPath,
		},
		Metadata: map[string]any{
			"type": "gemini",
		},
	}

	got := a.EnsureIndex()
	if got == "" {
		t.Fatal("auth index should not be empty")
	}
	if got != expectedIndex {
		t.Fatalf("auth index = %q, want %q", got, expectedIndex)
	}
}

func TestRecentRequestsSnapshotEmptyReturnsTwentyBuckets(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).In(time.Local)
	a := &Auth{}

	got := a.RecentRequestsSnapshot(now)
	if len(got) != recentRequestBucketCount {
		t.Fatalf("len = %d, want %d", len(got), recentRequestBucketCount)
	}

	currentBucketID := now.Unix() / recentRequestBucketSeconds
	baseBucketID := currentBucketID - int64(recentRequestBucketCount-1)
	for i, bucket := range got {
		if bucket.Success != 0 || bucket.Failed != 0 {
			t.Fatalf("bucket[%d] counts = %d/%d, want 0/0", i, bucket.Success, bucket.Failed)
		}
		if strings.TrimSpace(bucket.Time) == "" {
			t.Fatalf("bucket[%d] time label is empty", i)
		}
		expectedBucketID := baseBucketID + int64(i)
		start := time.Unix(expectedBucketID*recentRequestBucketSeconds, 0).In(time.Local)
		end := start.Add(10 * time.Minute)
		expected := start.Format("15:04") + "-" + end.Format("15:04")
		if bucket.Time != expected {
			t.Fatalf("bucket[%d] time = %q, want %q", i, bucket.Time, expected)
		}
	}
}

func TestRecentRequestsSnapshotIncludesCounts(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).In(time.Local)
	a := &Auth{}

	a.recordRecentRequest(now, true)
	a.recordRecentRequest(now, false)

	got := a.RecentRequestsSnapshot(now)
	if len(got) != recentRequestBucketCount {
		t.Fatalf("len = %d, want %d", len(got), recentRequestBucketCount)
	}

	newest := got[len(got)-1]
	if newest.Success != 1 || newest.Failed != 1 {
		t.Fatalf("newest bucket = success=%d failed=%d, want 1/1", newest.Success, newest.Failed)
	}
}

func TestRecentRequestsSnapshotBucketAdvanceMovesCounts(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).In(time.Local)
	next := now.Add(10 * time.Minute)
	a := &Auth{}

	a.recordRecentRequest(now, true)
	a.recordRecentRequest(next, false)

	got := a.RecentRequestsSnapshot(next)
	if len(got) != recentRequestBucketCount {
		t.Fatalf("len = %d, want %d", len(got), recentRequestBucketCount)
	}

	secondNewest := got[len(got)-2]
	newest := got[len(got)-1]
	if secondNewest.Success != 1 || secondNewest.Failed != 0 {
		t.Fatalf("second newest bucket = success=%d failed=%d, want 1/0", secondNewest.Success, secondNewest.Failed)
	}
	if newest.Success != 0 || newest.Failed != 1 {
		t.Fatalf("newest bucket = success=%d failed=%d, want 0/1", newest.Success, newest.Failed)
	}
}
