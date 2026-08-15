package amp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const neoOrbLifecycleTestProvider = "unix:///var/run/docker.sock"

func neoOrbLifecycleOwnershipFixture() (string, neoOrbLifecycleGeneration) {
	ownerID := "0123456789abcdef0123456789abcdef"
	threadID := "T-019FDEC9-B0CF-745D-8DA4-F250184E870E"
	containerName, homeVolumeName, rootVolumeName := neoOrbLifecycleResourceNames(ownerID, threadID, 7)
	return ownerID, neoOrbLifecycleGeneration{
		ThreadID:       threadID,
		Generation:     7,
		ContainerID:    "container-7",
		ContainerName:  containerName,
		HomeVolumeName: homeVolumeName,
		RootVolumeName: rootVolumeName,
		Phase:          neoOrbLifecyclePhaseActive,
		CreatedAt:      "2026-08-15T12:34:56Z",
	}
}

func newNeoOrbLifecycleTestStore(t *testing.T, provider string) (*neoOrbLifecycleStore, string) {
	t.Helper()
	parent := t.TempDir()
	threadDir := filepath.Join(parent, "threads")
	store, err := newNeoOrbLifecycleStore(threadDir, provider)
	if err != nil {
		t.Fatalf("newNeoOrbLifecycleStore: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close lifecycle store: %v", err)
		}
	})
	return store, threadDir
}

func closeNeoOrbLifecycleTestStore(t *testing.T, store *neoOrbLifecycleStore) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Errorf("close lifecycle store: %v", err)
	}
}

func activateNeoOrbLifecycleTestGeneration(t *testing.T, store *neoOrbLifecycleStore, threadID, containerID string) neoOrbLifecycleGeneration {
	t.Helper()
	generation, err := store.reserveGeneration(threadID, "portal-token")
	if err != nil {
		t.Fatalf("reserve lifecycle generation: %v", err)
	}
	generation.ContainerID = containerID
	if err := store.promoteGeneration(generation); err != nil {
		t.Fatalf("promote lifecycle generation: %v", err)
	}
	active := store.snapshot().Threads[threadID].Active
	if active == nil {
		t.Fatal("promoted lifecycle generation is missing")
	}
	return *active
}

func activateNeoOrbBoundLifecycleTestGeneration(t *testing.T, store *neoOrbLifecycleStore, threadID, containerID, authenticatedOwnerID string) neoOrbLifecycleGeneration {
	t.Helper()
	generation, err := store.reserveGeneration(threadID, strings.Repeat("p", neoOrbPortalTokenByteCount))
	if err != nil {
		t.Fatalf("reserve bound lifecycle generation: %v", err)
	}
	generation.ContainerID = containerID
	active, err := store.promoteGenerationBound(generation, authenticatedOwnerID)
	if err != nil {
		t.Fatalf("promote bound lifecycle generation: %v", err)
	}
	return active
}

func TestNeoOrbLifecycleStoreReplacesOnlyExactActiveContainerID(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	active := activateNeoOrbLifecycleTestGeneration(t, store, threadID, "container-old")
	before := store.snapshot()
	if err := store.replaceActiveContainerID(active, "container-new"); err != nil {
		t.Fatalf("replace active container identifier: %v", err)
	}
	after := store.snapshot()
	want := cloneNeoOrbLifecycleState(before)
	record := want.Threads[threadID]
	replacement := *record.Active
	replacement.ContainerID = "container-new"
	record.Active = &replacement
	want.Threads[threadID] = record
	if !reflect.DeepEqual(after, want) {
		t.Fatalf("replacement changed fields besides the active container identifier:\nafter=%#v\nwant=%#v", after, want)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before reload: %v", err)
	}
	reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reload replaced lifecycle store: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reloaded)
	if reloadedActive := reloaded.snapshot().Threads[threadID].Active; reloadedActive == nil || *reloadedActive != replacement {
		t.Fatalf("reloaded active generation = %#v", reloadedActive)
	}
}

func TestNeoOrbLifecycleStoreRejectsInexactActiveContainerReplacement(t *testing.T) {
	tests := map[string]func(*testing.T, *neoOrbLifecycleStore, neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string){
		"stale expected": func(_ *testing.T, _ *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			active.ContainerID = "container-stale"
			return active, "container-new"
		},
		"missing active": func(_ *testing.T, _ *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			active.ThreadID = "T-019fdec9-b0cf-745d-8da4-f250184e870f"
			return active, "container-new"
		},
		"pending generation": func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			if _, err := store.reserveGeneration(active.ThreadID, "portal-token-pending"); err != nil {
				t.Fatalf("reserve pending generation: %v", err)
			}
			return active, "container-new"
		},
		"cleanup tombstone": func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			if _, found, err := store.beginCleanup(active.ThreadID); err != nil || !found {
				t.Fatalf("begin lifecycle cleanup: found=%v err=%v", found, err)
			}
			return active, "container-new"
		},
		"invalid new identifier": func(_ *testing.T, _ *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			return active, "invalid/container"
		},
		"unchanged identifier": func(_ *testing.T, _ *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			return active, active.ContainerID
		},
		"duplicate identifier": func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, string) {
			activateNeoOrbLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870f", "container-duplicate")
			return active, "container-duplicate"
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			active := activateNeoOrbLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-old")
			expected, newContainerID := prepare(t, store, active)
			before := store.snapshot()
			if err := store.replaceActiveContainerID(expected, newContainerID); err == nil {
				t.Fatal("inexact active container replacement succeeded")
			}
			if after := store.snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected replacement changed lifecycle state:\nafter=%#v\nbefore=%#v", after, before)
			}
		})
	}
}

func TestNeoOrbLifecycleStoreActiveContainerReplacementRollbackAndDurability(t *testing.T) {
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	t.Run("unpublished write failure rolls back", func(t *testing.T) {
		store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		active := activateNeoOrbLifecycleTestGeneration(t, store, threadID, "container-old")
		before := store.snapshot()
		store.writeAtomic = func(string, []byte, os.FileMode) error { return errors.New("injected write failure") }
		if err := store.replaceActiveContainerID(active, "container-new"); err == nil {
			t.Fatal("replacement with failed write succeeded")
		}
		if after := store.snapshot(); !reflect.DeepEqual(after, before) {
			t.Fatalf("unpublished replacement was not rolled back:\nafter=%#v\nbefore=%#v", after, before)
		}
	})
	t.Run("published directory sync failure keeps replacement", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		active := activateNeoOrbLifecycleTestGeneration(t, store, threadID, "container-old")
		originalSync := store.syncDirectory
		store.syncDirectory = func(string) error { return errors.New("injected directory sync failure") }
		err := store.replaceActiveContainerID(active, "container-new")
		var durabilityErr *neoOrbLifecycleDurabilityError
		if !errors.As(err, &durabilityErr) {
			t.Fatalf("replacement error = %v, want durability uncertainty", err)
		}
		if replaced := store.snapshot().Threads[threadID].Active; replaced == nil || replaced.ContainerID != "container-new" {
			t.Fatalf("published replacement = %#v", replaced)
		}
		store.syncDirectory = originalSync
		if err := store.Close(); err != nil {
			t.Fatalf("close lifecycle store after durability recovery: %v", err)
		}
		reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reload published replacement: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reloaded)
		if replaced := reloaded.snapshot().Threads[threadID].Active; replaced == nil || replaced.ContainerID != "container-new" {
			t.Fatalf("reloaded published replacement = %#v", replaced)
		}
	})
}

func TestNeoOrbLifecycleStoreInitializesAndReloadsDurably(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, " unix:///var/run/docker.sock/../docker.sock ")
	state := store.snapshot()
	if state.Version != neoOrbLifecycleStoreVersion || state.Provider != neoOrbLifecycleTestProvider || !neoOrbLifecycleOwnerIDValid(state.OwnerID) || len(state.GenerationHighWater) != 0 || len(state.Threads) != 0 || len(state.CleanupTombstones) != 0 {
		t.Fatalf("initial state = %#v", state)
	}
	info, err := os.Stat(store.path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("store mode = %v, %v", info, err)
	}
	parentInfo, err := os.Stat(filepath.Dir(store.path))
	if err != nil || parentInfo.Mode().Perm() != 0o700 {
		t.Fatalf("parent mode = %v, %v", parentInfo, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before reload: %v", err)
	}
	reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reload lifecycle store: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reloaded)
	if reloaded.ownerID() != state.OwnerID {
		t.Fatalf("owner changed from %q to %q", state.OwnerID, reloaded.ownerID())
	}
}

func TestNeoOrbLifecycleStoreReservationPromotionAndClearAreExact(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	fixedNow := time.Date(2026, 8, 15, 12, 34, 56, 789, time.UTC)
	store.now = func() time.Time { return fixedNow }
	threadID := "T-019FDEC9-B0CF-745D-8DA4-F250184E870E"
	first, err := store.reserveGeneration(threadID, "portal-token-1")
	if err != nil {
		t.Fatalf("reserve first generation: %v", err)
	}
	expectedBase := "cliproxy-orb-" + store.ownerID()[:12] + "-" + strings.ToLower(threadID) + "-g000001"
	if first.Generation != 1 || first.ContainerName != expectedBase || first.HomeVolumeName != expectedBase+"-home" || first.RootVolumeName != expectedBase+"-root" || first.Phase != neoOrbLifecyclePhasePending || first.CreatedAt != fixedNow.Format(time.RFC3339Nano) {
		t.Fatalf("first generation = %#v", first)
	}
	expectedLabels := map[string]string{
		"cliproxy.orb":            threadID,
		"cliproxy.orb.owner":      store.ownerID(),
		"cliproxy.orb.lifecycle":  "1",
		"cliproxy.orb.generation": "1",
		"cliproxy.orb.role":       neoOrbLifecycleRoleHome,
		"cliproxy.orb.created-at": fixedNow.Format(time.RFC3339Nano),
	}
	if labels := neoOrbLifecycleLabels(store.ownerID(), first, neoOrbLifecycleRoleHome); !reflect.DeepEqual(labels, expectedLabels) {
		t.Fatalf("labels = %#v", labels)
	}
	wrong := first
	wrong.ContainerName += "-wrong"
	wrong.ContainerID = "container-1"
	if err := store.promoteGeneration(wrong); err == nil {
		t.Fatal("promoted mismatched pending generation")
	}
	first.ContainerID = "container-1"
	if err := store.promoteGeneration(first); err != nil {
		t.Fatalf("promote first generation: %v", err)
	}
	second, err := store.reserveGeneration(threadID, "portal-token-2")
	if err != nil || second.Generation != 2 {
		t.Fatalf("reserve second generation = %#v, %v", second, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before reload: %v", err)
	}
	reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reload pending generation: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reloaded)
	reloadedState := reloaded.snapshot()
	if reloadedState.Threads[threadID].Pending == nil || !reflect.DeepEqual(*reloadedState.Threads[threadID].Pending, second) {
		t.Fatalf("pending generation was not durable: %#v", reloadedState.Threads[threadID])
	}
	wrong = second
	wrong.HomeVolumeName += "-wrong"
	if err := reloaded.clearPendingGeneration(wrong); err == nil {
		t.Fatal("cleared mismatched pending generation")
	}
	if err := reloaded.clearPendingGeneration(second); err != nil {
		t.Fatalf("clear exact pending generation: %v", err)
	}
	third, err := reloaded.reserveGeneration(threadID, "portal-token-3")
	if err != nil || third.Generation != 3 {
		t.Fatalf("reserve after cleared intent = %#v, %v", third, err)
	}
	third.ContainerID = "container-2"
	if err := reloaded.promoteGeneration(third); err != nil {
		t.Fatalf("promote replacement generation: %v", err)
	}
	state := reloaded.snapshot()
	record := state.Threads[threadID]
	if state.GenerationHighWater[threadID] != 3 || record.Active == nil || record.Active.Generation != 3 || record.Active.Phase != neoOrbLifecyclePhaseActive || len(record.Retained) != 1 || record.Retained[0].Generation != 1 || record.Retained[0].Phase != neoOrbLifecyclePhaseRetained {
		t.Fatalf("promoted record = %#v", record)
	}
}

func TestNeoOrbLifecycleVolumeOwnershipIsExact(t *testing.T) {
	ownerID, generation := neoOrbLifecycleOwnershipFixture()
	validState := func() neoOrbVolumeState {
		return neoOrbVolumeState{
			Exists:  true,
			Name:    generation.HomeVolumeName,
			Driver:  "local",
			Labels:  neoOrbLifecycleLabels(ownerID, generation, neoOrbLifecycleRoleHome),
			Scope:   "local",
			Options: map[string]string{},
		}
	}
	if !neoOrbLifecycleVolumeMatches(validState(), ownerID, generation, neoOrbLifecycleRoleHome) {
		t.Fatal("exact lifecycle volume was rejected")
	}
	tests := map[string]func(*neoOrbVolumeState){
		"owner": func(state *neoOrbVolumeState) {
			state.Labels["cliproxy.orb.owner"] = "fedcba9876543210fedcba9876543210"
		},
		"generation": func(state *neoOrbVolumeState) {
			state.Labels["cliproxy.orb.generation"] = "8"
		},
		"role": func(state *neoOrbVolumeState) {
			state.Labels["cliproxy.orb.role"] = neoOrbLifecycleRoleRoot
		},
		"created-at": func(state *neoOrbVolumeState) {
			state.Labels["cliproxy.orb.created-at"] = "2026-08-15T12:34:57Z"
		},
		"name": func(state *neoOrbVolumeState) {
			state.Name = generation.HomeVolumeName + "-foreign"
		},
		"driver": func(state *neoOrbVolumeState) {
			state.Driver = "foreign"
		},
		"scope": func(state *neoOrbVolumeState) {
			state.Scope = "global"
		},
		"options": func(state *neoOrbVolumeState) {
			state.Options["foreign"] = "true"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := validState()
			mutate(&state)
			if neoOrbLifecycleVolumeMatches(state, ownerID, generation, neoOrbLifecycleRoleHome) {
				t.Fatalf("accepted mismatched lifecycle volume: %#v", state)
			}
		})
	}
}

func TestNeoOrbLifecycleContainerOwnershipIsExact(t *testing.T) {
	ownerID, generation := neoOrbLifecycleOwnershipFixture()
	homeVolume := neoOrbVolumeState{Exists: true, Name: generation.HomeVolumeName, Driver: "local", Mountpoint: "/var/lib/docker/volumes/" + generation.HomeVolumeName + "/_data", Labels: neoOrbLifecycleLabels(ownerID, generation, neoOrbLifecycleRoleHome), Scope: "local"}
	rootVolume := neoOrbVolumeState{Exists: true, Name: generation.RootVolumeName, Driver: "local", Mountpoint: "/var/lib/docker/volumes/" + generation.RootVolumeName + "/_data", Labels: neoOrbLifecycleLabels(ownerID, generation, neoOrbLifecycleRoleRoot), Scope: "local"}
	validState := func() neoOrbContainerState {
		return neoOrbContainerState{
			Exists:        true,
			ID:            generation.ContainerID,
			Name:          "/" + generation.ContainerName,
			Labels:        neoOrbLifecycleLabels(ownerID, generation, neoOrbLifecycleRoleContainer),
			RestartPolicy: "unless-stopped",
			Mounts: []neoOrbContainerMount{
				{Type: "volume", Source: homeVolume.Mountpoint, Name: generation.HomeVolumeName, Destination: "/home/user"},
				{Type: "volume", Source: rootVolume.Mountpoint, Name: generation.RootVolumeName, Destination: "/root"},
			},
		}
	}
	if !neoOrbLifecycleContainerMatches(generation.ContainerID, validState(), homeVolume, rootVolume, ownerID, generation) {
		t.Fatal("exact lifecycle container was rejected")
	}
	tests := map[string]func(*neoOrbContainerState){
		"owner": func(state *neoOrbContainerState) {
			state.Labels["cliproxy.orb.owner"] = "fedcba9876543210fedcba9876543210"
		},
		"generation": func(state *neoOrbContainerState) {
			state.Labels["cliproxy.orb.generation"] = "8"
		},
		"role": func(state *neoOrbContainerState) {
			state.Labels["cliproxy.orb.role"] = neoOrbLifecycleRoleHome
		},
		"created-at": func(state *neoOrbContainerState) {
			state.Labels["cliproxy.orb.created-at"] = "2026-08-15T12:34:57Z"
		},
		"name": func(state *neoOrbContainerState) {
			state.Name = "/" + generation.ContainerName + "-foreign"
		},
		"id": func(state *neoOrbContainerState) {
			state.ID = "container-foreign"
		},
		"mount": func(state *neoOrbContainerState) {
			state.Mounts[1].Name = generation.HomeVolumeName
		},
		"mount-source": func(state *neoOrbContainerState) {
			state.Mounts[0].Source = "/var/lib/docker/volumes/foreign/_data"
		},
		"restart-policy": func(state *neoOrbContainerState) {
			state.RestartPolicy = "no"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			state := validState()
			mutate(&state)
			if neoOrbLifecycleContainerMatches(generation.ContainerID, state, homeVolume, rootVolume, ownerID, generation) {
				t.Fatalf("accepted mismatched lifecycle container: %#v", state)
			}
		})
	}
	if neoOrbLifecycleContainerMatches("container-foreign", validState(), homeVolume, rootVolume, ownerID, generation) {
		t.Fatal("accepted mismatched lifecycle container identifier")
	}
}

func TestNeoOrbLifecycleVolumeCreationInspectsAndRejectsForeignAdoption(t *testing.T) {
	ownerID, generation := neoOrbLifecycleOwnershipFixture()
	fake := &neoOrbFakeProvider{}
	state, err := neoOrbCreateLifecycleVolume(context.Background(), fake, ownerID, generation, neoOrbLifecycleRoleHome)
	if err != nil || !neoOrbLifecycleVolumeMatches(state, ownerID, generation, neoOrbLifecycleRoleHome) {
		t.Fatalf("create exact lifecycle volume = %#v, %v", state, err)
	}
	if fake.callCount("create-volume:"+generation.HomeVolumeName) != 1 || fake.callCount("inspect-volume:"+generation.HomeVolumeName) != 1 {
		t.Fatalf("create lifecycle volume calls = %#v", fake.calls)
	}
	foreign := neoOrbVolumeState{
		Exists: true,
		Name:   generation.RootVolumeName,
		Driver: "local",
		Labels: neoOrbLifecycleLabels("fedcba9876543210fedcba9876543210", generation, neoOrbLifecycleRoleRoot),
		Scope:  "local",
	}
	fake = &neoOrbFakeProvider{volumes: map[string]neoOrbVolumeState{generation.RootVolumeName: foreign}}
	if _, err := neoOrbCreateLifecycleVolume(context.Background(), fake, ownerID, generation, neoOrbLifecycleRoleRoot); err == nil {
		t.Fatal("adopted foreign same-name lifecycle volume")
	}
	if fake.callCount("create-volume:"+generation.RootVolumeName) != 1 || fake.callCount("inspect-volume:"+generation.RootVolumeName) != 1 {
		t.Fatalf("foreign lifecycle volume calls = %#v", fake.calls)
	}
}

func TestNeoOrbLifecycleStoreRejectsCorruptionWithoutOverwrite(t *testing.T) {
	tests := map[string]func([]byte) []byte{
		"unknown field": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("{\n"), []byte("{\n  \"unknown\": true,\n"), 1)
		},
		"duplicate field": func(raw []byte) []byte {
			marker := []byte("  \"ownerId\": \"")
			index := bytes.Index(raw, marker)
			if index < 0 {
				return raw
			}
			lineEnd := bytes.IndexByte(raw[index:], '\n')
			line := append([]byte(nil), raw[index:index+lineEnd+1]...)
			return append(append(append([]byte(nil), raw[:index]...), line...), raw[index:]...)
		},
		"trailing JSON": func(raw []byte) []byte {
			return append(append([]byte(nil), raw...), []byte("{}\n")...)
		},
		"unsupported version": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("\"version\": 3"), []byte("\"version\": 4"), 1)
		},
		"legacy version": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("\"version\": 3"), []byte("\"version\": 1"), 1)
		},
		"missing generation high-water": func(raw []byte) []byte {
			return bytes.Replace(raw, []byte("  \"generationHighWater\": {},\n"), nil, 1)
		},
	}
	for name, corrupt := range tests {
		t.Run(name, func(t *testing.T) {
			store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			raw, err := os.ReadFile(store.path)
			if err != nil {
				t.Fatalf("read lifecycle store: %v", err)
			}
			corrupted := corrupt(raw)
			if bytes.Equal(corrupted, raw) {
				t.Fatal("test corruption did not change store")
			}
			if err := store.Close(); err != nil {
				t.Fatalf("close lifecycle store before corruption: %v", err)
			}
			if err := os.WriteFile(store.path, corrupted, 0o600); err != nil {
				t.Fatalf("write corrupt lifecycle store: %v", err)
			}
			if _, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider); err == nil {
				t.Fatal("corrupt lifecycle store loaded")
			}
			after, err := os.ReadFile(store.path)
			if err != nil || !bytes.Equal(after, corrupted) {
				t.Fatalf("corrupt lifecycle store was overwritten: equal=%v, err=%v", bytes.Equal(after, corrupted), err)
			}
		})
	}
}

func TestNeoOrbLifecycleStoreLoadsVersion2WithoutActivation(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	active := activateNeoOrbLifecycleTestGeneration(t, store, threadID, "container-v2")
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before v2 fixture: %v", err)
	}
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatalf("read lifecycle v2 fixture: %v", err)
	}
	v2 := bytes.Replace(raw, []byte("\"version\": 3"), []byte("\"version\": 2"), 1)
	if bytes.Equal(v2, raw) {
		t.Fatal("version 2 fixture did not change store")
	}
	if err := os.WriteFile(store.path, v2, 0o600); err != nil {
		t.Fatalf("write lifecycle v2 fixture: %v", err)
	}
	reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("load lifecycle v2 fixture: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reloaded)
	state := reloaded.snapshot()
	loaded := state.Threads[threadID].Active
	if state.Version != neoOrbLifecycleStoreVersion || loaded == nil || loaded.AuthenticatedOwnerID != "" || loaded.Revision != 0 || loaded.ActivationState != "" || loaded.OperationID != "" || loaded.OperationKind != "" || neoOrbLifecycleActionable(loaded) {
		t.Fatalf("loaded lifecycle v2 active = %#v, version=%d", loaded, state.Version)
	}
	want := active
	if *loaded != want {
		t.Fatalf("loaded lifecycle v2 generation = %#v, want %#v", *loaded, want)
	}
}

func TestNeoOrbLifecycleStoreRejectsVersion2ActivationData(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	activateNeoOrbBoundLifecycleTestGeneration(t, store, threadID, "container-v2-bound", "owner-v2")
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before invalid v2 fixture: %v", err)
	}
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatalf("read invalid lifecycle v2 fixture: %v", err)
	}
	v2 := bytes.Replace(raw, []byte("\"version\": 3"), []byte("\"version\": 2"), 1)
	if bytes.Equal(v2, raw) {
		t.Fatal("invalid version 2 fixture did not change store")
	}
	if err := os.WriteFile(store.path, v2, 0o600); err != nil {
		t.Fatalf("write invalid lifecycle v2 fixture: %v", err)
	}
	if _, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider); err == nil || !strings.Contains(err.Error(), "version 2 contains activation data") {
		t.Fatalf("invalid lifecycle v2 activation error = %v", err)
	}
	after, err := os.ReadFile(store.path)
	if err != nil || !bytes.Equal(after, v2) {
		t.Fatalf("invalid lifecycle v2 fixture was overwritten: equal=%v err=%v", bytes.Equal(after, v2), err)
	}
}

func TestNeoOrbLifecycleActivationVersion3RoundTrip(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	binding := activateNeoOrbBoundLifecycleTestGeneration(t, store, threadID, "container-round-trip", "owner-round-trip")
	launchingStart, err := store.beginActivation(binding, "operation-start", neoOrbLifecycleOperationStart)
	if err != nil {
		t.Fatalf("begin start activation: %v", err)
	}
	if launchingStart.ActivationState != neoOrbLifecycleActivationLaunching || launchingStart.OperationID != "operation-start" || launchingStart.OperationKind != neoOrbLifecycleOperationStart {
		t.Fatalf("start activation = %#v", launchingStart)
	}
	launchingExec, err := store.beginActivation(launchingStart, "operation-exec", neoOrbLifecycleOperationExecDetached)
	if err != nil {
		t.Fatalf("begin detached activation: %v", err)
	}
	if launchingExec.ActivationState != neoOrbLifecycleActivationLaunching || launchingExec.OperationID != "operation-exec" || launchingExec.OperationKind != neoOrbLifecycleOperationExecDetached {
		t.Fatalf("detached activation = %#v", launchingExec)
	}
	actionable, err := store.finishActivation(launchingExec)
	if err != nil {
		t.Fatalf("finish detached activation: %v", err)
	}
	if !neoOrbLifecycleActionable(&actionable) || actionable.OperationID != "" || actionable.OperationKind != "" {
		t.Fatalf("finished activation = %#v", actionable)
	}
	launchingUnpause, err := store.beginActivation(actionable, "operation-unpause", neoOrbLifecycleOperationUnpause)
	if err != nil {
		t.Fatalf("begin unpause activation: %v", err)
	}
	actionable, err = store.finishActivation(launchingUnpause)
	if err != nil {
		t.Fatalf("finish unpause activation: %v", err)
	}
	if !neoOrbLifecycleActionable(&actionable) {
		t.Fatalf("unpause activation = %#v", actionable)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle activation store: %v", err)
	}
	reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reload lifecycle activation store: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reloaded)
	state := reloaded.snapshot()
	loaded := state.Threads[threadID].Active
	if state.Version != neoOrbLifecycleStoreVersion || loaded == nil || *loaded != actionable || !neoOrbLifecycleActionable(loaded) {
		t.Fatalf("reloaded activation = %#v, version=%d", loaded, state.Version)
	}
}

func TestNeoOrbLifecycleActivationCASRejectsStaleOrInvalidatedState(t *testing.T) {
	type testCase struct {
		prepare func(*testing.T, *neoOrbLifecycleStore, neoOrbLifecycleGeneration) neoOrbLifecycleGeneration
		invoke  func(*neoOrbLifecycleStore, neoOrbLifecycleGeneration) error
	}
	tests := map[string]testCase{
		"stale generation": {
			prepare: func(_ *testing.T, _ *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) neoOrbLifecycleGeneration {
				active.Generation++
				return active
			},
			invoke: func(store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration) error {
				_, err := store.beginActivation(expected, "operation-start", neoOrbLifecycleOperationStart)
				return err
			},
		},
		"stale operation": {
			prepare: func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) neoOrbLifecycleGeneration {
				launching, err := store.beginActivation(active, "operation-start", neoOrbLifecycleOperationStart)
				if err != nil {
					t.Fatalf("begin start activation: %v", err)
				}
				launching.OperationID = "operation-stale"
				return launching
			},
			invoke: func(store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration) error {
				_, err := store.beginActivation(expected, "operation-exec", neoOrbLifecycleOperationExecDetached)
				return err
			},
		},
		"stale finish operation": {
			prepare: func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) neoOrbLifecycleGeneration {
				launchingStart, err := store.beginActivation(active, "operation-start", neoOrbLifecycleOperationStart)
				if err != nil {
					t.Fatalf("begin start activation: %v", err)
				}
				launchingExec, err := store.beginActivation(launchingStart, "operation-exec", neoOrbLifecycleOperationExecDetached)
				if err != nil {
					t.Fatalf("begin detached activation: %v", err)
				}
				launchingExec.OperationID = "operation-stale"
				return launchingExec
			},
			invoke: func(store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration) error {
				_, err := store.finishActivation(expected)
				return err
			},
		},
		"pending generation": {
			prepare: func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) neoOrbLifecycleGeneration {
				if _, err := store.reserveGeneration(active.ThreadID, "pending-token"); err != nil {
					t.Fatalf("reserve pending generation: %v", err)
				}
				return active
			},
			invoke: func(store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration) error {
				_, err := store.beginActivation(expected, "operation-start", neoOrbLifecycleOperationStart)
				return err
			},
		},
		"cleanup tombstone": {
			prepare: func(t *testing.T, store *neoOrbLifecycleStore, active neoOrbLifecycleGeneration) neoOrbLifecycleGeneration {
				if _, found, err := store.beginCleanup(active.ThreadID); err != nil || !found {
					t.Fatalf("begin cleanup: found=%v err=%v", found, err)
				}
				return active
			},
			invoke: func(store *neoOrbLifecycleStore, expected neoOrbLifecycleGeneration) error {
				_, err := store.beginActivation(expected, "operation-start", neoOrbLifecycleOperationStart)
				return err
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			active := activateNeoOrbBoundLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-cas", "owner-cas")
			expected := test.prepare(t, store, active)
			before := store.snapshot()
			if err := test.invoke(store, expected); err == nil {
				t.Fatal("stale or invalidated activation succeeded")
			}
			if after := store.snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected activation changed lifecycle state:\nafter=%#v\nbefore=%#v", after, before)
			}
		})
	}
}

func TestNeoOrbLifecycleActivationCASAbortRestoresOnlyExactPrior(t *testing.T) {
	tests := map[string]func(*testing.T, *neoOrbLifecycleStore, neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, neoOrbLifecycleActivation){
		"start to binding": func(t *testing.T, store *neoOrbLifecycleStore, binding neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, neoOrbLifecycleActivation) {
			launching, err := store.beginActivation(binding, "operation-start", neoOrbLifecycleOperationStart)
			if err != nil {
				t.Fatalf("begin start activation: %v", err)
			}
			return launching, neoOrbLifecycleActivation{State: binding.ActivationState}
		},
		"detached to start": func(t *testing.T, store *neoOrbLifecycleStore, binding neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, neoOrbLifecycleActivation) {
			launchingStart, err := store.beginActivation(binding, "operation-start", neoOrbLifecycleOperationStart)
			if err != nil {
				t.Fatalf("begin start activation: %v", err)
			}
			launchingExec, err := store.beginActivation(launchingStart, "operation-exec", neoOrbLifecycleOperationExecDetached)
			if err != nil {
				t.Fatalf("begin detached activation: %v", err)
			}
			return launchingExec, neoOrbLifecycleActivation{State: launchingStart.ActivationState, OperationID: launchingStart.OperationID, OperationKind: launchingStart.OperationKind}
		},
		"unpause to actionable": func(t *testing.T, store *neoOrbLifecycleStore, binding neoOrbLifecycleGeneration) (neoOrbLifecycleGeneration, neoOrbLifecycleActivation) {
			launchingStart, err := store.beginActivation(binding, "operation-start", neoOrbLifecycleOperationStart)
			if err != nil {
				t.Fatalf("begin start activation: %v", err)
			}
			launchingExec, err := store.beginActivation(launchingStart, "operation-exec", neoOrbLifecycleOperationExecDetached)
			if err != nil {
				t.Fatalf("begin detached activation: %v", err)
			}
			actionable, err := store.finishActivation(launchingExec)
			if err != nil {
				t.Fatalf("finish detached activation: %v", err)
			}
			launchingUnpause, err := store.beginActivation(actionable, "operation-unpause", neoOrbLifecycleOperationUnpause)
			if err != nil {
				t.Fatalf("begin unpause activation: %v", err)
			}
			return launchingUnpause, neoOrbLifecycleActivation{State: actionable.ActivationState}
		},
	}
	for name, prepare := range tests {
		t.Run(name, func(t *testing.T) {
			store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			binding := activateNeoOrbBoundLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-abort", "owner-abort")
			launching, prior := prepare(t, store, binding)
			restored, err := store.abortUninvokedActivation(launching, prior)
			if err != nil {
				t.Fatalf("abort activation: %v", err)
			}
			if restored.ActivationState != prior.State || restored.OperationID != prior.OperationID || restored.OperationKind != prior.OperationKind {
				t.Fatalf("restored activation = %#v, prior=%#v", restored, prior)
			}
			if active := store.snapshot().Threads[binding.ThreadID].Active; active == nil || *active != restored {
				t.Fatalf("stored restored activation = %#v", active)
			}
		})
	}

	for _, test := range []struct {
		name   string
		mutate func(*neoOrbLifecycleGeneration, *neoOrbLifecycleActivation)
	}{
		{name: "stale operation", mutate: func(expected *neoOrbLifecycleGeneration, _ *neoOrbLifecycleActivation) {
			expected.OperationID = "operation-stale"
		}},
		{name: "wrong prior", mutate: func(_ *neoOrbLifecycleGeneration, prior *neoOrbLifecycleActivation) {
			prior.State = neoOrbLifecycleActivationActionable
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			binding := activateNeoOrbBoundLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-stale-abort", "owner-abort")
			launching, err := store.beginActivation(binding, "operation-start", neoOrbLifecycleOperationStart)
			if err != nil {
				t.Fatalf("begin start activation: %v", err)
			}
			prior := neoOrbLifecycleActivation{State: binding.ActivationState}
			expected := launching
			test.mutate(&expected, &prior)
			before := store.snapshot()
			if _, err := store.abortUninvokedActivation(expected, prior); err == nil {
				t.Fatal("stale activation abort succeeded")
			}
			if after := store.snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("rejected abort changed lifecycle state:\nafter=%#v\nbefore=%#v", after, before)
			}
		})
	}
}

func TestNeoOrbLifecycleActivationWriteFailureRollbackAndDurability(t *testing.T) {
	assertDurabilityError := func(t *testing.T, err error) {
		t.Helper()
		var durabilityErr *neoOrbLifecycleDurabilityError
		if !errors.As(err, &durabilityErr) {
			t.Fatalf("activation error = %v, want durability uncertainty", err)
		}
	}
	prepareFinish := func(t *testing.T, store *neoOrbLifecycleStore) neoOrbLifecycleGeneration {
		t.Helper()
		binding := activateNeoOrbBoundLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-durable", "owner-durable")
		launchingStart, err := store.beginActivation(binding, "operation-start", neoOrbLifecycleOperationStart)
		if err != nil {
			t.Fatalf("begin start activation: %v", err)
		}
		launchingExec, err := store.beginActivation(launchingStart, "operation-exec", neoOrbLifecycleOperationExecDetached)
		if err != nil {
			t.Fatalf("begin detached activation: %v", err)
		}
		return launchingExec
	}

	for _, operation := range []string{"begin", "finish"} {
		t.Run(operation+" write failure rolls back", func(t *testing.T) {
			store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			var expected neoOrbLifecycleGeneration
			if operation == "begin" {
				expected = activateNeoOrbBoundLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-durable", "owner-durable")
			} else {
				expected = prepareFinish(t, store)
			}
			before := store.snapshot()
			originalWrite := store.writeAtomic
			store.writeAtomic = func(string, []byte, os.FileMode) error { return errors.New("injected activation write failure") }
			var err error
			if operation == "begin" {
				_, err = store.beginActivation(expected, "operation-start", neoOrbLifecycleOperationStart)
			} else {
				_, err = store.finishActivation(expected)
			}
			store.writeAtomic = originalWrite
			if err == nil {
				t.Fatal("activation with failed write succeeded")
			}
			if after := store.snapshot(); !reflect.DeepEqual(after, before) {
				t.Fatalf("unpublished activation was not rolled back:\nafter=%#v\nbefore=%#v", after, before)
			}
		})

		t.Run(operation+" directory sync failure keeps published state", func(t *testing.T) {
			store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
			var expected neoOrbLifecycleGeneration
			if operation == "begin" {
				expected = activateNeoOrbBoundLifecycleTestGeneration(t, store, "T-019fdec9-b0cf-745d-8da4-f250184e870e", "container-durable", "owner-durable")
			} else {
				expected = prepareFinish(t, store)
			}
			originalSync := store.syncDirectory
			store.syncDirectory = func(string) error { return errors.New("injected activation directory sync failure") }
			var published neoOrbLifecycleGeneration
			var err error
			if operation == "begin" {
				published, err = store.beginActivation(expected, "operation-start", neoOrbLifecycleOperationStart)
			} else {
				published, err = store.finishActivation(expected)
			}
			assertDurabilityError(t, err)
			active := store.snapshot().Threads[expected.ThreadID].Active
			if active == nil || *active != published {
				t.Fatalf("published activation = %#v, result=%#v", active, published)
			}
			if operation == "begin" && (published.ActivationState != neoOrbLifecycleActivationLaunching || published.OperationKind != neoOrbLifecycleOperationStart) {
				t.Fatalf("published begin activation = %#v", published)
			}
			if operation == "finish" && !neoOrbLifecycleActionable(&published) {
				t.Fatalf("published finish activation = %#v", published)
			}
			store.syncDirectory = originalSync
			if err := store.Close(); err != nil {
				t.Fatalf("close lifecycle store after durability recovery: %v", err)
			}
			reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
			if err != nil {
				t.Fatalf("reload published activation: %v", err)
			}
			defer closeNeoOrbLifecycleTestStore(t, reloaded)
			if active := reloaded.snapshot().Threads[expected.ThreadID].Active; active == nil || *active != published {
				t.Fatalf("reloaded published activation = %#v, want %#v", active, published)
			}
		})
	}
}

func TestNeoOrbLifecycleStoreRejectsProviderAndModeMismatch(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, "unix:///var/run/docker.sock")
	raw, err := os.ReadFile(store.path)
	if err != nil {
		t.Fatalf("read lifecycle store: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before mismatch checks: %v", err)
	}
	if _, err := newNeoOrbLifecycleStore(threadDir, "unix:///private/var/run/docker.sock"); err == nil {
		t.Fatal("provider mismatch loaded")
	}
	after, err := os.ReadFile(store.path)
	if err != nil || !bytes.Equal(after, raw) {
		t.Fatalf("provider mismatch overwrote store: equal=%v, err=%v", bytes.Equal(after, raw), err)
	}
	if err := os.Chmod(store.path, 0o644); err != nil {
		t.Fatalf("chmod lifecycle store: %v", err)
	}
	if _, err := newNeoOrbLifecycleStore(threadDir, "unix:///var/run/docker.sock"); err == nil {
		t.Fatal("public lifecycle file loaded")
	}
	if err := os.Chmod(store.path, 0o600); err != nil {
		t.Fatalf("restore lifecycle store mode: %v", err)
	}
	if err := os.Chmod(filepath.Dir(store.path), 0o755); err != nil {
		t.Fatalf("chmod lifecycle directory: %v", err)
	}
	if _, err := newNeoOrbLifecycleStore(threadDir, "unix:///var/run/docker.sock"); err == nil {
		t.Fatal("public lifecycle directory loaded")
	}
}

func TestNeoOrbLifecycleCleanupTombstonePersistsUntilExactCompletion(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	first, err := store.reserveGeneration(threadID, "portal-token-1")
	if err != nil {
		t.Fatalf("reserve first generation: %v", err)
	}
	first.ContainerID = "container-1"
	if err := store.promoteGeneration(first); err != nil {
		t.Fatalf("promote first generation: %v", err)
	}
	second, err := store.reserveGeneration(threadID, "portal-token-2")
	if err != nil {
		t.Fatalf("reserve second generation: %v", err)
	}
	second.ContainerID = "container-2"
	if err := store.promoteGeneration(second); err != nil {
		t.Fatalf("promote second generation: %v", err)
	}
	pending, err := store.reserveGeneration(threadID, "portal-token-3")
	if err != nil {
		t.Fatalf("reserve pending generation: %v", err)
	}
	tombstone, found, err := store.beginCleanup(threadID)
	if err != nil || !found {
		t.Fatalf("beginCleanup = %#v, %v, %v", tombstone, found, err)
	}
	if tombstone.Active == nil || tombstone.Active.Generation != 2 || tombstone.Pending == nil || !reflect.DeepEqual(*tombstone.Pending, pending) || len(tombstone.Retained) != 1 || tombstone.Retained[0].Generation != 1 {
		t.Fatalf("cleanup tombstone = %#v", tombstone)
	}
	state := store.snapshot()
	if _, exists := state.Threads[threadID]; exists {
		t.Fatal("thread record remained after cleanup began")
	}
	if _, exists := state.CleanupTombstones[threadID]; !exists {
		t.Fatal("cleanup tombstone was not stored")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store before reload: %v", err)
	}
	reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reload cleanup tombstone: %v", err)
	}
	wrong := tombstone
	wrong.Active = cloneNeoOrbLifecycleGeneration(tombstone.Active)
	wrong.Active.ContainerName += "-wrong"
	if err := reloaded.completeCleanup(wrong); err == nil {
		t.Fatal("completed mismatched cleanup tombstone")
	}
	if _, exists := reloaded.snapshot().CleanupTombstones[threadID]; !exists {
		t.Fatal("mismatched completion cleared cleanup tombstone")
	}
	if err := reloaded.completeCleanup(tombstone); err != nil {
		t.Fatalf("complete exact cleanup tombstone: %v", err)
	}
	if err := reloaded.Close(); err != nil {
		t.Fatalf("close lifecycle store after cleanup: %v", err)
	}
	finalStore, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reload completed cleanup: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, finalStore)
	if len(finalStore.snapshot().CleanupTombstones) != 0 {
		t.Fatalf("cleanup tombstone remained: %#v", finalStore.snapshot().CleanupTombstones)
	}
}

func TestNeoOrbLifecycleStoreUsesEffectiveDockerEndpointIdentity(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://DOCKER-A.example:2375/")
	store, threadDir := newNeoOrbLifecycleTestStore(t, "")
	if provider := store.snapshot().Provider; provider != "http://docker-a.example:2375" {
		t.Fatalf("provider = %q", provider)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close lifecycle store: %v", err)
	}
	t.Setenv("DOCKER_HOST", "tcp://docker-b.example:2375")
	if _, err := newNeoOrbLifecycleStore(threadDir, ""); err == nil || !strings.Contains(err.Error(), "provider mismatch") {
		t.Fatalf("changed effective provider error = %v", err)
	}
}

func TestNeoOrbLifecycleStoreRejectsSecondWriterUntilClose(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	if _, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider); err == nil || !strings.Contains(err.Error(), "locked by another process") {
		t.Fatalf("second writer error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close first writer: %v", err)
	}
	reopened, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("open writer after close: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reopened)
}

func TestNeoOrbLifecycleStoreRejectsHardLinkedStateAndLockFiles(t *testing.T) {
	t.Run("state", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		if err := store.Close(); err != nil {
			t.Fatalf("close lifecycle store: %v", err)
		}
		alias := filepath.Join(filepath.Dir(store.path), "state-alias")
		if err := os.Link(store.path, alias); err != nil {
			t.Fatalf("hard link lifecycle state: %v", err)
		}
		if _, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider); err == nil || !strings.Contains(err.Error(), "private regular file") {
			t.Fatalf("hard-linked state error = %v", err)
		}
		if err := os.Remove(alias); err != nil {
			t.Fatalf("remove lifecycle state alias: %v", err)
		}
		reopened, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reopen lifecycle store: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reopened)
	})

	t.Run("lock", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		if err := store.Close(); err != nil {
			t.Fatalf("close lifecycle store: %v", err)
		}
		lockPath := filepath.Join(filepath.Dir(store.path), neoOrbLifecycleLockFileName)
		alias := filepath.Join(filepath.Dir(store.path), "lock-alias")
		if err := os.Link(lockPath, alias); err != nil {
			t.Fatalf("hard link lifecycle lock: %v", err)
		}
		if _, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider); err == nil || !strings.Contains(err.Error(), "private regular file") {
			t.Fatalf("hard-linked lock error = %v", err)
		}
		if err := os.Remove(alias); err != nil {
			t.Fatalf("remove lifecycle lock alias: %v", err)
		}
		reopened, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reopen lifecycle store: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reopened)
	})
}

func TestNeoOrbLifecycleStoreInitializationRetriesPublishedDirectorySync(t *testing.T) {
	parent := t.TempDir()
	threadDir := filepath.Join(parent, "threads")
	syncCalls := 0
	store, err := newNeoOrbLifecycleStoreWithIO(threadDir, neoOrbLifecycleTestProvider, writeNeoDurableAtomicFile, func(path string) error {
		syncCalls++
		if syncCalls == 1 {
			return errors.New("injected directory sync failure")
		}
		return syncNeoDurableDirectory(path)
	})
	if err != nil {
		t.Fatalf("initialize after resolved durability uncertainty: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, store)
	if syncCalls != 2 || store.durabilityPending {
		t.Fatalf("initialization sync calls=%d pending=%v", syncCalls, store.durabilityPending)
	}
}

func TestNeoOrbLifecycleStoreInitializationRetriesNewParentDirectorySync(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "data")
	threadDir := filepath.Join(parent, "threads")
	parentSyncCalls := 0
	store, err := newNeoOrbLifecycleStoreWithIO(threadDir, neoOrbLifecycleTestProvider, writeNeoDurableAtomicFile, func(path string) error {
		if path == base {
			parentSyncCalls++
			if parentSyncCalls == 1 {
				return errors.New("injected parent directory sync failure")
			}
		}
		return syncNeoDurableDirectory(path)
	})
	if err != nil {
		t.Fatalf("initialize after resolved parent durability uncertainty: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, store)
	if parentSyncCalls != 2 || store.durabilityPending || store.durabilityPath != "" {
		t.Fatalf("parent sync calls=%d pending=%v path=%q", parentSyncCalls, store.durabilityPending, store.durabilityPath)
	}
}

func TestNeoOrbLifecycleStoreInitializationFailureReleasesWriterLock(t *testing.T) {
	parent := t.TempDir()
	threadDir := filepath.Join(parent, "threads")
	store, err := newNeoOrbLifecycleStoreWithIO(threadDir, neoOrbLifecycleTestProvider, writeNeoDurableAtomicFile, func(string) error {
		return errors.New("injected directory sync failure")
	})
	if store != nil {
		t.Fatal("failed lifecycle constructor returned a store")
	}
	var durabilityErr *neoOrbLifecycleDurabilityError
	if !errors.As(err, &durabilityErr) {
		t.Fatalf("constructor error = %v, want lifecycle durability error", err)
	}
	reopened, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reopen published lifecycle store after constructor failure: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reopened)
}

func TestNeoOrbLifecycleStoreCloseReportsPendingDurabilityAndReleasesLock(t *testing.T) {
	store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	store.syncDirectory = func(string) error {
		return errors.New("injected directory sync failure")
	}
	generation, err := store.reserveGeneration("T-019fdec9-b0cf-745d-8da4-f250184e870e", "portal-token")
	var durabilityErr *neoOrbLifecycleDurabilityError
	if generation.Generation != 1 || !errors.As(err, &durabilityErr) {
		t.Fatalf("reserve generation = %#v, %v", generation, err)
	}
	if err := store.Close(); !errors.As(err, &durabilityErr) {
		t.Fatalf("close error = %v, want lifecycle durability error", err)
	}
	if _, err := store.reserveGeneration("T-019fdec9-b0cf-745d-8da4-f250184e870e", "another-token"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("mutation after close error = %v", err)
	}
	reopened, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
	if err != nil {
		t.Fatalf("reopen lifecycle store after uncertain close: %v", err)
	}
	defer closeNeoOrbLifecycleTestStore(t, reopened)
	if pending := reopened.snapshot().Threads[generation.ThreadID].Pending; pending == nil || pending.Generation != generation.Generation {
		t.Fatalf("reopened pending generation = %#v", pending)
	}
}

func TestNeoOrbLifecycleCleanupTombstonesAreDeepCloned(t *testing.T) {
	store, _ := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	first, err := store.reserveGeneration(threadID, "portal-token-1")
	if err != nil {
		t.Fatalf("reserve first generation: %v", err)
	}
	first.ContainerID = "container-1"
	if err := store.promoteGeneration(first); err != nil {
		t.Fatalf("promote first generation: %v", err)
	}
	second, err := store.reserveGeneration(threadID, "portal-token-2")
	if err != nil {
		t.Fatalf("reserve second generation: %v", err)
	}
	second.ContainerID = "container-2"
	if err := store.promoteGeneration(second); err != nil {
		t.Fatalf("promote second generation: %v", err)
	}
	if _, err := store.reserveGeneration(threadID, "portal-token-3"); err != nil {
		t.Fatalf("reserve pending generation: %v", err)
	}
	tombstone, found, err := store.beginCleanup(threadID)
	if err != nil || !found {
		t.Fatalf("begin cleanup = %#v, %v, %v", tombstone, found, err)
	}
	tombstone.Active.ContainerName = "mutated-active"
	tombstone.Pending.ContainerName = "mutated-pending"
	tombstone.Retained[0].ContainerName = "mutated-retained"
	stored := store.snapshot().CleanupTombstones[threadID]
	if stored.Active.ContainerName == "mutated-active" || stored.Pending.ContainerName == "mutated-pending" || stored.Retained[0].ContainerName == "mutated-retained" {
		t.Fatalf("new tombstone shared state: %#v", stored)
	}
	existing, found, err := store.beginCleanup(threadID)
	if err != nil || !found {
		t.Fatalf("read existing cleanup = %#v, %v, %v", existing, found, err)
	}
	existing.Active.ContainerName = "mutated-existing-active"
	existing.Pending.ContainerName = "mutated-existing-pending"
	existing.Retained[0].ContainerName = "mutated-existing-retained"
	stored = store.snapshot().CleanupTombstones[threadID]
	if stored.Active.ContainerName == "mutated-existing-active" || stored.Pending.ContainerName == "mutated-existing-pending" || stored.Retained[0].ContainerName == "mutated-existing-retained" {
		t.Fatalf("existing tombstone shared state: %#v", stored)
	}
}

func TestNeoOrbLifecycleStoreKeepsPublishedStateAfterDirectorySyncFailure(t *testing.T) {
	threadID := "T-019fdec9-b0cf-745d-8da4-f250184e870e"
	assertDurabilityError := func(t *testing.T, err error) {
		t.Helper()
		var durabilityErr *neoOrbLifecycleDurabilityError
		if !errors.As(err, &durabilityErr) {
			t.Fatalf("error = %v, want lifecycle durability error", err)
		}
	}
	setFailingSync := func(store *neoOrbLifecycleStore) *bool {
		failing := true
		store.syncDirectory = func(path string) error {
			if failing {
				return errors.New("injected directory sync failure")
			}
			return syncNeoDurableDirectory(path)
		}
		return &failing
	}

	t.Run("reserve", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		failing := setFailingSync(store)
		generation, err := store.reserveGeneration(threadID, "portal-token-1")
		assertDurabilityError(t, err)
		if generation.Generation != 1 || store.snapshot().Threads[threadID].Pending == nil || store.snapshot().GenerationHighWater[threadID] != 1 {
			t.Fatalf("published reservation = %#v, state=%#v", generation, store.snapshot())
		}
		if err := store.clearPendingGeneration(generation); err == nil {
			t.Fatal("mutation was not fenced while durability remained uncertain")
		} else {
			assertDurabilityError(t, err)
		}
		*failing = false
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reload published reservation: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reloaded)
		if pending := reloaded.snapshot().Threads[threadID].Pending; pending == nil || pending.Generation != 1 {
			t.Fatalf("reloaded pending generation = %#v", pending)
		}
	})

	t.Run("promote", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		generation, err := store.reserveGeneration(threadID, "portal-token-1")
		if err != nil {
			t.Fatalf("reserve generation: %v", err)
		}
		generation.ContainerID = "container-1"
		failing := setFailingSync(store)
		assertDurabilityError(t, store.promoteGeneration(generation))
		if active := store.snapshot().Threads[threadID].Active; active == nil || active.Generation != 1 {
			t.Fatalf("published active generation = %#v", active)
		}
		if _, err := store.reserveGeneration(threadID, "portal-token-2"); err == nil {
			t.Fatal("mutation was not fenced while durability remained uncertain")
		} else {
			assertDurabilityError(t, err)
		}
		*failing = false
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reload published promotion: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reloaded)
		if active := reloaded.snapshot().Threads[threadID].Active; active == nil || active.Generation != 1 {
			t.Fatalf("reloaded active generation = %#v", active)
		}
	})

	t.Run("clear pending", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		generation, err := store.reserveGeneration(threadID, "portal-token-1")
		if err != nil {
			t.Fatalf("reserve generation: %v", err)
		}
		failing := setFailingSync(store)
		assertDurabilityError(t, store.clearPendingGeneration(generation))
		state := store.snapshot()
		if _, exists := state.Threads[threadID]; exists || state.GenerationHighWater[threadID] != 1 {
			t.Fatalf("published clear state = %#v", state)
		}
		if _, err := store.reserveGeneration(threadID, "portal-token-2"); err == nil {
			t.Fatal("mutation was not fenced while durability remained uncertain")
		} else {
			assertDurabilityError(t, err)
		}
		*failing = false
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reload published clear: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reloaded)
		next, err := reloaded.reserveGeneration(threadID, "portal-token-2")
		if err != nil || next.Generation != 2 {
			t.Fatalf("reserve after reloaded clear = %#v, %v", next, err)
		}
	})

	t.Run("begin cleanup", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		generation, err := store.reserveGeneration(threadID, "portal-token-1")
		if err != nil {
			t.Fatalf("reserve generation: %v", err)
		}
		generation.ContainerID = "container-1"
		if err := store.promoteGeneration(generation); err != nil {
			t.Fatalf("promote generation: %v", err)
		}
		failing := setFailingSync(store)
		tombstone, found, err := store.beginCleanup(threadID)
		assertDurabilityError(t, err)
		if !found || tombstone.Active == nil || store.snapshot().CleanupTombstones[threadID].Active == nil {
			t.Fatalf("published cleanup = %#v, %v, state=%#v", tombstone, found, store.snapshot())
		}
		if _, _, err := store.beginCleanup(threadID); err == nil {
			t.Fatal("mutation was not fenced while durability remained uncertain")
		} else {
			assertDurabilityError(t, err)
		}
		*failing = false
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reload published cleanup: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reloaded)
		if active := reloaded.snapshot().CleanupTombstones[threadID].Active; active == nil || active.Generation != 1 {
			t.Fatalf("reloaded cleanup generation = %#v", active)
		}
	})

	t.Run("complete cleanup", func(t *testing.T) {
		store, threadDir := newNeoOrbLifecycleTestStore(t, neoOrbLifecycleTestProvider)
		generation, err := store.reserveGeneration(threadID, "portal-token-1")
		if err != nil {
			t.Fatalf("reserve generation: %v", err)
		}
		generation.ContainerID = "container-1"
		if err := store.promoteGeneration(generation); err != nil {
			t.Fatalf("promote generation: %v", err)
		}
		tombstone, found, err := store.beginCleanup(threadID)
		if err != nil || !found {
			t.Fatalf("begin cleanup = %#v, %v, %v", tombstone, found, err)
		}
		failing := setFailingSync(store)
		assertDurabilityError(t, store.completeCleanup(tombstone))
		if _, exists := store.snapshot().CleanupTombstones[threadID]; exists {
			t.Fatalf("published completion retained tombstone: %#v", store.snapshot())
		}
		if _, err := store.reserveGeneration(threadID, "portal-token-2"); err == nil {
			t.Fatal("mutation was not fenced while durability remained uncertain")
		} else {
			assertDurabilityError(t, err)
		}
		*failing = false
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		reloaded, err := newNeoOrbLifecycleStore(threadDir, neoOrbLifecycleTestProvider)
		if err != nil {
			t.Fatalf("reload published completion: %v", err)
		}
		defer closeNeoOrbLifecycleTestStore(t, reloaded)
		next, err := reloaded.reserveGeneration(threadID, "portal-token-2")
		if err != nil || next.Generation != 2 {
			t.Fatalf("reserve after cleanup completion = %#v, %v", next, err)
		}
	})
}
