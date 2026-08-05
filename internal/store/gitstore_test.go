package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/transport"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type gitStoreTestCommander struct {
	loader transport.Loader
}

func (c *gitStoreTestCommander) Command(ctx context.Context, command string, endpoint *transport.Endpoint, _ transport.AuthMethod, params ...string) (transport.Command, error) {
	service := transport.Service(command)
	if service != transport.UploadPackService && service != transport.ReceivePackService {
		return nil, transport.ErrUnsupportedService
	}
	return &gitStoreTestCommand{
		ctx:         ctx,
		loader:      c.loader,
		endpoint:    endpoint,
		service:     service,
		gitProtocol: strings.Join(params, ":"),
		done:        make(chan error, 1),
	}, nil
}

type gitStoreTestCommand struct {
	ctx          context.Context
	loader       transport.Loader
	endpoint     *transport.Endpoint
	service      transport.Service
	gitProtocol  string
	stdin        *io.PipeReader
	stdinWriter  *io.PipeWriter
	stdout       *io.PipeWriter
	stdoutReader *io.PipeReader
	done         chan error
	closeOnce    sync.Once
}

func (*gitStoreTestCommand) StderrPipe() (io.Reader, error) {
	return nil, nil
}

func (c *gitStoreTestCommand) StdinPipe() (io.WriteCloser, error) {
	c.stdin, c.stdinWriter = io.Pipe()
	return c.stdinWriter, nil
}

func (c *gitStoreTestCommand) StdoutPipe() (io.Reader, error) {
	c.stdoutReader, c.stdout = io.Pipe()
	return c.stdoutReader, nil
}

func (c *gitStoreTestCommand) Start() error {
	storage, err := c.loader.Load(c.endpoint)
	if err != nil {
		c.done <- err
		close(c.done)
		return err
	}
	go func() {
		var errServe error
		switch c.service {
		case transport.UploadPackService:
			errServe = transport.UploadPack(c.ctx, storage, io.NopCloser(c.stdin), c.stdout, &transport.UploadPackOptions{GitProtocol: c.gitProtocol})
		case transport.ReceivePackService:
			errServe = transport.ReceivePack(c.ctx, storage, io.NopCloser(c.stdin), c.stdout, &transport.ReceivePackOptions{GitProtocol: c.gitProtocol})
		default:
			errServe = fmt.Errorf("unsupported service: %s", c.service)
		}
		if errServe != nil {
			_ = c.stdout.CloseWithError(errServe)
		}
		c.done <- errServe
		close(c.done)
	}()
	return nil
}

func (c *gitStoreTestCommand) Close() error {
	c.closeOnce.Do(func() {
		if c.stdinWriter != nil {
			_ = c.stdinWriter.Close()
		}
		<-c.done
		if c.stdout != nil {
			_ = c.stdout.Close()
		}
		if c.stdoutReader != nil {
			_ = c.stdoutReader.Close()
		}
		if c.stdin != nil {
			_ = c.stdin.Close()
		}
	})
	return nil
}

func init() {
	transport.Register("file", transport.NewPackTransport(&gitStoreTestCommander{loader: transport.DefaultLoader}))
}

type testBranchSpec struct {
	name     string
	contents string
}

func TestGitTokenStoreSavePersistsBeforeLocalPublication(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	path := filepath.Join(root, "workspace", "auths", "credential.json")
	var durableAtPublish []byte
	store.renameFile = func(oldPath, newPath string) error {
		durableAtPublish = readRemoteBranchFile(t, remoteDir, "trunk", "auths/credential.json")
		if _, err := os.Stat(newPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("visible auth exists before publication: %v", err)
		}
		return os.Rename(oldPath, newPath)
	}

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertJSONField(t, durableAtPublish, "value", "candidate")
	published, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read published auth: %v", errRead)
	}
	assertJSONField(t, published, "value", "candidate")
	assertNoStoreTempFile(t, path)

	remoteRepo, errOpen := git.PlainOpen(remoteDir)
	if errOpen != nil {
		t.Fatalf("open remote repo: %v", errOpen)
	}
	ref, errRef := remoteRepo.Reference(plumbing.NewBranchReferenceName("trunk"), false)
	if errRef != nil {
		t.Fatalf("read remote branch: %v", errRef)
	}
	commit, errCommit := remoteRepo.CommitObject(ref.Hash())
	if errCommit != nil {
		t.Fatalf("read remote commit: %v", errCommit)
	}
	if commit.NumParents() != 0 {
		t.Fatalf("remote commit parents = %d, want 0", commit.NumParents())
	}
}

func TestGitTokenStoreSavePushFailureDoesNotPublishLocalFile(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	path := filepath.Join(root, "workspace", "auths", "credential.json")
	if _, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "previous"},
	}); err != nil {
		t.Fatalf("save previous auth: %v", err)
	}
	previous, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read previous auth: %v", errRead)
	}
	repo, errOpen := git.PlainOpen(filepath.Join(root, "workspace"))
	if errOpen != nil {
		t.Fatalf("open workspace repo: %v", errOpen)
	}
	head, errHead := repo.Head()
	if errHead != nil {
		t.Fatalf("read workspace head: %v", errHead)
	}
	store.pushRepo = func(context.Context, *git.Repository, *git.PushOptions) error {
		return errors.New("push rejected")
	}

	_, errSave := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if errSave == nil {
		t.Fatal("Save succeeded, want push error")
	}
	assertLocalStoreBytes(t, path, previous)
	assertJSONField(t, readRemoteBranchFile(t, remoteDir, "trunk", "auths/credential.json"), "value", "previous")
	assertGitStoreState(t, repo, head.Hash())
	assertNoStoreTempFile(t, path)

	store.pushRepo = nil
	if _, errRetry := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	}); errRetry != nil {
		t.Fatalf("retry Save: %v", errRetry)
	}
	assertJSONField(t, readRemoteBranchFile(t, remoteDir, "trunk", "auths/credential.json"), "value", "candidate")
}

func TestGitTokenStoreSavePublishFailureRollsBackRemote(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	path := filepath.Join(root, "workspace", "auths", "credential.json")
	if _, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "previous"},
	}); err != nil {
		t.Fatalf("save previous auth: %v", err)
	}
	previous, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read previous auth: %v", errRead)
	}
	repo, errOpen := git.PlainOpen(filepath.Join(root, "workspace"))
	if errOpen != nil {
		t.Fatalf("open workspace repo: %v", errOpen)
	}
	head, errHead := repo.Head()
	if errHead != nil {
		t.Fatalf("read workspace head: %v", errHead)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, errSave := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if errSave == nil {
		t.Fatal("Save succeeded, want publish error")
	}
	assertLocalStoreBytes(t, path, previous)
	assertJSONField(t, readRemoteBranchFile(t, remoteDir, "trunk", "auths/credential.json"), "value", "previous")
	assertGitStoreState(t, repo, head.Hash())
	assertNoStoreTempFile(t, path)
}

func TestGitTokenStoreSaveRollbackLeaseConflictPreservesNewerRemoteHead(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	path := filepath.Join(root, "workspace", "auths", "credential.json")
	if _, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "previous"},
	}); err != nil {
		t.Fatalf("save previous auth: %v", err)
	}
	previous, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read previous auth: %v", errRead)
	}
	repo, errOpen := git.PlainOpen(filepath.Join(root, "workspace"))
	if errOpen != nil {
		t.Fatalf("open workspace repo: %v", errOpen)
	}
	head, errHead := repo.Head()
	if errHead != nil {
		t.Fatalf("read workspace head: %v", errHead)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }
	pushes := 0
	var newerHead plumbing.Hash
	store.pushRepo = func(ctx context.Context, repo *git.Repository, opts *git.PushOptions) error {
		pushes++
		if pushes == 1 {
			return repo.PushContext(ctx, opts)
		}
		newerHead = advanceRemoteHeadFromCurrentTree(t, remoteDir, "trunk")
		return repo.PushContext(ctx, opts)
	}

	_, errSave := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if errSave == nil || !strings.Contains(errSave.Error(), "remote rollback failed") {
		t.Fatalf("Save error = %v, want lease conflict", errSave)
	}
	remoteRepo, errRemote := git.PlainOpen(remoteDir)
	if errRemote != nil {
		t.Fatalf("open remote repo: %v", errRemote)
	}
	remoteHead, errRemoteHead := remoteRepo.Reference(plumbing.NewBranchReferenceName("trunk"), false)
	if errRemoteHead != nil {
		t.Fatalf("read remote head: %v", errRemoteHead)
	}
	if remoteHead.Hash() != newerHead {
		t.Fatalf("remote head = %s, want newer head %s", remoteHead.Hash(), newerHead)
	}
	assertLocalStoreBytes(t, path, previous)
	assertGitStoreState(t, repo, head.Hash())
	assertNoStoreTempFile(t, path)
}

func TestGitTokenStoreSaveUsesTemporarySecurePathForTokenStorage(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	storage := &storeTestTokenStorage{data: []byte(`{"type":"gemini","token":"value"}`), mode: 0o644}
	store.renameFile = func(oldPath, newPath string) error {
		assertStoreFileMode(t, oldPath, 0o600)
		return os.Rename(oldPath, newPath)
	}
	auth := &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"type": "gemini"},
		Storage:  storage,
	}
	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if storage.path != path+".tmp" {
		t.Fatalf("storage path = %q, want %q", storage.path, path+".tmp")
	}
	assertLocalStoreBytes(t, path, storage.data)
	assertStoreFileMode(t, path, 0o600)
	assertNoStoreTempFile(t, path)
}

func TestGitTokenStoreSavePreservesEmptyStorageNoOp(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	auth := &cliproxyauth.Auth{ID: "credential.json", Storage: &storeTestEmptyStorage{}}
	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("stat visible auth error = %v, want not exist", errStat)
	}
	assertSavedAuthNormalized(t, auth, path)
	assertNoStoreTempFile(t, path)
}

func TestGitTokenStoreSaveSameJSONNormalizesAuth(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)
	store := NewGitTokenStore(remoteDir, "", "", "trunk")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if _, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "same"},
	}); err != nil {
		t.Fatalf("save initial auth: %v", err)
	}
	auth := &cliproxyauth.Auth{ID: "credential.json", Metadata: map[string]any{"value": "same"}}
	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	assertSavedAuthNormalized(t, auth, path)
	assertNoStoreTempFile(t, path)
}

func TestEnsureRepositoryUsesRemoteDefaultBranchWhenBranchNotConfigured(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
		testBranchSpec{name: "release/2026", contents: "release branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "trunk", "remote default branch\n")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "trunk", "remote default branch updated\n", "advance trunk")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "release/2026", "release branch updated\n", "advance release")

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository second call: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "trunk", "remote default branch updated\n")
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryUsesConfiguredBranchWhenExplicitlySet(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
		testBranchSpec{name: "release/2026", contents: "release branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "release/2026")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "release/2026", "release branch\n")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "trunk", "remote default branch updated\n", "advance trunk")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "release/2026", "release branch updated\n", "advance release")

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository second call: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "release/2026", "release branch updated\n")
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryReturnsErrorForMissingConfiguredBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "missing-branch")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	err := store.EnsureRepository()
	if err == nil {
		t.Fatal("EnsureRepository succeeded, want error for nonexistent configured branch")
	}
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryReturnsErrorForMissingConfiguredBranchOnExistingRepositoryPull(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}

	reopened := NewGitTokenStore(remoteDir, "", "", "missing-branch")
	reopened.SetBaseDir(baseDir)

	err := reopened.EnsureRepository()
	if err == nil {
		t.Fatal("EnsureRepository succeeded on reopen, want error for nonexistent configured branch")
	}
	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), "trunk")
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryInitializesEmptyRemoteUsingConfiguredBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := filepath.Join(root, "remote.git")
	if _, err := git.PlainInit(remoteDir, true); err != nil {
		t.Fatalf("init bare remote: %v", err)
	}

	branch := "feature/gemini-fix"
	store := NewGitTokenStore(remoteDir, "", "", branch)
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), branch)
	assertRemoteBranchExistsWithCommit(t, remoteDir, branch)
	assertRemoteBranchDoesNotExist(t, remoteDir, "master")
}

func TestEnsureRepositoryExistingRepoSwitchesToConfiguredBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "develop", contents: "remote develop branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "master", "remote master branch\n")

	reopened := NewGitTokenStore(remoteDir, "", "", "develop")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository reopen: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "develop", "remote develop branch\n")

	workspaceDir := filepath.Join(root, "workspace")
	if err := os.WriteFile(filepath.Join(workspaceDir, "branch.txt"), []byte("local develop update\n"), 0o600); err != nil {
		t.Fatalf("write local branch marker: %v", err)
	}

	reopened.mu.Lock()
	err := reopened.commitAndPushLocked("Update develop branch marker", "branch.txt")
	reopened.mu.Unlock()
	if err != nil {
		t.Fatalf("commitAndPushLocked: %v", err)
	}

	assertRepositoryHeadBranch(t, workspaceDir, "develop")
	assertRemoteBranchContents(t, remoteDir, "develop", "local develop update\n")
	assertRemoteBranchContents(t, remoteDir, "master", "remote master branch\n")
}

func TestEnsureRepositoryExistingRepoSwitchesToConfiguredBranchCreatedAfterClone(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "master", "remote master branch\n")

	advanceRemoteBranchFromNewBranch(t, filepath.Join(root, "seed"), remoteDir, "release/2026", "release branch\n", "create release")

	reopened := NewGitTokenStore(remoteDir, "", "", "release/2026")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository reopen: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "release/2026", "release branch\n")
}

func TestEnsureRepositoryResetsToRemoteDefaultWhenBranchUnset(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "develop", contents: "remote develop branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	// First store pins to develop and prepares local workspace
	storePinned := NewGitTokenStore(remoteDir, "", "", "develop")
	storePinned.SetBaseDir(baseDir)
	if err := storePinned.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository pinned: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "develop", "remote develop branch\n")

	// Second store has branch unset and should reset local workspace to remote default (master)
	storeDefault := NewGitTokenStore(remoteDir, "", "", "")
	storeDefault.SetBaseDir(baseDir)
	if err := storeDefault.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository default: %v", err)
	}
	// Local HEAD should now follow remote default (master)
	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), "master")

	// Make a local change and push using the store with branch unset; push should update remote master
	workspaceDir := filepath.Join(root, "workspace")
	if err := os.WriteFile(filepath.Join(workspaceDir, "branch.txt"), []byte("local master update\n"), 0o600); err != nil {
		t.Fatalf("write local master marker: %v", err)
	}
	storeDefault.mu.Lock()
	if err := storeDefault.commitAndPushLocked("Update master marker", "branch.txt"); err != nil {
		storeDefault.mu.Unlock()
		t.Fatalf("commitAndPushLocked: %v", err)
	}
	storeDefault.mu.Unlock()

	assertRemoteBranchContents(t, remoteDir, "master", "local master update\n")
}

func TestEnsureRepositoryFollowsRenamedRemoteDefaultBranchWhenAvailable(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "main", contents: "remote main branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "master", "remote master branch\n")

	setRemoteHeadBranch(t, remoteDir, "main")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "main", "remote main branch updated\n", "advance main")

	reopened := NewGitTokenStore(remoteDir, "", "", "")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository after remote default rename: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "main", "remote main branch updated\n")
	assertRemoteHeadBranch(t, remoteDir, "main")
}

func TestEnsureRepositoryKeepsCurrentBranchWhenRemoteDefaultCannotBeResolved(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "develop", contents: "remote develop branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	pinned := NewGitTokenStore(remoteDir, "", "", "develop")
	pinned.SetBaseDir(baseDir)
	if err := pinned.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository pinned: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "develop", "remote develop branch\n")

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
		http.Error(w, "auth required", http.StatusUnauthorized)
	}))
	defer authServer.Close()

	repo, err := git.PlainOpen(filepath.Join(root, "workspace"))
	if err != nil {
		t.Fatalf("open workspace repo: %v", err)
	}
	cfg, err := repo.Config()
	if err != nil {
		t.Fatalf("read repo config: %v", err)
	}
	cfg.Remotes["origin"].URLs = []string{authServer.URL}
	if err := repo.SetConfig(cfg); err != nil {
		t.Fatalf("set repo config: %v", err)
	}

	reopened := NewGitTokenStore(remoteDir, "", "", "")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository default branch fallback: %v", err)
	}
	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), "develop")
}

func setupGitRemoteRepository(t *testing.T, root, defaultBranch string, branches ...testBranchSpec) string {
	t.Helper()

	remoteDir := filepath.Join(root, "remote.git")
	if _, err := git.PlainInit(remoteDir, true); err != nil {
		t.Fatalf("init bare remote: %v", err)
	}

	seedDir := filepath.Join(root, "seed")
	seedRepo, err := git.PlainInit(seedDir, false)
	if err != nil {
		t.Fatalf("init seed repo: %v", err)
	}
	if err := seedRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(defaultBranch))); err != nil {
		t.Fatalf("set seed HEAD: %v", err)
	}

	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}

	defaultSpec, ok := findBranchSpec(branches, defaultBranch)
	if !ok {
		t.Fatalf("missing default branch spec for %q", defaultBranch)
	}
	commitBranchMarker(t, seedDir, worktree, defaultSpec, "seed default branch")

	for _, branch := range branches {
		if branch.name == defaultBranch {
			continue
		}
		if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(defaultBranch)}); err != nil {
			t.Fatalf("checkout default branch %s: %v", defaultBranch, err)
		}
		if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch.name), Create: true}); err != nil {
			t.Fatalf("create branch %s: %v", branch.name, err)
		}
		commitBranchMarker(t, seedDir, worktree, branch, "seed branch "+branch.name)
	}

	if _, err := seedRepo.CreateRemote(&gitconfig.RemoteConfig{Name: "origin", URLs: []string{remoteDir}}); err != nil {
		t.Fatalf("create origin remote: %v", err)
	}
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []gitconfig.RefSpec{gitconfig.RefSpec("refs/heads/*:refs/heads/*")},
	}); err != nil {
		t.Fatalf("push seed branches: %v", err)
	}

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	if err := remoteRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(defaultBranch))); err != nil {
		t.Fatalf("set remote HEAD: %v", err)
	}

	return remoteDir
}

func commitBranchMarker(t *testing.T, seedDir string, worktree *git.Worktree, branch testBranchSpec, message string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(seedDir, "branch.txt"), []byte(branch.contents), 0o600); err != nil {
		t.Fatalf("write branch marker for %s: %v", branch.name, err)
	}
	if _, err := worktree.Add("branch.txt"); err != nil {
		t.Fatalf("add branch marker for %s: %v", branch.name, err)
	}
	if _, err := worktree.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "CLIProxyAPI",
			Email: "cliproxy@local",
			When:  time.Unix(1711929600, 0),
		},
	}); err != nil {
		t.Fatalf("commit branch marker for %s: %v", branch.name, err)
	}
}

func advanceRemoteBranch(t *testing.T, seedDir, remoteDir, branch, contents, message string) {
	t.Helper()

	seedRepo, err := git.PlainOpen(seedDir)
	if err != nil {
		t.Fatalf("open seed repo: %v", err)
	}
	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch)}); err != nil {
		t.Fatalf("checkout branch %s: %v", branch, err)
	}
	commitBranchMarker(t, seedDir, worktree, testBranchSpec{name: branch, contents: contents}, message)
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs: []gitconfig.RefSpec{
			gitconfig.RefSpec(plumbing.NewBranchReferenceName(branch).String() + ":" + plumbing.NewBranchReferenceName(branch).String()),
		},
	}); err != nil {
		t.Fatalf("push branch %s update to %s: %v", branch, remoteDir, err)
	}
}

func advanceRemoteHeadFromCurrentTree(t *testing.T, remoteDir, branch string) plumbing.Hash {
	t.Helper()
	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	branchName := plumbing.NewBranchReferenceName(branch)
	current, err := remoteRepo.Reference(branchName, false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	currentCommit, err := remoteRepo.CommitObject(current.Hash())
	if err != nil {
		t.Fatalf("read remote commit: %v", err)
	}
	signature := object.Signature{
		Name:  "Concurrent writer",
		Email: "concurrent@local",
		When:  time.Unix(1711929660, 0),
	}
	commit := &object.Commit{
		Author:       signature,
		Committer:    signature,
		Message:      "Concurrent update",
		TreeHash:     currentCommit.TreeHash,
		ParentHashes: []plumbing.Hash{current.Hash()},
	}
	encoded := &plumbing.MemoryObject{}
	encoded.SetType(plumbing.CommitObject)
	if err = commit.Encode(encoded); err != nil {
		t.Fatalf("encode concurrent commit: %v", err)
	}
	hash, err := remoteRepo.Storer.SetEncodedObject(encoded)
	if err != nil {
		t.Fatalf("store concurrent commit: %v", err)
	}
	if err = remoteRepo.Storer.SetReference(plumbing.NewHashReference(branchName, hash)); err != nil {
		t.Fatalf("advance remote branch %s: %v", branch, err)
	}
	return hash
}

func advanceRemoteBranchFromNewBranch(t *testing.T, seedDir, remoteDir, branch, contents, message string) {
	t.Helper()

	seedRepo, err := git.PlainOpen(seedDir)
	if err != nil {
		t.Fatalf("open seed repo: %v", err)
	}
	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName("master")}); err != nil {
		t.Fatalf("checkout master before creating %s: %v", branch, err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch), Create: true}); err != nil {
		t.Fatalf("create branch %s: %v", branch, err)
	}
	commitBranchMarker(t, seedDir, worktree, testBranchSpec{name: branch, contents: contents}, message)
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs: []gitconfig.RefSpec{
			gitconfig.RefSpec(plumbing.NewBranchReferenceName(branch).String() + ":" + plumbing.NewBranchReferenceName(branch).String()),
		},
	}); err != nil {
		t.Fatalf("push new branch %s update to %s: %v", branch, remoteDir, err)
	}
}

func findBranchSpec(branches []testBranchSpec, name string) (testBranchSpec, bool) {
	for _, branch := range branches {
		if branch.name == name {
			return branch, true
		}
	}
	return testBranchSpec{}, false
}

func assertRepositoryBranchAndContents(t *testing.T, repoDir, branch, wantContents string) {
	t.Helper()

	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("local repo head: %v", err)
	}
	if got, want := head.Name(), plumbing.NewBranchReferenceName(branch); got != want {
		t.Fatalf("local head branch = %s, want %s", got, want)
	}
	contents, err := os.ReadFile(filepath.Join(repoDir, "branch.txt"))
	if err != nil {
		t.Fatalf("read branch marker: %v", err)
	}
	if got := string(contents); got != wantContents {
		t.Fatalf("branch marker contents = %q, want %q", got, wantContents)
	}
}

func assertRepositoryHeadBranch(t *testing.T, repoDir, branch string) {
	t.Helper()

	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("local repo head: %v", err)
	}
	if got, want := head.Name(), plumbing.NewBranchReferenceName(branch); got != want {
		t.Fatalf("local head branch = %s, want %s", got, want)
	}
}

func assertRemoteHeadBranch(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	head, err := remoteRepo.Reference(plumbing.HEAD, false)
	if err != nil {
		t.Fatalf("read remote HEAD: %v", err)
	}
	if got, want := head.Target(), plumbing.NewBranchReferenceName(branch); got != want {
		t.Fatalf("remote HEAD target = %s, want %s", got, want)
	}
}

func setRemoteHeadBranch(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	if err := remoteRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(branch))); err != nil {
		t.Fatalf("set remote HEAD to %s: %v", branch, err)
	}
}

func assertRemoteBranchExistsWithCommit(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	if got := ref.Hash(); got == plumbing.ZeroHash {
		t.Fatalf("remote branch %s hash = %s, want non-zero hash", branch, got)
	}
}

func assertRemoteBranchDoesNotExist(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	if _, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false); err == nil {
		t.Fatalf("remote branch %s exists, want missing", branch)
	} else if err != plumbing.ErrReferenceNotFound {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
}

func assertRemoteBranchContents(t *testing.T, remoteDir, branch, wantContents string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	commit, err := remoteRepo.CommitObject(ref.Hash())
	if err != nil {
		t.Fatalf("read remote branch %s commit: %v", branch, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatalf("read remote branch %s tree: %v", branch, err)
	}
	file, err := tree.File("branch.txt")
	if err != nil {
		t.Fatalf("read remote branch %s file: %v", branch, err)
	}
	contents, err := file.Contents()
	if err != nil {
		t.Fatalf("read remote branch %s contents: %v", branch, err)
	}
	if contents != wantContents {
		t.Fatalf("remote branch %s contents = %q, want %q", branch, contents, wantContents)
	}
}

func readRemoteBranchFile(t *testing.T, remoteDir, branch, path string) []byte {
	t.Helper()
	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	commit, err := remoteRepo.CommitObject(ref.Hash())
	if err != nil {
		t.Fatalf("read remote branch %s commit: %v", branch, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatalf("read remote branch %s tree: %v", branch, err)
	}
	file, err := tree.File(path)
	if err != nil {
		t.Fatalf("read remote branch %s file %s: %v", branch, path, err)
	}
	contents, err := file.Contents()
	if err != nil {
		t.Fatalf("read remote branch %s file %s contents: %v", branch, path, err)
	}
	return []byte(contents)
}

func assertGitStoreState(t *testing.T, repo *git.Repository, wantHead plumbing.Hash) {
	t.Helper()
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("read workspace head: %v", err)
	}
	if head.Hash() != wantHead {
		t.Fatalf("workspace head = %s, want %s", head.Hash(), wantHead)
	}
	worktree, err := repo.Worktree()
	if err != nil {
		t.Fatalf("open workspace worktree: %v", err)
	}
	status, err := worktree.Status()
	if err != nil {
		t.Fatalf("workspace status: %v", err)
	}
	if !status.IsClean() {
		t.Fatalf("workspace status is dirty: %s", status.String())
	}
}
