package store

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

type postgresStoreTestCall struct {
	query string
	args  []driver.NamedValue
}

type postgresStoreTestBackend struct {
	mu         sync.Mutex
	calls      []postgresStoreTestCall
	failAt     int
	inspect    func(postgresStoreTestCall)
	content    []byte
	hasContent bool
}

func (b *postgresStoreTestBackend) call(query string, args []driver.NamedValue) error {
	call := postgresStoreTestCall{query: query, args: clonePostgresStoreArgs(args)}
	b.mu.Lock()
	b.calls = append(b.calls, call)
	callIndex := len(b.calls)
	inspect := b.inspect
	fail := b.failAt == callIndex
	b.mu.Unlock()
	if inspect != nil {
		inspect(call)
	}
	if fail {
		return errors.New("database rejected operation")
	}
	return nil
}

func (b *postgresStoreTestBackend) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	if err := b.call(query, args); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return postgresStoreTestExec(query, args, &b.content, &b.hasContent)
}

func (b *postgresStoreTestBackend) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := b.call(query, args); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return postgresStoreTestQuery(query, &b.content, &b.hasContent)
}

func postgresStoreTestExec(query string, args []driver.NamedValue, content *[]byte, hasContent *bool) (driver.Result, error) {
	switch {
	case strings.HasPrefix(strings.TrimSpace(query), "INSERT INTO"):
		if strings.Contains(query, "DO NOTHING") && *hasContent {
			return driver.RowsAffected(0), nil
		}
		*content = postgresStoreTestValue(args, 1)
		*hasContent = true
		return driver.RowsAffected(1), nil
	case strings.HasPrefix(strings.TrimSpace(query), "UPDATE"):
		if !*hasContent {
			return driver.RowsAffected(0), nil
		}
		if len(args) > 2 {
			candidate := postgresStoreTestValue(args, 2)
			if !bytes.Equal(*content, candidate) {
				return driver.RowsAffected(0), nil
			}
		}
		*content = postgresStoreTestValue(args, 1)
		return driver.RowsAffected(1), nil
	case strings.HasPrefix(strings.TrimSpace(query), "DELETE") && len(args) > 1:
		candidate := postgresStoreTestValue(args, 1)
		if !*hasContent || !bytes.Equal(*content, candidate) {
			return driver.RowsAffected(0), nil
		}
		*content = nil
		*hasContent = false
		return driver.RowsAffected(1), nil
	case strings.HasPrefix(strings.TrimSpace(query), "DELETE"):
		*content = nil
		*hasContent = false
		return driver.RowsAffected(1), nil
	default:
		return driver.RowsAffected(1), nil
	}
}

func postgresStoreTestQuery(query string, content *[]byte, hasContent *bool) (driver.Rows, error) {
	trimmed := strings.TrimSpace(query)
	rows := &postgresStoreTestRows{columns: []string{"content"}}
	switch {
	case strings.HasPrefix(trimmed, "SELECT") && strings.Contains(query, "FOR UPDATE"):
		if *hasContent {
			rows.values = [][]driver.Value{{string(*content)}}
		}
	case strings.HasPrefix(trimmed, "DELETE") && strings.Contains(query, "RETURNING content"):
		if *hasContent {
			rows.values = [][]driver.Value{{string(*content)}}
			*content = nil
			*hasContent = false
		}
	default:
		return nil, errors.New("query unsupported")
	}
	return rows, nil
}

type postgresStoreTestRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *postgresStoreTestRows) Columns() []string { return r.columns }

func (*postgresStoreTestRows) Close() error { return nil }

func (r *postgresStoreTestRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

func (b *postgresStoreTestBackend) snapshot() []postgresStoreTestCall {
	b.mu.Lock()
	defer b.mu.Unlock()
	calls := make([]postgresStoreTestCall, len(b.calls))
	copy(calls, b.calls)
	return calls
}

func (b *postgresStoreTestBackend) setContent(data []byte) {
	b.mu.Lock()
	b.content = append([]byte(nil), data...)
	b.hasContent = true
	b.mu.Unlock()
}

func (b *postgresStoreTestBackend) durableSnapshot() ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.content...), b.hasContent
}

type postgresStoreTestConnector struct {
	backend *postgresStoreTestBackend
}

func (c *postgresStoreTestConnector) Connect(context.Context) (driver.Conn, error) {
	return &postgresStoreTestConn{backend: c.backend}, nil
}

func (*postgresStoreTestConnector) Driver() driver.Driver { return postgresStoreTestDriver{} }

type postgresStoreTestDriver struct{}

func (postgresStoreTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type postgresStoreTestConn struct {
	backend *postgresStoreTestBackend
	tx      *postgresStoreTestTx
}

func (*postgresStoreTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare unsupported")
}

func (*postgresStoreTestConn) Close() error { return nil }

func (c *postgresStoreTestConn) Begin() (driver.Tx, error) {
	return c.beginTx()
}

func (c *postgresStoreTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.tx != nil {
		return c.tx.exec(query, args)
	}
	return c.backend.exec(query, args)
}

func (c *postgresStoreTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.tx != nil {
		return c.tx.query(query, args)
	}
	return c.backend.query(query, args)
}

func (c *postgresStoreTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.beginTx()
}

func (c *postgresStoreTestConn) beginTx() (driver.Tx, error) {
	if c.tx != nil {
		return nil, errors.New("transaction already active")
	}
	c.backend.mu.Lock()
	tx := &postgresStoreTestTx{
		conn:       c,
		content:    append([]byte(nil), c.backend.content...),
		hasContent: c.backend.hasContent,
	}
	c.backend.mu.Unlock()
	c.tx = tx
	return tx, nil
}

type postgresStoreTestTx struct {
	conn       *postgresStoreTestConn
	content    []byte
	hasContent bool
	done       bool
}

func (tx *postgresStoreTestTx) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	if err := tx.conn.backend.call(query, args); err != nil {
		return nil, err
	}
	if strings.HasPrefix(strings.TrimSpace(query), "INSERT INTO") && strings.Contains(query, "DO NOTHING") && !tx.hasContent {
		tx.conn.backend.mu.Lock()
		if tx.conn.backend.hasContent {
			tx.content = append([]byte(nil), tx.conn.backend.content...)
			tx.hasContent = true
			tx.conn.backend.mu.Unlock()
			return driver.RowsAffected(0), nil
		}
		tx.conn.backend.mu.Unlock()
	}
	return postgresStoreTestExec(query, args, &tx.content, &tx.hasContent)
}

func (tx *postgresStoreTestTx) query(query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := tx.conn.backend.call(query, args); err != nil {
		return nil, err
	}
	return postgresStoreTestQuery(query, &tx.content, &tx.hasContent)
}

func (tx *postgresStoreTestTx) Commit() error {
	if tx.done {
		return errors.New("transaction already done")
	}
	tx.conn.backend.mu.Lock()
	tx.conn.backend.content = append([]byte(nil), tx.content...)
	tx.conn.backend.hasContent = tx.hasContent
	tx.conn.backend.mu.Unlock()
	tx.done = true
	tx.conn.tx = nil
	return nil
}

func (tx *postgresStoreTestTx) Rollback() error {
	if tx.done {
		return sql.ErrTxDone
	}
	tx.done = true
	tx.conn.tx = nil
	return nil
}

func TestPostgresStoreSavePersistsBeforeLocalPublication(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	previous := []byte(`{"value":"previous"}`)
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	var seen []byte
	backend.inspect = func(postgresStoreTestCall) {
		seen, _ = os.ReadFile(path)
	}

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if string(seen) != string(previous) {
		t.Fatalf("visible bytes during database write = %q, want %q", seen, previous)
	}
	calls := backend.snapshot()
	if len(calls) != 2 || !strings.Contains(calls[0].query, "FOR UPDATE") || !strings.Contains(calls[1].query, "INSERT INTO") {
		t.Fatalf("database calls = %#v, want locked lookup then INSERT", calls)
	}
	assertJSONField(t, postgresStorePayload(t, calls[1]), "value", "candidate")
	published, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read published auth: %v", errRead)
	}
	assertJSONField(t, published, "value", "candidate")
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveFailureDoesNotPublishLocalFile(t *testing.T) {
	for _, test := range []struct {
		name     string
		previous []byte
	}{
		{name: "existing", previous: []byte(`{"value":"previous"}`)},
		{name: "new"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &postgresStoreTestBackend{failAt: 1}
			store := newPostgresStoreForTest(t, backend)
			path := filepath.Join(store.authDir, "credential.json")
			if test.previous != nil {
				if err := os.WriteFile(path, test.previous, 0o600); err != nil {
					t.Fatalf("write previous auth: %v", err)
				}
			}

			_, err := store.Save(context.Background(), &cliproxyauth.Auth{
				ID:       "credential.json",
				Metadata: map[string]any{"value": "candidate"},
			})
			if err == nil {
				t.Fatal("Save succeeded, want database error")
			}
			assertLocalStoreBytes(t, path, test.previous)
			assertNoStoreTempFile(t, path)
		})
	}
}

func TestPostgresStoreSavePublishFailureRollsBackDatabase(t *testing.T) {
	for _, test := range []struct {
		name         string
		previous     []byte
		installVerb  string
		rollbackVerb string
	}{
		{name: "restore existing", previous: []byte(`{"value":"previous"}`), installVerb: "UPDATE", rollbackVerb: "UPDATE"},
		{name: "delete new", installVerb: "INSERT INTO", rollbackVerb: "DELETE FROM"},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &postgresStoreTestBackend{}
			store := newPostgresStoreForTest(t, backend)
			path := filepath.Join(store.authDir, "credential.json")
			if test.previous != nil {
				if err := os.WriteFile(path, test.previous, 0o600); err != nil {
					t.Fatalf("write previous auth: %v", err)
				}
				backend.setContent(test.previous)
			}
			store.renameFile = func(string, string) error { return errors.New("publish rejected") }

			_, err := store.Save(context.Background(), &cliproxyauth.Auth{
				ID:       "credential.json",
				Metadata: map[string]any{"value": "candidate"},
			})
			if err == nil {
				t.Fatal("Save succeeded, want publish error")
			}
			calls := backend.snapshot()
			if len(calls) != 3 || !strings.Contains(calls[0].query, "FOR UPDATE") || !strings.Contains(calls[1].query, test.installVerb) || !strings.Contains(calls[2].query, test.rollbackVerb) {
				t.Fatalf("database calls = %#v, want locked lookup, %s, then %s", calls, test.installVerb, test.rollbackVerb)
			}
			if test.previous != nil && string(postgresStorePayload(t, calls[2])) != string(test.previous) {
				t.Fatalf("rollback bytes = %q, want %q", postgresStorePayload(t, calls[2]), test.previous)
			}
			assertLocalStoreBytes(t, path, test.previous)
			assertNoStoreTempFile(t, path)
		})
	}
}

func TestPostgresStoreSavePublishFailureRestoresExactDurableRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	localPrevious := []byte(`{"value":"local"}`)
	durablePrevious := []byte(`{"value":"durable"}`)
	if err := os.WriteFile(path, localPrevious, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	backend.setContent(durablePrevious)
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil {
		t.Fatal("Save succeeded, want publish error")
	}
	calls := backend.snapshot()
	if len(calls) != 3 || !strings.Contains(calls[0].query, "FOR UPDATE") || !strings.HasPrefix(strings.TrimSpace(calls[1].query), "UPDATE") || !strings.Contains(calls[2].query, "content = $3") {
		t.Fatalf("database calls = %#v, want locked lookup, replacement, then CAS restore", calls)
	}
	if rollback := postgresStorePayload(t, calls[2]); string(rollback) != string(durablePrevious) {
		t.Fatalf("rollback bytes = %q, want durable bytes %q", rollback, durablePrevious)
	}
	durable, exists := backend.durableSnapshot()
	if !exists || string(durable) != string(durablePrevious) {
		t.Fatalf("database record = (%q, %t), want restored durable record", durable, exists)
	}
	assertLocalStoreBytes(t, path, localPrevious)
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveInsertConflictCapturesConcurrentRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	localPrevious := []byte(`{"value":"local"}`)
	concurrent := []byte(`{"value":"concurrent"}`)
	if err := os.WriteFile(path, localPrevious, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	backend.inspect = func(call postgresStoreTestCall) {
		if strings.HasPrefix(strings.TrimSpace(call.query), "INSERT INTO") {
			backend.setContent(concurrent)
		}
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil {
		t.Fatal("Save succeeded, want publish error")
	}
	calls := backend.snapshot()
	if len(calls) != 5 || !strings.Contains(calls[0].query, "FOR UPDATE") || !strings.Contains(calls[1].query, "DO NOTHING") || !strings.Contains(calls[2].query, "FOR UPDATE") || !strings.HasPrefix(strings.TrimSpace(calls[3].query), "UPDATE") || !strings.Contains(calls[4].query, "content = $3") {
		t.Fatalf("database calls = %#v, want conflicting insert retried under row lock before CAS restore", calls)
	}
	if rollback := postgresStorePayload(t, calls[4]); string(rollback) != string(concurrent) {
		t.Fatalf("rollback bytes = %q, want concurrent bytes %q", rollback, concurrent)
	}
	durable, exists := backend.durableSnapshot()
	if !exists || string(durable) != string(concurrent) {
		t.Fatalf("database record = (%q, %t), want restored concurrent record", durable, exists)
	}
	assertLocalStoreBytes(t, path, localPrevious)
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveSurfacesPublishAndRollbackErrors(t *testing.T) {
	previous := []byte(`{"value":"previous"}`)
	backend := &postgresStoreTestBackend{failAt: 3}
	backend.setContent(previous)
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || !strings.Contains(err.Error(), "publish rejected") || !strings.Contains(err.Error(), "database rejected operation") {
		t.Fatalf("Save error = %v, want publish and rollback failures", err)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveRollbackConflictPreservesNewerDatabaseRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	previous := []byte(`{"value":"previous"}`)
	newer := []byte(`{"value":"newer"}`)
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	backend.setContent(previous)
	backend.inspect = func(call postgresStoreTestCall) {
		if strings.HasPrefix(strings.TrimSpace(call.query), "UPDATE") && strings.Contains(call.query, "content = $3") {
			backend.setContent(newer)
		}
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"value": "candidate"},
	})
	if err == nil || !strings.Contains(err.Error(), "database rollback failed") {
		t.Fatalf("Save error = %v, want rollback conflict", err)
	}
	durable, exists := backend.durableSnapshot()
	if !exists || string(durable) != string(newer) {
		t.Fatalf("database record = (%q, %t), want newer record", durable, exists)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveEmptyPayloadPublishFailureRestoresExactDeletedRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	localPrevious := []byte(`{"value":"local"}`)
	durablePrevious := []byte(`{"value":"durable"}`)
	if err := os.WriteFile(path, localPrevious, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	backend.setContent(durablePrevious)
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential.json",
		Storage: &storeTestTokenStorage{},
	})
	if err == nil {
		t.Fatal("Save succeeded, want publish error")
	}
	calls := backend.snapshot()
	if len(calls) != 2 || !strings.HasPrefix(strings.TrimSpace(calls[0].query), "DELETE") || !strings.Contains(calls[1].query, "DO NOTHING") {
		t.Fatalf("database calls = %#v, want DELETE then conditional INSERT", calls)
	}
	if string(postgresStorePayload(t, calls[1])) != string(durablePrevious) {
		t.Fatalf("rollback bytes = %q, want deleted durable bytes %q", postgresStorePayload(t, calls[1]), durablePrevious)
	}
	durable, exists := backend.durableSnapshot()
	if !exists || string(durable) != string(durablePrevious) {
		t.Fatalf("database record = (%q, %t), want restored durable record", durable, exists)
	}
	assertLocalStoreBytes(t, path, localPrevious)
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveEmptyPayloadRollbackConflictPreservesNewerDatabaseRecord(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	previous := []byte(`{"value":"previous"}`)
	newer := []byte(`{"value":"newer"}`)
	if err := os.WriteFile(path, previous, 0o600); err != nil {
		t.Fatalf("write previous auth: %v", err)
	}
	backend.setContent(previous)
	backend.inspect = func(call postgresStoreTestCall) {
		if strings.Contains(call.query, "DO NOTHING") {
			backend.setContent(newer)
		}
	}
	store.renameFile = func(string, string) error { return errors.New("publish rejected") }

	_, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential.json",
		Storage: &storeTestTokenStorage{},
	})
	if err == nil || !strings.Contains(err.Error(), "database rollback failed") {
		t.Fatalf("Save error = %v, want rollback conflict", err)
	}
	durable, exists := backend.durableSnapshot()
	if !exists || string(durable) != string(newer) {
		t.Fatalf("database record = (%q, %t), want newer record", durable, exists)
	}
	assertLocalStoreBytes(t, path, previous)
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveNewEmptyPayloadDeletesDurableRecord(t *testing.T) {
	durablePrevious := []byte(`{"value":"durable"}`)
	backend := &postgresStoreTestBackend{}
	backend.setContent(durablePrevious)
	store := newPostgresStoreForTest(t, backend)
	path, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:      "credential.json",
		Storage: &storeTestTokenStorage{},
	})
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	calls := backend.snapshot()
	if len(calls) != 1 || !strings.HasPrefix(strings.TrimSpace(calls[0].query), "DELETE") || !strings.Contains(calls[0].query, "RETURNING content") {
		t.Fatalf("database calls = %#v, want single DELETE ... RETURNING content", calls)
	}
	if _, exists := backend.durableSnapshot(); exists {
		t.Fatalf("database record still present, want deleted")
	}
	assertLocalStoreBytes(t, path, []byte{})
	assertNoStoreTempFile(t, path)
}

func TestPostgresStoreSaveUsesTemporaryPathForTokenStorage(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	storage := &storeTestTokenStorage{data: []byte(`{"type":"gemini","token":"value"}`), mode: 0o644}
	store.renameFile = func(oldPath, newPath string) error {
		assertStoreFileMode(t, oldPath, 0o600)
		return os.Rename(oldPath, newPath)
	}
	path, err := store.Save(context.Background(), &cliproxyauth.Auth{
		ID:       "credential.json",
		Metadata: map[string]any{"type": "gemini"},
		Storage:  storage,
	})
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

func TestPostgresStoreSavePreservesEmptyStorageNoOp(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	auth := &cliproxyauth.Auth{ID: "credential.json", Storage: &storeTestEmptyStorage{}}
	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("stat visible auth error = %v, want not exist", errStat)
	}
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("database calls = %#v, want none", calls)
	}
	assertSavedAuthNormalized(t, auth, path)
}

func TestPostgresStoreSaveSameJSONNormalizesAuth(t *testing.T) {
	backend := &postgresStoreTestBackend{}
	store := newPostgresStoreForTest(t, backend)
	path := filepath.Join(store.authDir, "credential.json")
	if err := os.WriteFile(path, []byte(`{"disabled":false,"value":"same"}`), 0o600); err != nil {
		t.Fatalf("write existing auth: %v", err)
	}
	auth := &cliproxyauth.Auth{ID: "credential.json", Metadata: map[string]any{"value": "same"}}
	gotPath, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if gotPath != path {
		t.Fatalf("Save path = %q, want %q", gotPath, path)
	}
	if calls := backend.snapshot(); len(calls) != 0 {
		t.Fatalf("database calls = %#v, want none", calls)
	}
	assertSavedAuthNormalized(t, auth, path)
}

func newPostgresStoreForTest(t *testing.T, backend *postgresStoreTestBackend) *PostgresStore {
	t.Helper()
	authDir := filepath.Join(t.TempDir(), "auths")
	if err := os.MkdirAll(authDir, 0o700); err != nil {
		t.Fatalf("create auth dir: %v", err)
	}
	db := sql.OpenDB(&postgresStoreTestConnector{backend: backend})
	t.Cleanup(func() { _ = db.Close() })
	return &PostgresStore{
		db:      db,
		cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
		authDir: authDir,
	}
}

func postgresStorePayload(t *testing.T, call postgresStoreTestCall) []byte {
	t.Helper()
	if len(call.args) < 2 {
		t.Fatalf("database args = %#v, want payload", call.args)
	}
	switch value := call.args[1].Value.(type) {
	case []byte:
		return value
	case string:
		return []byte(value)
	default:
		t.Fatalf("database payload type = %T", call.args[1].Value)
		return nil
	}
}

func clonePostgresStoreArgs(args []driver.NamedValue) []driver.NamedValue {
	cloned := make([]driver.NamedValue, len(args))
	copy(cloned, args)
	for i := range cloned {
		if value, ok := cloned[i].Value.([]byte); ok {
			cloned[i].Value = append([]byte(nil), value...)
		}
	}
	return cloned
}

func postgresStoreTestValue(args []driver.NamedValue, index int) []byte {
	if index >= len(args) {
		return nil
	}
	switch value := args[index].Value.(type) {
	case []byte:
		return append([]byte(nil), value...)
	case string:
		return []byte(value)
	default:
		return nil
	}
}

var _ driver.ExecerContext = (*postgresStoreTestConn)(nil)
var _ driver.QueryerContext = (*postgresStoreTestConn)(nil)
var _ driver.ConnBeginTx = (*postgresStoreTestConn)(nil)
var _ driver.Connector = (*postgresStoreTestConnector)(nil)
