package auth

import "context"

type skipPersistContextKey struct{}
type watcherReplayContextKey struct{}

// WithSkipPersist returns a derived context that disables persistence for Manager Update/Register calls.
func WithSkipPersist(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, skipPersistContextKey{}, true)
}

// WithWatcherReplay marks a file watcher update and disables persistence to avoid a write-back loop.
func WithWatcherReplay(ctx context.Context) context.Context {
	return context.WithValue(WithSkipPersist(ctx), watcherReplayContextKey{}, true)
}

func shouldSkipPersist(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	v := ctx.Value(skipPersistContextKey{})
	enabled, ok := v.(bool)
	return ok && enabled
}

func isWatcherReplay(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(watcherReplayContextKey{}).(bool)
	return enabled
}
