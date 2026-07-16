package amp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "cliproxyapi-amp-tests-")
	if err != nil {
		panic(err)
	}
	neoAmpDataDir = func() string { return filepath.Join(root, "data") }
	neoHeadlessPIDDir = func() string { return filepath.Join(root, "owned-pids") }
	neoAmpHeadlessPIDDir = func() string { return filepath.Join(root, "cache", "amp", "pids") }
	if err := os.Setenv("XDG_CACHE_HOME", filepath.Join(root, "cache")); err != nil {
		panic(err)
	}

	code := m.Run()
	_ = os.RemoveAll(root)
	os.Exit(code)
}
