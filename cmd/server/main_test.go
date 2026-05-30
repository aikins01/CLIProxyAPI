package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeFileInfo struct {
	isDir bool
}

func (f fakeFileInfo) Name() string       { return "cliproxyapi.conf" }
func (f fakeFileInfo) Size() int64        { return 1 }
func (f fakeFileInfo) Mode() fs.FileMode  { return 0o600 }
func (f fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f fakeFileInfo) IsDir() bool        { return f.isDir }
func (f fakeFileInfo) Sys() any           { return nil }

func TestHomebrewConfigPathForExecutableUsesArmHomebrewConfig(t *testing.T) {
	stat := func(path string) (fs.FileInfo, error) {
		if path == filepath.Join("/opt/homebrew", "etc", "cliproxyapi.conf") {
			return fakeFileInfo{}, nil
		}
		return nil, os.ErrNotExist
	}

	got := homebrewConfigPathForExecutable([]string{"/opt/homebrew/opt/cliproxyapi/bin/cliproxyapi"}, stat)
	if got != "/opt/homebrew/etc/cliproxyapi.conf" {
		t.Fatalf("config path = %q, want /opt/homebrew/etc/cliproxyapi.conf", got)
	}
}

func TestHomebrewConfigPathForExecutableUsesIntelHomebrewConfig(t *testing.T) {
	stat := func(path string) (fs.FileInfo, error) {
		if path == filepath.Join("/usr/local", "etc", "cliproxyapi.conf") {
			return fakeFileInfo{}, nil
		}
		return nil, os.ErrNotExist
	}

	got := homebrewConfigPathForExecutable([]string{"/usr/local/Cellar/cliproxyapi/1.0.0/bin/cliproxyapi"}, stat)
	if got != "/usr/local/etc/cliproxyapi.conf" {
		t.Fatalf("config path = %q, want /usr/local/etc/cliproxyapi.conf", got)
	}
}

func TestHomebrewConfigPathForExecutableSkipsMissingConfig(t *testing.T) {
	stat := func(string) (fs.FileInfo, error) {
		return nil, os.ErrNotExist
	}

	got := homebrewConfigPathForExecutable([]string{"/opt/homebrew/opt/cliproxyapi/bin/cliproxyapi"}, stat)
	if got != "" {
		t.Fatalf("config path = %q, want empty", got)
	}
}

func TestHomebrewConfigPathForExecutableSkipsNonHomebrewExecutable(t *testing.T) {
	stat := func(string) (fs.FileInfo, error) {
		return fakeFileInfo{}, nil
	}

	got := homebrewConfigPathForExecutable([]string{"/tmp/cliproxyapi"}, stat)
	if got != "" {
		t.Fatalf("config path = %q, want empty", got)
	}
}
