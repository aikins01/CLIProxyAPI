//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package amp

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestReadNeoWorkspaceImageRejectsFIFO(t *testing.T) {
	workspace := t.TempDir()
	if err := unix.Mkfifo(filepath.Join(workspace, "pipe"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readNeoWorkspaceImage(workspace, "pipe"); err == nil {
		t.Fatal("FIFO workspace image was accepted")
	}
}

func TestReadNeoWorkspaceImageRejectsHardLinkedFinalFile(t *testing.T) {
	workspace := t.TempDir()
	imagePath := filepath.Join(workspace, "image.png")
	if err := os.WriteFile(imagePath, []byte("image-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(imagePath, filepath.Join(workspace, "image-alias.png")); err != nil {
		t.Fatal(err)
	}
	if _, err := readNeoWorkspaceImage(workspace, "image.png"); err == nil {
		t.Fatal("hard-linked workspace image was accepted")
	}
	if _, err := readNeoWorkspaceImage(workspace, "image-alias.png"); err == nil {
		t.Fatal("hard-link alias was accepted")
	}
}
