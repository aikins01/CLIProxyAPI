//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package amp

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

func readNeoWorkspaceImage(workspace, relative string) ([]byte, error) {
	if workspace == "" || !filepath.IsAbs(workspace) {
		return nil, errors.New("publish_image workspace is unavailable")
	}
	if _, err := validateNeoPublishImagePath(relative); err != nil {
		return nil, err
	}
	rootFD, err := unix.Open(workspace, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.New("publish_image workspace is unavailable")
	}
	defer func() {
		if rootFD >= 0 {
			_ = unix.Close(rootFD)
		}
	}()
	currentFD := rootFD
	components := strings.Split(relative, "/")
	for index, component := range components {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if index < len(components)-1 {
			flags |= unix.O_DIRECTORY
		} else {
			flags |= unix.O_NONBLOCK
		}
		nextFD, err := unix.Openat(currentFD, component, flags, 0)
		if err != nil {
			if currentFD != rootFD {
				_ = unix.Close(currentFD)
			}
			return nil, errors.New("publish_image could not read the image")
		}
		if currentFD != rootFD {
			_ = unix.Close(currentFD)
		}
		currentFD = nextFD
	}
	if currentFD == rootFD {
		rootFD = -1
	}
	file := os.NewFile(uintptr(currentFD), relative)
	if file == nil {
		_ = unix.Close(currentFD)
		return nil, errors.New("publish_image could not read the image")
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			log.Error("amp orbs: publish image file close error")
		}
	}()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !neoOrbFileHasSingleLink(info) {
		return nil, errors.New("publish_image path must name a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, neoAttachmentMaxImageBytes+1))
	if err != nil {
		return nil, errors.New("publish_image could not read the image")
	}
	if len(data) > neoAttachmentMaxImageBytes {
		return nil, errors.New("publish_image image exceeds the maximum size")
	}
	return data, nil
}

func neoOrbFileOwnedByCurrentUser(info os.FileInfo) bool {
	if info == nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid()
}

func neoOrbFileHasSingleLink(info os.FileInfo) bool {
	if info == nil {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1
}
