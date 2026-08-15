//go:build darwin

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const (
	publishImageEndpoint  = "/ampcode/local-broker/publish-image-result.json"
	publishImageMaxBytes  = 5138022
	publishImageMaxCount  = 8
	publishImagePathLimit = 4096
)

var errPublishImageTooLarge = errors.New("publish image exceeds size limit")

type publishImageRequest struct {
	RequestID  string `json:"requestId"`
	ThreadID   string `json:"threadId"`
	RunnerID   string `json:"runnerId"`
	BrokerID   string `json:"brokerId"`
	SessionID  string `json:"sessionId"`
	Generation uint64 `json:"sessionGeneration"`
	Path       string `json:"path"`
}

type publishImageResult struct {
	RequestID  string `json:"requestId"`
	ThreadID   string `json:"threadId"`
	RunnerID   string `json:"runnerId"`
	BrokerID   string `json:"brokerId"`
	SessionID  string `json:"sessionId"`
	Generation uint64 `json:"sessionGeneration"`
	Data       string `json:"data,omitempty"`
	ErrorCode  string `json:"errorCode,omitempty"`
}

func validatePublishImageRelativePath(relative string) error {
	if relative == "" || len(relative) > publishImagePathLimit || strings.TrimSpace(relative) != relative || strings.ContainsRune(relative, 0) || strings.Contains(relative, "\\") || strings.HasPrefix(relative, "/") || path.IsAbs(relative) || path.Clean(relative) != relative || relative == "." {
		return errors.New("invalid publish image path")
	}
	for _, component := range strings.Split(relative, "/") {
		if component == "" || component == "." || component == ".." {
			return errors.New("invalid publish image path")
		}
	}
	return nil
}

func (localBroker *broker) validatePublishImageRequests(requests *[]publishImageRequest) error {
	if requests == nil {
		return nil
	}
	if len(*requests) > publishImageMaxCount {
		return errors.New("response contains too many publish image requests")
	}
	seen := make(map[string]struct{}, len(*requests))
	for _, request := range *requests {
		if request.RequestID == "" || len(request.RequestID) > identifierLimit || containsControl(request.RequestID) {
			return errors.New("response contains invalid publish image request ID")
		}
		if _, exists := seen[request.RequestID]; exists {
			return errors.New("response contains duplicate publish image request ID")
		}
		seen[request.RequestID] = struct{}{}
		if request.BrokerID != localBroker.config.BrokerID || request.SessionID != localBroker.sessionID || request.Generation != localBroker.sessionGeneration {
			return errors.New("response contains publish image request for another broker session")
		}
		if !threadIDPattern.MatchString(request.ThreadID) {
			return errors.New("response contains invalid publish image thread ID")
		}
		if _, ok := localBroker.workspacesByRunner[request.RunnerID]; !ok {
			return errors.New("response contains publish image request for an unapproved runner")
		}
		if err := validatePublishImageRelativePath(request.Path); err != nil {
			return err
		}
	}
	return nil
}

func (localBroker *broker) processPublishImageRequests(ctx context.Context, response heartbeatResponse) error {
	var requestErrors []error
	draining := true
	for draining {
		select {
		case err := <-localBroker.publishImageErrors:
			requestErrors = append(requestErrors, err)
		default:
			draining = false
		}
	}
	if response.PublishImageRequests == nil || len(*response.PublishImageRequests) == 0 {
		return errors.Join(requestErrors...)
	}
	for _, request := range *response.PublishImageRequests {
		request := request
		localBroker.mu.Lock()
		if localBroker.shuttingDown {
			localBroker.mu.Unlock()
			break
		}
		if localBroker.publishImageActive == nil {
			localBroker.publishImageActive = make(map[string]struct{})
		}
		_, active := localBroker.publishImageActive[request.RequestID]
		if !active && len(localBroker.publishImageActive) >= publishImageMaxCount {
			localBroker.mu.Unlock()
			requestErrors = append(requestErrors, errors.New("too many active publish image requests"))
			continue
		}
		if localBroker.publishImageCtx == nil {
			localBroker.publishImageCtx, localBroker.publishImageCancel = context.WithCancel(ctx)
		}
		publishImageCtx := localBroker.publishImageCtx
		if !active {
			localBroker.publishImageActive[request.RequestID] = struct{}{}
			localBroker.publishImageWG.Add(1)
		}
		localBroker.mu.Unlock()
		if active {
			continue
		}
		go func() {
			defer localBroker.publishImageWG.Done()
			defer func() {
				localBroker.mu.Lock()
				delete(localBroker.publishImageActive, request.RequestID)
				localBroker.mu.Unlock()
			}()
			if err := localBroker.fulfillPublishImageRequest(publishImageCtx, request); err != nil {
				select {
				case localBroker.publishImageErrors <- err:
				default:
				}
			}
		}()
	}
	return errors.Join(requestErrors...)
}

func (localBroker *broker) fulfillPublishImageRequest(ctx context.Context, request publishImageRequest) error {
	result := publishImageResult{RequestID: request.RequestID, ThreadID: request.ThreadID, RunnerID: request.RunnerID, BrokerID: request.BrokerID, SessionID: request.SessionID, Generation: request.Generation}
	if !localBroker.publishImageAssignmentCurrent(request) {
		result.ErrorCode = "stale_assignment"
	} else {
		workspace := localBroker.workspacesByRunner[request.RunnerID]
		data, err := readPublishImageWorkspaceFile(workspace, request.Path)
		switch {
		case errors.Is(err, errPublishImageTooLarge):
			result.ErrorCode = "too_large"
		case err != nil:
			result.ErrorCode = "read_failed"
		case !localBroker.publishImageAssignmentCurrent(request):
			result.ErrorCode = "stale_assignment"
		default:
			result.Data = base64.StdEncoding.EncodeToString(data)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return localBroker.postPublishImageResult(ctx, result)
}

func (localBroker *broker) publishImageAssignmentCurrent(request publishImageRequest) bool {
	if request.BrokerID != localBroker.config.BrokerID || request.SessionID != localBroker.sessionID || request.Generation != localBroker.sessionGeneration {
		return false
	}
	localBroker.mu.Lock()
	defer localBroker.mu.Unlock()
	child := localBroker.children[childKey{runnerID: request.RunnerID, threadID: request.ThreadID}]
	return child != nil && !child.stopping && !localBroker.shuttingDown
}

func readPublishImageWorkspaceFile(workspace *workspaceConfig, relative string) (data []byte, resultErr error) {
	return readPublishImageWorkspaceFileWithRootOpener(workspace, relative, openPublishImageWorkspaceRoot)
}

func openPublishImageWorkspaceRoot(workspace string) (*os.File, error) {
	rootFD, err := unix.Open(workspace, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	root := os.NewFile(uintptr(rootFD), "publish-image-workspace")
	if root == nil {
		_ = unix.Close(rootFD)
		return nil, errors.New("publish image workspace is unavailable")
	}
	return root, nil
}

func readPublishImageWorkspaceFileWithRootOpener(workspace *workspaceConfig, relative string, openRoot func(string) (*os.File, error)) (data []byte, resultErr error) {
	if err := validatePublishImageRelativePath(relative); err != nil {
		return nil, err
	}
	if workspace == nil || workspace.info == nil {
		return nil, errors.New("publish image workspace is unavailable")
	}
	root, err := openRoot(workspace.Path)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	rootInfo, err := root.Stat()
	if err != nil || !rootInfo.IsDir() || !os.SameFile(workspace.info, rootInfo) {
		return nil, errors.New("publish image workspace changed after approval")
	}
	rootFD := int(root.Fd())
	currentFD := rootFD
	for index, component := range strings.Split(relative, "/") {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if index < strings.Count(relative, "/") {
			flags |= unix.O_DIRECTORY
		} else {
			flags |= unix.O_NONBLOCK
		}
		nextFD, openErr := unix.Openat(currentFD, component, flags, 0)
		if currentFD != rootFD {
			closeErr := unix.Close(currentFD)
			if openErr != nil || closeErr != nil {
				if openErr == nil {
					closeErr = errors.Join(closeErr, unix.Close(nextFD))
				}
				return nil, errors.Join(openErr, closeErr)
			}
		}
		if openErr != nil {
			return nil, openErr
		}
		currentFD = nextFD
	}
	file := os.NewFile(uintptr(currentFD), relative)
	if file == nil {
		return nil, errors.New("publish image is unavailable")
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 {
		return nil, errors.New("publish image is not a regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return nil, errors.New("publish image must not be hard linked")
	}
	data, err = io.ReadAll(io.LimitReader(file, publishImageMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("publish image is empty")
	}
	if len(data) > publishImageMaxBytes {
		return nil, errPublishImageTooLarge
	}
	return data, nil
}

func (localBroker *broker) postPublishImageResult(ctx context.Context, result publishImageResult) error {
	body, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode publish image result: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, localBroker.config.APIURL+publishImageEndpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create publish image result request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+localBroker.config.apiKey)
	request.Header.Set("Content-Type", "application/json")
	response, err := localBroker.client.Do(request)
	if err != nil {
		return fmt.Errorf("send publish image result: %w", err)
	}
	_, readErr := io.ReadAll(io.LimitReader(response.Body, maxHeartbeatBody+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return errors.New("read publish image result response")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("publish image result rejected with HTTP %d", response.StatusCode)
	}
	return nil
}
