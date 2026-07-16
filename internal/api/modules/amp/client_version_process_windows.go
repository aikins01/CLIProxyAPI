//go:build windows

package amp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func ampRunClientVersionProbe(ctx context.Context, command string, args, env []string, waitDelay time.Duration) ([]byte, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, fmt.Errorf("create Amp version probe job: %w", err)
	}
	defer windows.CloseHandle(job)

	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err = windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)),
		uint32(unsafe.Sizeof(limits)),
	); err != nil {
		return nil, fmt.Errorf("configure Amp version probe job: %w", err)
	}

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Env = env
	cmd.WaitDelay = waitDelay
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.CREATE_SUSPENDED}
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Cancel = func() error {
		return ampTerminateWindowsClientVersionProbe(job, cmd)
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}

	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		_ = ampTerminateWindowsClientVersionProbe(job, cmd)
		_ = cmd.Wait()
		return nil, fmt.Errorf("open Amp version probe process: %w", err)
	}
	err = windows.AssignProcessToJobObject(job, process)
	windows.CloseHandle(process)
	if err != nil {
		_ = ampTerminateWindowsClientVersionProbe(job, cmd)
		_ = cmd.Wait()
		return nil, fmt.Errorf("assign Amp version probe job: %w", err)
	}
	if err = ampResumeWindowsProcess(uint32(cmd.Process.Pid)); err != nil {
		_ = ampTerminateWindowsClientVersionProbe(job, cmd)
		_ = cmd.Wait()
		return nil, fmt.Errorf("resume Amp version probe: %w", err)
	}

	err = cmd.Wait()
	terminateErr := ampTerminateWindowsClientVersionProbe(job, cmd)
	if err == nil {
		err = terminateErr
	}
	return stdout.Bytes(), err
}

func ampResumeWindowsProcess(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	if err = windows.Thread32First(snapshot, &entry); err != nil {
		return err
	}
	for {
		if entry.OwnerProcessID == pid {
			thread, openErr := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
			if openErr != nil {
				return openErr
			}
			_, resumeErr := windows.ResumeThread(thread)
			windows.CloseHandle(thread)
			return resumeErr
		}
		if err = windows.Thread32Next(snapshot, &entry); err != nil {
			if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
				return fmt.Errorf("Amp version probe thread not found")
			}
			return err
		}
	}
}

func ampTerminateWindowsClientVersionProbe(job windows.Handle, cmd *exec.Cmd) error {
	jobErr := windows.TerminateJobObject(job, 1)
	var processErr error
	if cmd != nil && cmd.Process != nil {
		processErr = cmd.Process.Kill()
		if errors.Is(processErr, os.ErrProcessDone) || errors.Is(processErr, syscall.EINVAL) {
			processErr = nil
		}
	}
	if jobErr != nil {
		return jobErr
	}
	return processErr
}
