package mounter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	cmount "github.com/mwantia/nomad-csi-s3-plugin/pkg/common/mount"

	"github.com/mitchellh/go-ps"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
	"k8s.io/utils/mount"
)

type Mounter interface {
	Stage(ctx context.Context, stagePath string) error
	Unstage(ctx context.Context, stagePath string) error
	Mount(ctx context.Context, source string, target string) error
	Unmount(ctx context.Context, target string) error
}

func NewMounter(meta *s3.FSMeta, cfg *s3.S3Config) (Mounter, error) {
	mounter := meta.Mounter
	if len(mounter) <= 0 {
		mounter = cfg.Mounter
	}

	switch mounter {
	case "s3fs":
		return NewS3FSMounter(meta, cfg)
	}

	// Defaults to 's3fs'
	return NewS3FSMounter(meta, cfg)
}

func FuseMount(ctx context.Context, path, command string, args []string) error {
	cmd := exec.Command(command, args...)

	log.Printf("Mounting fuse with command: %s and args: %s", command, args)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error fuseMount command: %s\nargs: %s\noutput", command, args)
	}

	verifier := cmount.NewMountVerifier()
	return verifier.WaitForMount(ctx, path)
}

func FuseUnmount(ctx context.Context, path string) error {
	if err := mount.New("").Unmount(path); err != nil {
		return err
	}
	// as fuse quits immediately, we will try to wait until the process is done
	process, err := FindFuseMountProcess(ctx, path)
	if err != nil {
		log.Printf("Error getting PID of fuse mount: %s", err)

		return nil
	}
	if process == nil {
		log.Printf("Unable to find PID of fuse mount %s, it must have finished already", path)

		return nil
	}

	log.Printf("Found fuse pid %v of mount %s, checking if it still runs", process.Pid, path)

	return WaitForProcess(ctx, process, 1)
}

func WaitForMount(ctx context.Context, path string, timeout time.Duration) error {
	var elapsed time.Duration
	interval := 10 * time.Millisecond
	for {
		notMount, err := mount.New("").IsLikelyNotMountPoint(path)
		if err != nil {
			return err
		}
		if !notMount {
			return nil
		}

		time.Sleep(interval)
		elapsed = elapsed + interval

		if elapsed >= timeout {
			return errors.New("timeout waiting for mount")
		}
	}
}

func FindFuseMountProcess(ctx context.Context, path string) (*os.Process, error) {
	processes, err := ps.Processes()
	if err != nil {
		return nil, err
	}
	for _, p := range processes {
		cmdLine, err := GetCmdLine(p.Pid())
		if err != nil {
			log.Printf("Unable to get cmdline of PID %v: %s", p.Pid(), err)

			continue
		}
		if strings.Contains(cmdLine, path) {
			log.Printf("Found matching pid %v on path %s", p.Pid(), path)

			process, err := os.FindProcess(p.Pid())
			if err != nil {
				return nil, err
			}

			return process, err
		}
	}

	return nil, nil
}

func WaitForProcess(ctx context.Context, p *os.Process, backoff int) error {
	if backoff == 20 {
		return fmt.Errorf("timeout waiting for PID %v to end", p.Pid)
	}
	cmdLine, err := GetCmdLine(p.Pid)
	if err != nil {
		log.Printf("Error checking cmdline of PID %v, assuming it is dead: %s", p.Pid, err)

		return nil
	}
	if cmdLine == "" {
		// ignore defunct processes
		// TODO: debug why this happens in the first place
		// seems to only happen on k8s, not on local docker
		log.Printf("Fuse process seems dead, returning")

		return nil
	}
	if err := p.Signal(syscall.Signal(0)); err != nil {
		log.Printf("Fuse process does not seem active or we are unprivileged: %s", err)

		return nil
	}

	log.Printf("Fuse process with PID %v still active, waiting...", p.Pid)
	time.Sleep(time.Duration(backoff*100) * time.Millisecond)

	return WaitForProcess(ctx, p, backoff+1)
}

func GetCmdLine(pid int) (string, error) {
	cmdLineFile := fmt.Sprintf("/proc/%v/cmdline", pid)
	cmdLine, err := os.ReadFile(cmdLineFile)
	if err != nil {
		return "", err
	}

	return string(cmdLine), nil
}
