package mount

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"k8s.io/mount-utils"
)

func BindFSMount(ctx context.Context, source, target string) error {
	if _, err := exec.LookPath("bindfs"); err != nil {
		return fmt.Errorf("bindfs not found in PATH: %w", err)
	}

	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("source path error: %w", err)
	}

	if err := os.MkdirAll(target, 0o750); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "bindfs", source, target)
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bindfs mount failed: %v - stderr: %s", err, stderr.String())
	}

	// Verify mount success
	verify, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := NewMountVerifier().WaitForMount(verify, target); err != nil {
		if cleanup := CleanupMountPoint(target); cleanup != nil {
			return fmt.Errorf("mount verification failed: %v, cleanup failed: %v", err, cleanup)
		}

		return err
	}

	mounted, err := IsBindMounted(target)
	if err != nil {
		return fmt.Errorf("mount verification failed: %w", err)
	}
	if !mounted {
		return fmt.Errorf("bindfs mount verification failed: mount point not found")
	}

	return nil
}

func UnmountBindFS(ctx context.Context, target string) error {
	if err := CleanupMountPoint(target); err != nil {
		return err
	}

	// Try to remove the target directory
	if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func IsBindMounted(path string) (bool, error) {
	mounter := mount.New("")
	notMount, err := mounter.IsLikelyNotMountPoint(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return !notMount, nil
}

func CleanupMountPoint(mountPath string) error {
	mounter := mount.New("")

	if err := mount.CleanupMountPoint(mountPath, mounter, true); err != nil {
		return err
	}

	return nil
}
