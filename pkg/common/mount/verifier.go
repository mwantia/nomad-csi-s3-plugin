package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"k8s.io/mount-utils"
)

type MountVerifier struct {
	mounter *mount.SafeFormatAndMount
	backoff *ExponentialBackoff
}

type ExponentialBackoff struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsed      time.Duration
	Multiplier      float64
}

func NewMountVerifier() *MountVerifier {
	return &MountVerifier{
		mounter: &mount.SafeFormatAndMount{
			Interface: mount.New(""),
		},
		backoff: &ExponentialBackoff{
			InitialInterval: 100 * time.Millisecond,
			MaxInterval:     2 * time.Second,
			MaxElapsed:      30 * time.Second,
			Multiplier:      1.5,
		},
	}
}

func (mv *MountVerifier) WaitForMount(ctx context.Context, path string) error {
	interval := mv.backoff.InitialInterval
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			mounted, err := mv.VerifyMount(path)
			if err != nil {
				return fmt.Errorf("mount verification failed: %w", err)
			}
			if mounted {
				return nil
			}

			if time.Since(start) > mv.backoff.MaxElapsed {
				return errors.New("timeout waiting for mount")
			}

			time.Sleep(interval)
			interval = time.Duration(float64(interval) * mv.backoff.Multiplier)
			if interval > mv.backoff.MaxInterval {
				interval = mv.backoff.MaxInterval
			}
		}
	}
}

func (mv *MountVerifier) VerifyMount(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	notMount, err := mv.mounter.IsLikelyNotMountPoint(path)
	if err != nil {
		return false, err
	}
	if notMount {
		return false, nil
	}

	stat := syscall.Statfs_t{}
	if err := syscall.Statfs(path, &stat); err != nil {
		return false, fmt.Errorf("statfs failed: %w", err)
	}

	// Additional checks can be added here
	// - Check filesystem type
	// - Verify mount options
	// - Check for read/write access

	return true, nil
}

func CheckMount(path string) (bool, error) {
	verifier := NewMountVerifier()

	isMounted, err := verifier.VerifyMount(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(path, 0o750); err != nil {
				return false, err
			}
			isMounted = true
		} else {
			return false, err
		}
	}

	return isMounted, nil
}
