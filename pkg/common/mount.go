package common

import (
	"os"
	"os/exec"

	"k8s.io/mount-utils"
)

func CheckMount(path string) (bool, error) {
	isMounted, err := mount.New("").IsLikelyNotMountPoint(path)
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

func MoundBindFS(source, target string) error {
	args := []string{
		source,
		target,
	}
	cmd := exec.Command("bindfs", args...)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return nil
}

func CleanupMountPoint(mountPath string) error {
	mounter := mount.New("")

	if err := mount.CleanupMountPoint(mountPath, mounter, true); err != nil {
		return err
	}

	return nil
}
