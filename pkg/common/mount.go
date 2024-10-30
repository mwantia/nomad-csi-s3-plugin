package common

import (
	"os"

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
