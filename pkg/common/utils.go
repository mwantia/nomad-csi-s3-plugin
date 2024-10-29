package common

import (
	"crypto/sha1"
	"encoding/hex"
	"hash/fnv"
	"io"
	"strings"
	"sync"
)

type KeyMutex struct {
	mutexes []sync.RWMutex
	size    int32
}

func VolumeIDToBucketPrefix(volumeID string) (string, string) {
	splitVolumeID := strings.Split(volumeID, "/")
	if len(splitVolumeID) > 1 {
		return splitVolumeID[0], splitVolumeID[1]
	}

	return volumeID, ""
}

func SanitizeVolumeID(volumeID string) string {
	volumeID = strings.ToLower(volumeID)
	if len(volumeID) > 63 {
		h := sha1.New()
		io.WriteString(h, volumeID)
		volumeID = hex.EncodeToString(h.Sum(nil))
	}
	return volumeID
}

func HashToUint32(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)

	return h.Sum32()
}

func NewKeyMutex(size int32) *KeyMutex {
	return &KeyMutex{
		mutexes: make([]sync.RWMutex, size),
		size:    size,
	}
}

func (km *KeyMutex) GetMutex(key string) *sync.RWMutex {
	hashed := HashToUint32([]byte(key))
	index := hashed % uint32(km.size)

	return &km.mutexes[index]
}
