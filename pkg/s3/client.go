package s3

import (
	"context"

	"github.com/minio/minio-go/v7"
)

const (
	MetadataName = ".metadata.json"
)

type S3Client struct {
	Config  *S3Config
	Minio   *minio.Client
	Context context.Context
}

type S3Config struct {
	AccessKeyID     string `json:"accesskey"`
	SecretAccessKey string `json:"secretkey"`
	Region          string `json:"region"`
	Endpoint        string `json:"endpoint"`
	Mounter         string `json:"mounter"`
}

type FSMeta struct {
	BucketName    string `json:"name"`
	Prefix        string `json:"prefix"`
	UsePrefix     bool   `json:"useprefix"`
	Mounter       string `json:"mounter"`
	FSPath        string `json:"fspath"`
	CapacityBytes int64  `json:"capacitybytes"`
}

