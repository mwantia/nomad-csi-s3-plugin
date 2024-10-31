package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common/config"
)

const (
	MetadataName = ".metadata.json"
)

type S3Client struct {
	Config *S3Config
	Minio  *minio.Client
}

type S3Config struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"accesskey"`
	SecretAccessKey string `json:"secretkey"`
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

func CreateClientFromConfig(cfg *S3Config) (*S3Client, error) {
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	endpoint := u.Hostname()
	if u.Port() != "" {
		endpoint = u.Hostname() + ":" + u.Port()
	}

	m, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.Region),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		return nil, err
	}

	return &S3Client{
		Config: cfg,
		Minio:  m,
	}, nil
}

func CreateClient(cfg *config.DriverConfig, secret map[string]string) (*S3Client, error) {
	if alias, ok := secret["alias"]; ok {
		if cfg != nil {
			if a, ok := cfg.GetAlias(alias); ok {
				return CreateClientFromConfig(&S3Config{
					Endpoint:        a.Endpoint,
					Region:          a.Region,
					AccessKeyID:     a.AccessKeyID,
					SecretAccessKey: a.SecretAccessKey,
				})
			}
		}

		log.Printf("unable to create client based on the provided alias '%s'", alias)
	}

	return CreateClientFromConfig(&S3Config{
		Endpoint:        secret["endpoint"],
		Region:          secret["region"],
		AccessKeyID:     secret["accessKeyID"],
		SecretAccessKey: secret["secretAccessKey"],
	})
}

func (c *S3Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	exists, err := c.Minio.BucketExists(ctx, bucketName)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (c *S3Client) CreateBucket(ctx context.Context, bucketName string) error {
	err := c.Minio.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{
		Region: c.Config.Region,
	})
	if err != nil {
		return err
	}

	return nil
}

func (c *S3Client) CreatePrefix(ctx context.Context, bucketName string, prefix string) error {
	_, err := c.Minio.PutObject(ctx, bucketName, prefix+"/", bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})
	if err != nil {
		return err
	}

	return nil
}

func (c *S3Client) RemovePrefix(ctx context.Context, bucketName string, prefix string) error {
	var err error

	if err = c.RemoveObjects(ctx, bucketName, prefix); err == nil {
		return c.Minio.RemoveObject(ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	log.Printf("removeObjects failed with: %s", err)

	if err = c.RemoveObjectsOneByOne(ctx, bucketName, prefix); err == nil {
		return c.Minio.RemoveObject(ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	return err
}

func (c *S3Client) RemoveBucket(ctx context.Context, bucketName string) error {
	var err error

	if err = c.RemoveObjects(ctx, bucketName, ""); err == nil {
		return c.Minio.RemoveBucket(ctx, bucketName)
	}

	log.Printf("removeObjects failed with: %s, will try removeObjectsOneByOne", err)

	if err = c.RemoveObjectsOneByOne(ctx, bucketName, ""); err == nil {
		return c.Minio.RemoveBucket(ctx, bucketName)
	}

	return err
}

func (c *S3Client) RemoveObjects(ctx context.Context, bucketName, prefix string) error {
	objectsCh := make(chan minio.ObjectInfo)
	var err error

	go func() {
		defer close(objectsCh)

		for object := range c.Minio.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			if object.Err != nil {
				err = object.Err
				return
			}
			objectsCh <- object
		}
	}()

	if err != nil {
		return err
	}

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}
	errorCh := c.Minio.RemoveObjects(ctx, bucketName, objectsCh, opts)
	haveErrWhenRemoveObjects := false
	for e := range errorCh {
		log.Printf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
		haveErrWhenRemoveObjects = true
	}

	if haveErrWhenRemoveObjects {
		return fmt.Errorf("failed to remove all objects of bucket %s", bucketName)
	}

	return nil
}

func (c *S3Client) RemoveObjectsOneByOne(ctx context.Context, bucketName, prefix string) error {
	objectsCh := make(chan minio.ObjectInfo, 1)
	removeErrCh := make(chan minio.RemoveObjectError, 1)
	var err error

	go func() {
		defer close(objectsCh)

		for object := range c.Minio.ListObjects(ctx, bucketName,
			minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if object.Err != nil {
				err = object.Err
				return
			}
			objectsCh <- object
		}
	}()

	if err != nil {
		return err
	}

	go func() {
		defer close(removeErrCh)

		for object := range objectsCh {
			err := c.Minio.RemoveObject(ctx, bucketName, object.Key,
				minio.RemoveObjectOptions{VersionID: object.VersionID})
			if err != nil {
				removeErrCh <- minio.RemoveObjectError{
					ObjectName: object.Key,
					VersionID:  object.VersionID,
					Err:        err,
				}
			}
		}
	}()

	haveErrWhenRemoveObjects := false
	for e := range removeErrCh {
		log.Printf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
		haveErrWhenRemoveObjects = true
	}
	if haveErrWhenRemoveObjects {
		return fmt.Errorf("failed to remove all objects of path %s", bucketName)
	}

	return nil
}

func (c *S3Client) SetFSMeta(ctx context.Context, meta *FSMeta) error {
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(meta)
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	_, err := c.Minio.PutObject(ctx, meta.BucketName, path.Join(meta.Prefix, MetadataName), b, int64(b.Len()), opts)
	if err != nil {
		return err
	}

	return nil
}

func (c *S3Client) GetFSMeta(ctx context.Context, bucketName, prefix string) (*FSMeta, error) {
	opts := minio.GetObjectOptions{}
	obj, err := c.Minio.GetObject(ctx, bucketName, path.Join(prefix, MetadataName), opts)
	if err != nil {
		return &FSMeta{}, err
	}

	objInfo, err := obj.Stat()
	if err != nil {
		return &FSMeta{}, err
	}

	if objInfo.Size <= 0 {
		return &FSMeta{}, fmt.Errorf("invalid size defined for object")
	}

	b := make([]byte, objInfo.Size)
	_, err = obj.Read(b)

	if err != nil && err != io.EOF {
		return &FSMeta{}, err
	}

	var meta FSMeta
	err = json.Unmarshal(b, &meta)
	if err != nil {
		return &FSMeta{}, err
	}

	return &meta, nil
}
