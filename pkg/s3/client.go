package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"

	"github.com/golang/glog"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	MetadataName  = ".metadata.json"
	VendorVersion = "v1.2.0-rc.2"
	DriverName    = "github.com.mwantia.nomad-csi-s3-plugin"
)

type S3Client struct {
	Config *S3Config
	Minio  *minio.Client
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

func NewClientFromConfig(cfg *S3Config) (*S3Client, error) {
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

func NewClientFromSecret(secret map[string]string) (*S3Client, error) {
	return NewClientFromConfig(&S3Config{
		AccessKeyID:     secret["accessKeyID"],
		SecretAccessKey: secret["secretAccessKey"],
		Region:          secret["region"],
		Endpoint:        secret["endpoint"],
	})
}

func (c *S3Client) BucketExists(ctx context.Context, bucketName string) (bool, error) {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "BucketExists",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	exists, err := c.Minio.BucketExists(ctx, bucketName)
	span.SetAttributes(attribute.Bool("exists", exists))

	if err != nil {
		return false, common.HandleInternalError(err, span)
	}

	return exists, nil
}

func (c *S3Client) CreateBucket(ctx context.Context, bucketName string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "CreateBucket",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	err := c.Minio.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{
		Region: c.Config.Region,
	})
	if err != nil {
		return common.HandleInternalError(err, span)
	}

	return nil
}

func (c *S3Client) CreatePrefix(ctx context.Context, bucketName string, prefix string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "CreatePrefix",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
			attribute.String("prefix", prefix),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	_, err := c.Minio.PutObject(ctx, bucketName, prefix+"/", bytes.NewReader([]byte("")), 0, minio.PutObjectOptions{})
	if err != nil {
		return common.HandleInternalError(err, span)
	}

	return nil
}

func (c *S3Client) RemovePrefix(ctx context.Context, bucketName string, prefix string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "RemovePrefix",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
			attribute.String("prefix", prefix),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	var err error

	if err = c.RemoveObjects(ctx, bucketName, prefix); err == nil {
		return c.Minio.RemoveObject(ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	glog.Warningf("removeObjects failed with: %s", err)

	if err = c.RemoveObjectsOneByOne(ctx, bucketName, prefix); err == nil {
		return c.Minio.RemoveObject(ctx, bucketName, prefix, minio.RemoveObjectOptions{})
	}

	return common.HandleInternalError(err, span)
}

func (c *S3Client) RemoveBucket(ctx context.Context, bucketName string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "RemoveBucket",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	var err error

	if err = c.RemoveObjects(ctx, bucketName, ""); err == nil {
		return c.Minio.RemoveBucket(ctx, bucketName)
	}

	glog.Warningf("removeObjects failed with: %s, will try removeObjectsOneByOne", err)

	if err = c.RemoveObjectsOneByOne(ctx, bucketName, ""); err == nil {
		return c.Minio.RemoveBucket(ctx, bucketName)
	}

	return common.HandleInternalError(err, span)
}

func (c *S3Client) RemoveObjects(ctx context.Context, bucketName, prefix string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "RemoveObjects",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
			attribute.String("prefix", prefix),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

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
		glog.Error("Error listing objects", err)

		return common.HandleInternalError(err, span)
	}

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}
	errorCh := c.Minio.RemoveObjects(ctx, bucketName, objectsCh, opts)
	haveErrWhenRemoveObjects := false
	for e := range errorCh {
		glog.Errorf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
		haveErrWhenRemoveObjects = true
	}

	if haveErrWhenRemoveObjects {
		err := fmt.Errorf("failed to remove all objects of bucket %s", bucketName)

		return common.HandleInternalError(err, span)
	}

	return nil
}

func (c *S3Client) RemoveObjectsOneByOne(ctx context.Context, bucketName, prefix string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "RemoveObjectsOneByOne",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", bucketName),
			attribute.String("prefix", prefix),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

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
		glog.Error("Error listing objects", err)

		return common.HandleInternalError(err, span)
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
		glog.Errorf("Failed to remove object %s, error: %s", e.ObjectName, e.Err)
		haveErrWhenRemoveObjects = true
	}
	if haveErrWhenRemoveObjects {
		err := fmt.Errorf("failed to remove all objects of path %s", bucketName)

		return common.HandleInternalError(err, span)
	}

	return nil
}

func (c *S3Client) SetFSMeta(ctx context.Context, meta *FSMeta) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "SetFSMeta",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketname", meta.BucketName),
			attribute.String("prefix", meta.Prefix),
			attribute.String("fspath", meta.FSPath),
			attribute.String("path", path.Join(meta.Prefix, MetadataName)),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(meta)
	opts := minio.PutObjectOptions{ContentType: "application/json"}
	_, err := c.Minio.PutObject(ctx, meta.BucketName, path.Join(meta.Prefix, MetadataName), b, int64(b.Len()), opts)
	if err != nil {
		return common.HandleInternalError(err, span)
	}

	return nil
}

func (c *S3Client) GetFSMeta(ctx context.Context, bucketName, prefix string) (*FSMeta, error) {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "GetFSMeta",
		trace.WithAttributes(
			attribute.String("endpoint", c.Config.Endpoint),
			attribute.String("region", c.Config.Region),
			attribute.String("bucketName", bucketName),
			attribute.String("path", path.Join(prefix, MetadataName)),
		),
		trace.WithSpanKind(trace.SpanKindClient),
	)
	defer span.End()

	opts := minio.GetObjectOptions{}
	obj, err := c.Minio.GetObject(ctx, bucketName, path.Join(prefix, MetadataName), opts)
	if err != nil {
		return &FSMeta{}, common.HandleError(err, span)
	}

	objInfo, err := obj.Stat()
	if err != nil {
		return &FSMeta{}, common.HandleError(err, span)
	}

	span.SetAttributes(attribute.Int64("objsize", objInfo.Size))

	if objInfo.Size <= 0 {
		return &FSMeta{}, common.HandleError(fmt.Errorf("invalid size defined for object"), span)
	}

	b := make([]byte, objInfo.Size)
	_, err = obj.Read(b)

	if err != nil && err != io.EOF {
		return &FSMeta{}, common.HandleError(err, span)
	}

	var meta FSMeta
	err = json.Unmarshal(b, &meta)

	span.SetAttributes(attribute.String("fspath", meta.FSPath))

	if err != nil {
		return &FSMeta{}, common.HandleError(err, span)
	}

	return &meta, nil
}
