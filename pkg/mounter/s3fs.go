package mounter

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path"

	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/utils/mount"
)

type S3FSMounter struct {
	Meta *s3.FSMeta
	Cfg  *s3.S3Config
}

func NewS3FSMounter(meta *s3.FSMeta, cfg *s3.S3Config) (Mounter, error) {
	return &S3FSMounter{
		Meta: meta,
		Cfg:  cfg,
	}, nil
}

// Stage implements Mounter.
func (s *S3FSMounter) Stage(ctx context.Context, stagePath string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "Stage",
		trace.WithAttributes(
			attribute.String("type", "s3fs"),
			attribute.String("stagepath", stagePath),
			attribute.String("endpoint", s.Cfg.Endpoint),
			attribute.String("region", s.Cfg.Region),
			attribute.String("bucketname", s.Meta.BucketName),
			attribute.String("prefix", s.Meta.Prefix),
			attribute.String("fspath", s.Meta.FSPath),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	passfile, err := WriteS3FSPassFile(s.Cfg.AccessKeyID + ":" + s.Cfg.SecretAccessKey)
	if err != nil {
		return common.HandleInternalError(err, span)
	}

	span.SetAttributes(attribute.String("passfile", passfile))

	region := s.Cfg.Region
	if len(region) <= 0 {
		region = "us-east-1"
	}

	return FuseMount(ctx, stagePath, "s3fs", []string{
		fmt.Sprintf("%s:/%s", s.Meta.BucketName, path.Join(s.Meta.Prefix, s.Meta.FSPath)),
		stagePath,
		"-o", fmt.Sprintf("url=%s", s.Cfg.Endpoint),
		"-o", fmt.Sprintf("endpoint=%s", region),
		"-o", fmt.Sprintf("passwd_file=%s", passfile),
		"-o", "use_path_request_style",
		"-o", "allow_other",
		"-o", "mp_umask=000",
	})
}

// Unstage implements Mounter.
func (s *S3FSMounter) Unstage(ctx context.Context, stagePath string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "Unstage",
		trace.WithAttributes(
			attribute.String("type", "s3fs"),
			attribute.String("stagepath", stagePath),
			attribute.String("endpoint", s.Cfg.Endpoint),
			attribute.String("region", s.Cfg.Region),
			attribute.String("bucketname", s.Meta.BucketName),
			attribute.String("prefix", s.Meta.Prefix),
			attribute.String("fspath", s.Meta.FSPath),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if err := FuseUnmount(ctx, stagePath); err != nil {
		return common.HandleInternalError(err, span)
	}

	if err := os.Remove(stagePath); err != nil && !os.IsNotExist(err) {
		return common.HandleInternalError(err, span)
	}

	return nil
}

func (s *S3FSMounter) Mount(ctx context.Context, source string, target string) error {
	_, span := otel.Tracer(DriverName).Start(ctx, "Mount",
		trace.WithAttributes(
			attribute.String("type", "s3fs"),
			attribute.String("source", source),
			attribute.String("target", target),
			attribute.String("endpoint", s.Cfg.Endpoint),
			attribute.String("region", s.Cfg.Region),
			attribute.String("bucketname", s.Meta.BucketName),
			attribute.String("prefix", s.Meta.Prefix),
			attribute.String("fspath", s.Meta.FSPath),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if err := mount.New("").Mount(source, target, "", []string{"bind"}); err != nil {
		return common.HandleInternalError(err, span)
	}

	return nil
}

func (s *S3FSMounter) Unmount(ctx context.Context, target string) error {
	_, span := otel.Tracer(DriverName).Start(ctx, "Unmount",
		trace.WithAttributes(
			attribute.String("type", "s3fs"),
			attribute.String("target", target),
			attribute.String("endpoint", s.Cfg.Endpoint),
			attribute.String("region", s.Cfg.Region),
			attribute.String("bucketname", s.Meta.BucketName),
			attribute.String("prefix", s.Meta.Prefix),
			attribute.String("fspath", s.Meta.FSPath),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if err := mount.CleanupMountPoint(target, mount.New(""), true); err != nil {
		return common.HandleInternalError(err, span)
	}

	return nil
}

func WriteS3FSPassFile(content string) (string, error) {
	root := os.Getenv("HOME")
	// If home is unavailable (for whatever reason) fallback to tmp
	if len(root) <= 0 {
		root = "/tmp"
	}

	filename := base64.StdEncoding.EncodeToString([]byte(content))
	passfile := path.Join(root, filename)
	pwFile, err := os.OpenFile(passfile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	_, err = pwFile.WriteString(content)
	if err != nil {
		return "", err
	}

	pwFile.Close()

	return passfile, nil
}
