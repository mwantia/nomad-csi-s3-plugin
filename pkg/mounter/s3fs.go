package mounter

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path"

	cmount "github.com/mwantia/nomad-csi-s3-plugin/pkg/common/mount"

	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
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
	passfile, err := WriteS3FSPassFile(s.Cfg.AccessKeyID + ":" + s.Cfg.SecretAccessKey)
	if err != nil {
		return err
	}

	return FuseMount(ctx, stagePath, "s3fs", []string{
		fmt.Sprintf("%s:/%s", s.Meta.BucketName, path.Join(s.Meta.Prefix, s.Meta.FSPath)),
		stagePath,
		"-o", fmt.Sprintf("url=%s", s.Cfg.Endpoint),
		"-o", fmt.Sprintf("endpoint=%s", s.Cfg.Region),
		"-o", fmt.Sprintf("passwd_file=%s", passfile),
		"-o", "use_path_request_style",
		"-o", "allow_other",
		"-o", "mp_umask=000",
	})
}

// Unstage implements Mounter.
func (s *S3FSMounter) Unstage(ctx context.Context, stagePath string) error {
	if err := FuseUnmount(ctx, stagePath); err != nil {
		return err
	}

	if err := os.Remove(stagePath); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (s *S3FSMounter) Mount(ctx context.Context, source string, target string) error {
	return cmount.BindFSMount(ctx, source, target)
	// return FuseMount(ctx, target, "bindfs", []string{ source,target})
}

func (s *S3FSMounter) Unmount(ctx context.Context, target string) error {
	mounted, err := cmount.IsBindMounted(target)
	if err != nil {
		return fmt.Errorf("failed to check mount type: %w", err)
	}

	if mounted {
		if err := cmount.UnmountBindFS(ctx, target); err != nil {
			return fmt.Errorf("failed to unmount bindfs: %w", err)
		}
		return nil
	}

	if err := mount.CleanupMountPoint(target, mount.New(""), true); err != nil {
		return err
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
