package driver

import (
	"context"

	"github.com/golang/glog"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/mounter"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Volume struct {
	VolumeId string
	// volume's real mount point
	stagingTargetPath string
	// Target paths to which the volume has been published.
	// These paths are symbolic links to the real mount point.
	// So multiple pods using the same volume can share a mount.
	targetPaths map[string]bool
	mounter     mounter.Mounter
}

func NewVolume(volumeID string, mounter mounter.Mounter) *Volume {
	return &Volume{
		VolumeId:    volumeID,
		mounter:     mounter,
		targetPaths: make(map[string]bool),
	}
}

func (vol *Volume) Stage(ctx context.Context, path string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "Stage",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	staged := vol.IsStaged()
	span.SetAttributes(attribute.Bool("staged", staged))

	if staged {
		return nil
	}

	if err := vol.mounter.Stage(ctx, path); err != nil {
		return common.HandleError(err, span)
	}

	vol.stagingTargetPath = path
	return nil
}

func (vol *Volume) Unstage(ctx context.Context, path string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "Unstage",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	staged := vol.IsStaged()
	span.SetAttributes(attribute.Bool("staged", staged))

	if !staged {
		return nil
	}

	if err := vol.mounter.Unstage(ctx, vol.stagingTargetPath); err != nil {
		return common.HandleError(err, span)
	}

	return nil
}

func (vol *Volume) Publish(ctx context.Context, path string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "Publish",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if err := vol.mounter.Mount(ctx, vol.stagingTargetPath, path); err != nil {
		return common.HandleError(err, span)
	}

	vol.targetPaths[path] = true
	return nil
}

func (vol *Volume) Unpublish(ctx context.Context, path string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "Unpublish",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if _, ok := vol.targetPaths[path]; !ok {
		glog.Warningf("volume %s hasn't been published to %s", vol.VolumeId, path)
		return nil
	}

	if err := vol.mounter.Unmount(ctx, path); err != nil {
		return common.HandleError(err, span)
	}

	delete(vol.targetPaths, path)
	return nil
}

func (vol *Volume) IsStaged() bool {
	return vol.stagingTargetPath != ""
}
