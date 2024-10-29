package driver

import (
	"context"
	"log"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/mounter"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Nodeserver struct {
	*csicommon.DefaultNodeServer
	Volumes sync.Map
	Mutexes *common.KeyMutex
}

func (n *Nodeserver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	_, span := otel.Tracer(DriverName).Start(ctx, "NodePublishVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
			attribute.String("stagingtargetpath", req.GetStagingTargetPath()),
			attribute.String("targetpath", req.GetTargetPath()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	if req.GetVolumeCapability() == nil {
		return nil, common.HandleInvalidArgumentError("Volume capability missing in request", span)
	}

	if len(req.GetVolumeId()) == 0 {
		return nil, common.HandleInvalidArgumentError("Volume-ID missing in request", span)
	}

	if len(req.GetStagingTargetPath()) == 0 {
		return nil, common.HandleInvalidArgumentError("Staging target path missing in request", span)
	}

	if len(req.GetTargetPath()) == 0 {
		return nil, common.HandleInvalidArgumentError("Target path missing in request", span)
	}

	isMountable, err := common.CheckMount(req.GetTargetPath())
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	span.SetAttributes(attribute.Bool("checkmount", isMountable))

	if !isMountable {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	deviceID := ""
	if req.GetPublishContext() != nil {
		deviceID = req.GetPublishContext()[deviceID]
	}

	span.SetAttributes(attribute.String("deviceid", deviceID))

	mutex := n.GetVolumeMutex(req.GetVolumeId())

	mutex.Lock()
	defer mutex.Unlock()

	volume, ok := n.Volumes.Load(req.GetVolumeId())
	if !ok {
		return nil, common.HandleInternalError(err, span)
	}

	if err := volume.(*Volume).Publish(ctx, req.GetTargetPath()); err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	log.Printf("volume %s successfuly mounted to %s", req.GetVolumeId(), req.GetTargetPath())

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *Nodeserver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "NodeUnpublishVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
			attribute.String("targetpath", req.GetTargetPath()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	if len(req.GetVolumeId()) == 0 {
		return nil, common.HandleInvalidArgumentError("Volume-ID missing in request", span)
	}

	if len(req.GetTargetPath()) == 0 {
		return nil, common.HandleInvalidArgumentError("Target path missing in request", span)
	}

	mutex := n.GetVolumeMutex(req.GetVolumeId())

	mutex.Lock()
	defer mutex.Unlock()

	volume, ok := n.Volumes.Load(req.GetVolumeId())
	if !ok {
		log.Printf("volume %s hasn't been published yet", req.GetVolumeId())

		return &csi.NodeUnpublishVolumeResponse{}, nil
	}

	if err := volume.(*Volume).Unpublish(ctx, req.GetTargetPath()); err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	log.Printf("volume %s has been unpublished from %s", req.GetVolumeId(), req.GetTargetPath())

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *Nodeserver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	_, span := otel.Tracer(DriverName).Start(ctx, "NodeStageVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
			attribute.String("stagingtargetpath", req.GetStagingTargetPath()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	if req.GetVolumeCapability() == nil {
		return nil, common.HandleInvalidArgumentError("Volume capability missing in request", span)
	}

	if len(req.GetVolumeId()) == 0 {
		return nil, common.HandleInvalidArgumentError("Volume-ID missing in request", span)
	}

	if len(req.GetStagingTargetPath()) == 0 {
		return nil, common.HandleInvalidArgumentError("Staging Target path missing in request", span)
	}

	mutex := n.GetVolumeMutex(req.GetVolumeId())

	mutex.Lock()
	defer mutex.Unlock()

	isMountable, err := common.CheckMount(req.GetStagingTargetPath())
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	span.SetAttributes(attribute.Bool("checkmount", isMountable))

	if !isMountable {
		return &csi.NodeStageVolumeResponse{}, nil
	}

	minio, err := s3.NewClientFromSecret(req.GetSecrets())
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	bucketName, prefix := common.VolumeIDToBucketPrefix(req.GetVolumeId())
	meta, err := minio.GetFSMeta(ctx, bucketName, prefix)
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	mounter, err := mounter.NewMounter(meta, minio.Config)
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	volume := NewVolume(req.GetVolumeId(), mounter)

	if err := volume.Stage(ctx, req.GetStagingTargetPath()); err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	n.Volumes.Store(req.GetVolumeId(), volume)
	log.Printf("volume %s successfully staged to %s", req.GetVolumeId(), req.GetStagingTargetPath())

	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *Nodeserver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "NodeUnstageVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
			attribute.String("stagingtargetpath", req.GetStagingTargetPath()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	if len(req.GetVolumeId()) == 0 {
		return nil, common.HandleInvalidArgumentError("Volume-ID missing in request", span)
	}

	if len(req.GetStagingTargetPath()) == 0 {
		return nil, common.HandleInvalidArgumentError("Staging target path missing in request", span)
	}

	mutex := n.GetVolumeMutex(req.GetVolumeId())

	mutex.Lock()
	defer mutex.Unlock()

	volume, ok := n.Volumes.Load(req.GetVolumeId())
	if !ok {
		log.Printf("volume %s hasn't been staged yet", req.GetVolumeId())

		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	if err := volume.(*Volume).Unstage(ctx, req.GetStagingTargetPath()); err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	n.Volumes.Delete(req.GetVolumeId())
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *Nodeserver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	_, span := otel.Tracer(DriverName).Start(ctx, "NodeGetCapabilities",
		trace.WithAttributes(),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	nscap := &csi.NodeServiceCapability{
		Type: &csi.NodeServiceCapability_Rpc{
			Rpc: &csi.NodeServiceCapability_RPC{
				Type: csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
			},
		},
	}

	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: []*csi.NodeServiceCapability{
			nscap,
		},
	}, nil
}

func (n *Nodeserver) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	_, span := otel.Tracer(DriverName).Start(ctx, "NodeExpandVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	return &csi.NodeExpandVolumeResponse{}, common.HandleUnimplementedError("NodeExpandVolume", span)
}

func (n *Nodeserver) GetVolumeMutex(volumeID string) *sync.RWMutex {
	return n.Mutexes.GetMutex(volumeID)
}
