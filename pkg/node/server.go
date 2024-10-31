package node

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common/config"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common/mount"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/mounter"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Nodeserver struct {
	*csicommon.DefaultNodeServer
	Cfg        *config.DriverConfig
	Volumes    sync.Map
	Mutexes    *common.KeyMutex
	Verifiers  map[string]*mount.MountVerifier
	VerifierMu sync.Mutex
}

func (n *Nodeserver) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID missing in request")
	}

	if len(req.GetStagingTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}

	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	isMountable, err := common.CheckMount(req.GetTargetPath())
	if err != nil {
		return nil, err
	}

	if !isMountable {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	deviceID := ""
	if req.GetPublishContext() != nil {
		deviceID = req.GetPublishContext()[deviceID]
	}

	log.Printf("DeviceID: %s", deviceID)

	mutex := n.GetVolumeMutex(req.GetVolumeId())

	mutex.Lock()
	defer mutex.Unlock()

	volume, ok := n.Volumes.Load(req.GetVolumeId())
	if !ok {
		return nil, err
	}

	if err := volume.(*Volume).Publish(ctx, req.GetTargetPath()); err != nil {
		return nil, err
	}

	log.Printf("volume %s successfuly mounted to %s", req.GetVolumeId(), req.GetTargetPath())

	return &csi.NodePublishVolumeResponse{}, nil
}

func (n *Nodeserver) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID missing in request")
	}

	if len(req.GetTargetPath()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
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
		return nil, err
	}

	log.Printf("volume %s has been unpublished from %s", req.GetVolumeId(), req.GetTargetPath())

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (n *Nodeserver) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}

	volumeid := req.GetVolumeId()
	stagingpath := req.GetStagingTargetPath()

	if len(volumeid) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID missing in request")
	}

	if len(stagingpath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}

	mutex := n.GetVolumeMutex(volumeid)

	mutex.Lock()
	defer mutex.Unlock()

	isMountable, err := common.CheckMount(stagingpath)
	if err != nil {
		return nil, err
	}

	if !isMountable {
		return &csi.NodeStageVolumeResponse{}, nil
	}

	minio, err := s3.CreateClient(n.Cfg, req.GetSecrets())
	if err != nil {
		return nil, err
	}

	bucketName, prefix := common.VolumeIDToBucketPrefix(volumeid)
	meta, err := minio.GetFSMeta(ctx, bucketName, prefix)
	if err != nil {
		return nil, err
	}

	mounter, err := mounter.NewMounter(meta, minio.Config)
	if err != nil {
		return nil, err
	}

	volume := NewVolume(volumeid, mounter)

	if err := volume.Stage(ctx, stagingpath); err != nil {
		return nil, err
	}

	n.Volumes.Store(volumeid, volume)
	log.Printf("volume %s successfully staged to %s", volumeid, stagingpath)

	return &csi.NodeStageVolumeResponse{}, nil
}

func (n *Nodeserver) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	volumeid := req.GetVolumeId()
	stagingpath := req.GetStagingTargetPath()

	if len(volumeid) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID missing in request")
	}

	if len(stagingpath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Staging target path missing in request")
	}

	mutex := n.GetVolumeMutex(volumeid)

	mutex.Lock()
	defer mutex.Unlock()

	volume, ok := n.Volumes.Load(volumeid)
	if !ok {
		log.Printf("volume %s hasn't been staged yet", volumeid)

		return &csi.NodeUnstageVolumeResponse{}, nil
	}

	if err := volume.(*Volume).Unstage(ctx, stagingpath); err != nil {
		return nil, err
	}

	n.Volumes.Delete(volumeid)

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (n *Nodeserver) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
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
	return &csi.NodeExpandVolumeResponse{}, status.Error(codes.Unimplemented, fmt.Sprintf("%s is not implemented", "NodeExpandVolume"))
}

func (n *Nodeserver) GetVolumeMutex(volumeID string) *sync.RWMutex {
	return n.Mutexes.GetMutex(volumeID)
}
