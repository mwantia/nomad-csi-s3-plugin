package driver

import (
	"context"
	"fmt"
	"log"
	"path"
	"strconv"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultFsPath = "csi-fs"
)

type ControllerServer struct {
	*csicommon.DefaultControllerServer
}

func (c *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	volumeID := common.SanitizeVolumeID(req.GetName())

	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Name missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume Capabilities missing in request")
	}

	params := req.GetParameters()
	capacityBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	mounterType := params["mounter"]

	bucketName := volumeID
	prefix := ""
	usePrefix, usePrefixError := strconv.ParseBool(params["usePrefix"])
	defaultFsPath := DefaultFsPath

	// check if bucket name is overridden
	if nameOverride, ok := params["bucket"]; ok {
		bucketName = nameOverride
		prefix = volumeID
		volumeID = path.Join(bucketName, prefix)
	}

	// check if volume prefix is overridden
	if overridePrefix := usePrefix; usePrefixError == nil && overridePrefix {
		prefix = ""
		defaultFsPath = ""
		if prefixOverride, ok := params["prefix"]; ok && prefixOverride != "" {
			prefix = prefixOverride
		}
		volumeID = path.Join(bucketName, prefix)
	}

	if err := c.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Printf("invalid create volume req: %v", req)

		return nil, err
	}

	log.Printf("Got a request to create volume %s", volumeID)

	meta := &s3.FSMeta{
		BucketName:    bucketName,
		UsePrefix:     usePrefix,
		Prefix:        prefix,
		Mounter:       mounterType,
		CapacityBytes: capacityBytes,
		FSPath:        defaultFsPath,
	}

	client, err := s3.NewClientFromSecret(req.GetSecrets())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %s", err)
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket %s exists: %v", volumeID, err)
	}

	if exists {
		// get meta, ignore errors as it could just mean meta does not exist yet
		m, err := client.GetFSMeta(ctx, bucketName, prefix)
		if err == nil {
			// Check if volume capacity requested is bigger than the already existing capacity
			if capacityBytes > m.CapacityBytes {
				return nil, status.Error(codes.AlreadyExists, fmt.Sprintf("Volume with the same name: %s but smaller size already exist", volumeID))
			}
		}
	} else {
		if err = client.CreateBucket(ctx, bucketName); err != nil {
			return nil, fmt.Errorf("failed to create bucket %s: %v", bucketName, err)
		}
	}

	if err = client.CreatePrefix(ctx, bucketName, path.Join(prefix, defaultFsPath)); err != nil && prefix != "" {
		return nil, fmt.Errorf("failed to create prefix %s: %v", path.Join(prefix, defaultFsPath), err)
	}

	if err := client.SetFSMeta(ctx, meta); err != nil {
		return nil, fmt.Errorf("error setting bucket metadata: %w", err)
	}

	log.Printf("create volume %s", volumeID)

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      volumeID,
			CapacityBytes: capacityBytes,
			VolumeContext: req.GetParameters(),
		},
	}, nil
}

func (c *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	volumeid := req.GetVolumeId()
	log.Printf("VolumeID: '%s'", volumeid)

	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID missing in request")
	}

	bucketName, prefix := common.VolumeIDToBucketPrefix(req.GetVolumeId())
	var meta *s3.FSMeta

	if err := c.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Printf("Invalid delete volume req: %v", req)

		return nil, err
	}

	log.Printf("Deleting volume %s", req.GetVolumeId())

	client, err := s3.NewClientFromSecret(req.GetSecrets())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %s", err)
	}

	if meta, err = client.GetFSMeta(ctx, bucketName, prefix); err != nil {
		log.Printf("FSMeta of volume %s does not exist, ignoring delete request", req.GetVolumeId())

		return &csi.DeleteVolumeResponse{}, nil
	}

	var deleteErr error
	if meta.UsePrefix {
		// UsePrefix is true, we do not delete anything
		log.Printf("Nothing to remove for %s", bucketName)

		return &csi.DeleteVolumeResponse{}, nil
	} else if prefix == "" {
		// prefix is empty, we delete the whole bucket
		if err := client.RemoveBucket(ctx, bucketName); err != nil {
			deleteErr = err
		}

		log.Printf("Bucket %s removed", bucketName)
	} else {
		if err := client.RemovePrefix(ctx, bucketName, prefix); err != nil {
			deleteErr = fmt.Errorf("unable to remove prefix: %w", err)
		}

		log.Printf("Prefix %s removed", prefix)
	}

	if deleteErr != nil {
		log.Printf("remove volume failed, will ensure fsmeta exists to avoid losing control over volume")
		if err := client.SetFSMeta(ctx, meta); err != nil {
			log.Fatalf("%v", err)
		}

		return nil, deleteErr
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (c *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	volcaps := req.GetVolumeCapabilities()

	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "VolumeID missing in request")
	}

	if len(volcaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	bucketName, prefix := common.VolumeIDToBucketPrefix(req.GetVolumeId())

	client, err := s3.NewClientFromSecret(req.GetSecrets())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize S3 client: %s", err)
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("bucket of volume with id %s does not exist", req.GetVolumeId()))
	}

	if _, err := client.GetFSMeta(ctx, bucketName, prefix); err != nil {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("fsmeta of volume with id %s does not exist", req.GetVolumeId()))
	}

	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	ok, err := HasVolumeCapabilitiesSupport(volcaps)
	if err != nil {
		return nil, err
	}

	if ok {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volcaps}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (c *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return &csi.ControllerExpandVolumeResponse{}, status.Error(codes.Unimplemented, fmt.Sprintf("%s is not implemented", "ControllerExpandVolume"))
}

func HasVolumeCapabilitiesSupport(volcaps []*csi.VolumeCapability) (bool, error) {
	supports := func(cap *csi.VolumeCapability) bool {
		switch cap.GetAccessType().(type) {
		case *csi.VolumeCapability_Mount:
			break
		case *csi.VolumeCapability_Block:
			return false
		default:
			return false
		}

		for _, vs := range VolumeCapabilities {
			if vs.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}

		return false
	}

	match := true
	for _, vs := range volcaps {
		if !supports(vs) {
			match = false
		}
	}

	return match, nil
}
