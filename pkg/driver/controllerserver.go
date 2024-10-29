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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	ctx, span := otel.Tracer(DriverName).Start(ctx, "CreateVolume",
		trace.WithAttributes(
			attribute.String("name", req.GetName()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	volumeID := common.SanitizeVolumeID(req.GetName())

	if len(volumeID) == 0 {
		return nil, common.HandleInvalidArgumentError("Name missing in request", span)
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, common.HandleInvalidArgumentError("Volume Capabilities missing in request", span)
	}

	params := req.GetParameters()
	capacityBytes := int64(req.GetCapacityRange().GetRequiredBytes())
	mounterType := params["mounter"]

	bucketName := volumeID
	prefix := ""
	usePrefix, usePrefixError := strconv.ParseBool(params["usePrefix"])
	defaultFsPath := "csi-fs"

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
		return nil, common.HandleInternalError(err, span)
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
		err := fmt.Errorf("failed to initialize S3 client: %s", err)

		return nil, common.HandleInternalError(err, span)
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		err := fmt.Errorf("failed to check if bucket %s exists: %v", volumeID, err)

		return nil, common.HandleInternalError(err, span)
	}

	if exists {
		// get meta, ignore errors as it could just mean meta does not exist yet
		m, err := client.GetFSMeta(ctx, bucketName, prefix)
		if err == nil {
			// Check if volume capacity requested is bigger than the already existing capacity
			if capacityBytes > m.CapacityBytes {
				err := status.Error(codes.AlreadyExists, fmt.Sprintf("Volume with the same name: %s but smaller size already exist", volumeID))

				return nil, common.HandleInternalError(err, span)
			}
		}
	} else {
		if err = client.CreateBucket(ctx, bucketName); err != nil {
			err := fmt.Errorf("failed to create bucket %s: %v", bucketName, err)

			return nil, common.HandleInternalError(err, span)
		}
	}

	if err = client.CreatePrefix(ctx, bucketName, path.Join(prefix, defaultFsPath)); err != nil && prefix != "" {
		err := fmt.Errorf("failed to create prefix %s: %v", path.Join(prefix, defaultFsPath), err)
		return nil, common.HandleInternalError(err, span)
	}

	if err := client.SetFSMeta(ctx, meta); err != nil {
		err := fmt.Errorf("error setting bucket metadata: %w", err)

		return nil, common.HandleInternalError(err, span)
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
	ctx, span := otel.Tracer(DriverName).Start(ctx, "DeleteVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	if len(req.GetVolumeId()) == 0 {
		return nil, common.HandleInvalidArgumentError("Volume ID missing in request", span)
	}

	bucketName, prefix := common.VolumeIDToBucketPrefix(req.GetVolumeId())
	var meta *s3.FSMeta

	if err := c.Driver.ValidateControllerServiceRequest(csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME); err != nil {
		log.Printf("Invalid delete volume req: %v", req)

		return nil, common.HandleInternalError(err, span)
	}

	log.Printf("Deleting volume %s", req.GetVolumeId())

	client, err := s3.NewClientFromSecret(req.GetSecrets())
	if err != nil {
		err := fmt.Errorf("failed to initialize S3 client: %s", err)

		return nil, common.HandleInternalError(err, span)
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

		return nil, common.HandleInternalError(deleteErr, span)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (c *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "ValidateVolumeCapabilities",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	if len(req.GetVolumeId()) == 0 {
		return nil, common.HandleInvalidArgumentError("Volume ID missing in request", span)
	}

	if req.GetVolumeCapabilities() == nil {
		return nil, common.HandleInvalidArgumentError("Volume capabilities missing in request", span)
	}

	bucketName, prefix := common.VolumeIDToBucketPrefix(req.GetVolumeId())

	span.SetAttributes(
		attribute.String("bucketname", bucketName),
		attribute.String("prefix", prefix),
	)

	client, err := s3.NewClientFromSecret(req.GetSecrets())
	if err != nil {
		err := fmt.Errorf("failed to initialize S3 client: %s", err)

		return nil, common.HandleInternalError(err, span)
	}

	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}

	span.SetAttributes(attribute.Bool("exists", exists))

	if !exists {
		err := status.Error(codes.NotFound, fmt.Sprintf("bucket of volume with id %s does not exist", req.GetVolumeId()))

		return nil, common.HandleInternalError(err, span)
	}

	if _, err := client.GetFSMeta(ctx, bucketName, prefix); err != nil {
		err := status.Error(codes.NotFound, fmt.Sprintf("fsmeta of volume with id %s does not exist", req.GetVolumeId()))

		return nil, common.HandleInternalError(err, span)
	}

	// We currently only support RWO
	supportedAccessMode := &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
	}

	for _, capability := range req.VolumeCapabilities {
		if capability.GetAccessMode().GetMode() != supportedAccessMode.GetMode() {
			return &csi.ValidateVolumeCapabilitiesResponse{Message: "Only single node writer is supported"}, nil
		}
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: []*csi.VolumeCapability{
				{
					AccessMode: supportedAccessMode,
				},
			},
		},
	}, nil
}

func (c *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	_, span := otel.Tracer(DriverName).Start(ctx, "ControllerExpandVolume",
		trace.WithAttributes(
			attribute.String("volumeid", req.GetVolumeId()),
		),
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	return &csi.ControllerExpandVolumeResponse{}, common.HandleUnimplementedError("ControllerExpandVolume", span)
}
