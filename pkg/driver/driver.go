package driver

import (
	"context"
	"log"

	"github.com/container-storage-interface/spec/lib/go/csi"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/controller"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/identity"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/node"
)

type Driver struct {
	Driver           *csicommon.CSIDriver
	Endpoint         string
	IdentityServer   *identity.IdentityServer
	NodeServer       *node.Nodeserver
	ControllerServer *controller.ControllerServer
}

var (
	VendorVersion = "v1.0.1"
	DriverName    = "github.com.mwantia.nomad-csi-s3-plugin"
)

func New(node string, endpoint string) (*Driver, error) {
	d := csicommon.NewCSIDriver(DriverName, VendorVersion, node)
	if d == nil {
		log.Fatalf("Failed to initialize CSI driver '%s' with version '%s' on node '%s' and endpoint '%s'",
			DriverName, VendorVersion, node, endpoint)
	}

	return &Driver{
		Endpoint: endpoint,
		Driver:   d,
	}, nil
}

func (d *Driver) NewIdentityServer() *identity.IdentityServer {
	return &identity.IdentityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d.Driver),
	}
}

func (d *Driver) NewControllerServer() *controller.ControllerServer {
	return &controller.ControllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d.Driver),
	}
}

func (d *Driver) NewNodeServer() *node.Nodeserver {
	return &node.Nodeserver{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.Driver),
		Mutexes:           common.NewKeyMutex(32),
	}
}

func (d *Driver) Run(ctx context.Context) error {
	log.Printf("Driver: %s", DriverName)
	log.Printf("Version: %s", VendorVersion)

	d.Driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	})
	d.Driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{
		csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER,
	})

	d.IdentityServer = d.NewIdentityServer()
	d.NodeServer = d.NewNodeServer()
	d.ControllerServer = d.NewControllerServer()

	server := csicommon.NewNonBlockingGRPCServer()
	server.Start(d.Endpoint, d.IdentityServer, d.ControllerServer, d.NodeServer)
	server.Wait()

	return nil
}
