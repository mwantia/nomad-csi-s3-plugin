package driver

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/glog"
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
)

type Driver struct {
	Driver           *csicommon.CSIDriver
	Endpoint         string
	IdentityServer   *IdentityServer
	NodeServer       *Nodeserver
	ControllerServer *ControllerServer
}

var (
	VendorVersion = "v1.2.0-rc.2"
	DriverName    = "github.com.mwantia.nomad-csi-s3-plugin"
)

func New(node string, endpoint string) (*Driver, error) {
	d := csicommon.NewCSIDriver(DriverName, VendorVersion, node)
	if d == nil {
		glog.Fatalln("Failed to initialize CSI driver '%s' with version '%s' on node '%s' and endpoint '%s'",
			DriverName, VendorVersion, node, endpoint)
	}

	return &Driver{
		Endpoint: endpoint,
		Driver:   d,
	}, nil
}

func (d *Driver) NewIdentityServer() *IdentityServer {
	return &IdentityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d.Driver),
	}
}

func (d *Driver) NewControllerServer() *ControllerServer {
	return &ControllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d.Driver),
	}
}

func (d *Driver) NewNodeServer() *Nodeserver {
	return &Nodeserver{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d.Driver),
	}
}

func (d *Driver) Run() {
	glog.Info("Driver: %s", DriverName)
	glog.Info("Version: %s", VendorVersion)

	d.Driver.AddControllerServiceCapabilities([]csi.ControllerServiceCapability_RPC_Type{csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME})
	d.Driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	d.IdentityServer = d.NewIdentityServer()
	d.NodeServer = d.NewNodeServer()
	d.ControllerServer = d.NewControllerServer()

	server := csicommon.NewNonBlockingGRPCServer()
	server.Start(d.Endpoint, d.IdentityServer, d.ControllerServer, d.NodeServer)
	server.Wait()
}
