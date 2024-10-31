package identity

import (
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common/config"
)

type IdentityServer struct {
	*csicommon.DefaultIdentityServer
	Cfg *config.DriverConfig
}
