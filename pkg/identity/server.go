package identity

import (
	csicommon "github.com/kubernetes-csi/drivers/pkg/csi-common"
)

type IdentityServer struct {
	*csicommon.DefaultIdentityServer
}
