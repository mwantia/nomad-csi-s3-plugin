package driver_test

import (
	"context"
	"log"
	"os"

	"github.com/mwantia/nomad-csi-s3-plugin/pkg/driver"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/kubernetes-csi/csi-test/pkg/sanity"
)

var _ = Describe("S3Driver", func() {
	Context("s3fs", func() {
		socket := "/tmp/csi.sock"
		endpoint := "unix://" + socket
		ctx := context.Background()

		if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		d, err := driver.New("node-test", endpoint)
		if err != nil {
			log.Fatal(err)
		}

		go d.Run(ctx)

		Describe("CSI sanity", func() {
			sanityCfg := &sanity.Config{
				TargetPath:  os.TempDir() + "/s3fs-target",
				StagingPath: os.TempDir() + "/s3fs-staging",
				Address:     endpoint,
				SecretsFile: "../../test/secret.yaml",
				TestVolumeParameters: map[string]string{
					"mounter": "s3fs",
					"bucket":  "testbucket1",
				},
			}
			sanity.GinkgoTest(sanityCfg)
		})
	})
})
