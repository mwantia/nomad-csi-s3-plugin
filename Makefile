
```go
func TestDriver(tst *testing.T) {
	d, err := New("node-test", "unix://tmp/csi.sock")
	if err != nil {
		tst.Errorf("Error: %v", err)
	}

	ctx := context.Background()
	shutdown, _ := otel.SetupOpentelemetry(ctx, otel.OpenTelemtryConfig{
		Endpoint:     "localhost:4318",
		ServiceName:  DriverName,
		Hostname:     "node-test",
		BatchTimeout: 5 * time.Second,
		BatchSize:    10,
	})

	if err = d.Run(); err != nil {
		tst.Errorf("Unable to start driver: %v", err)
	}

	shutdown(ctx)
}
```

```go
var _ = Describe("S3Driver", func() {
	Context("s3fs", func() {
		socket := "/tmp/csi-s3fs.sock"
		endpoint := "unix://" + socket
		if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
			Expect(err).NotTo(HaveOccurred())
		}
		driver, err := New("test-node", endpoint)
		if err != nil {
			log.Fatal(err)
		}
		go driver.Run()

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
```