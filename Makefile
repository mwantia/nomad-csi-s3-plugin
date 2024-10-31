DOCKER_IMAGE := mwantia/nomad-csi-s3-plugin
DOCKER_VERSION := v1.0.4

.PHONY: all release cleanup

all: cleanup release

release:
	docker build -t $(DOCKER_IMAGE):$(DOCKER_VERSION) -t $(DOCKER_IMAGE):latest . --push

cleanup:
	umount /tmp/s3fs-target/target
	umount /tmp/s3fs-staging
	rm -rf /tmp/s3fs-*