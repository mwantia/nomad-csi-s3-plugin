package driver

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDriver(tst *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(tst, "S3Driver")
}
