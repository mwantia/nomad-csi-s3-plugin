package driver

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDriver(tst *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(tst, "S3Driver")
	time.Sleep(time.Second * 5)
}
