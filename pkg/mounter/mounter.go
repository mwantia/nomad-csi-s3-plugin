package mounter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/mitchellh/go-ps"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/common"
	"github.com/mwantia/nomad-csi-s3-plugin/pkg/s3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/utils/mount"
)

const (
	VendorVersion = "v1.2.0-rc.2"
	DriverName    = "github.com.mwantia.nomad-csi-s3-plugin"
)

type Mounter interface {
	Stage(ctx context.Context, stagePath string) error
	Unstage(ctx context.Context, stagePath string) error
	Mount(ctx context.Context, source string, target string) error
	Unmount(ctx context.Context, target string) error
}

func NewMounter(meta *s3.FSMeta, cfg *s3.S3Config) (Mounter, error) {
	mounter := meta.Mounter
	if len(mounter) <= 0 {
		mounter = cfg.Mounter
	}

	switch mounter {
	case "s3fs":
		return NewS3FSMounter(meta, cfg)
	}

	// Defaults to 's3fs'
	return NewS3FSMounter(meta, cfg)
}

func FuseMount(ctx context.Context, path, command string, args []string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "FuseMount",
		trace.WithAttributes(
			attribute.String("path", path),
			attribute.String("command", command),
			attribute.String("args", strings.Join(args, ",")),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	cmd := exec.Command(command, args...)

	log.Printf("Mounting fuse with command: %s and args: %s", command, args)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		span.RecordError(err)
		span.SetStatus(1, err.Error())

		return fmt.Errorf("error fuseMount command: %s\nargs: %s\noutput", command, args)
	}

	return WaitForMount(ctx, path, 10*time.Second)
}

func FuseUnmount(ctx context.Context, path string) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "FuseUnmount",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if err := mount.New("").Unmount(path); err != nil {
		span.RecordError(err)
		span.SetStatus(1, err.Error())

		return err
	}
	// as fuse quits immediately, we will try to wait until the process is done
	process, err := FindFuseMountProcess(ctx, path)
	if err != nil {
		span.RecordError(err)
		log.Fatalf("Error getting PID of fuse mount: %s", err)

		return nil
	}
	if process == nil {
		span.RecordError(err)
		log.Printf("Unable to find PID of fuse mount %s, it must have finished already", path)

		return nil
	}

	span.SetAttributes(attribute.Int("pid", process.Pid))

	log.Printf("Found fuse pid %v of mount %s, checking if it still runs", process.Pid, path)

	return WaitForProcess(ctx, process, 1)
}

func WaitForMount(ctx context.Context, path string, timeout time.Duration) error {
	_, span := otel.Tracer(DriverName).Start(ctx, "WaitForMount",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	var elapsed time.Duration
	interval := 10 * time.Millisecond
	for {
		notMount, err := mount.New("").IsLikelyNotMountPoint(path)
		if err != nil {
			return common.HandleInternalError(err, span)
		}
		if !notMount {
			return nil
		}

		time.Sleep(interval)
		elapsed = elapsed + interval

		if elapsed >= timeout {
			err := errors.New("timeout waiting for mount")

			return common.HandleInternalError(err, span)
		}
	}
}

func FindFuseMountProcess(ctx context.Context, path string) (*os.Process, error) {
	_, span := otel.Tracer(DriverName).Start(ctx, "FindFuseMountProcess",
		trace.WithAttributes(
			attribute.String("path", path),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	processes, err := ps.Processes()
	if err != nil {
		return nil, common.HandleInternalError(err, span)
	}
	for _, p := range processes {
		cmdLine, err := GetCmdLine(p.Pid())
		if err != nil {
			span.RecordError(err)
			log.Printf("Unable to get cmdline of PID %v: %s", p.Pid(), err)

			continue
		}
		if strings.Contains(cmdLine, path) {
			log.Printf("Found matching pid %v on path %s", p.Pid(), path)

			process, err := os.FindProcess(p.Pid())
			if err != nil {
				return nil, common.HandleInternalError(err, span)
			}

			span.SetAttributes(attribute.Int("pid", process.Pid))

			return process, err
		}
	}

	return nil, nil
}

func WaitForProcess(ctx context.Context, p *os.Process, backoff int) error {
	ctx, span := otel.Tracer(DriverName).Start(ctx, "WaitForProcess",
		trace.WithAttributes(
			attribute.Int("pid", p.Pid),
		),
		trace.WithSpanKind(trace.SpanKindInternal),
	)
	defer span.End()

	if backoff == 20 {
		err := fmt.Errorf("timeout waiting for PID %v to end", p.Pid)

		return common.HandleInternalError(err, span)
	}
	cmdLine, err := GetCmdLine(p.Pid)
	if err != nil {
		span.RecordError(err)
		log.Printf("Error checking cmdline of PID %v, assuming it is dead: %s", p.Pid, err)

		return nil
	}
	if cmdLine == "" {
		// ignore defunct processes
		// TODO: debug why this happens in the first place
		// seems to only happen on k8s, not on local docker
		span.RecordError(err)
		log.Printf("Fuse process seems dead, returning")

		return nil
	}
	if err := p.Signal(syscall.Signal(0)); err != nil {
		span.RecordError(err)
		log.Printf("Fuse process does not seem active or we are unprivileged: %s", err)

		return nil
	}

	log.Printf("Fuse process with PID %v still active, waiting...", p.Pid)
	time.Sleep(time.Duration(backoff*100) * time.Millisecond)

	return WaitForProcess(ctx, p, backoff+1)
}

func GetCmdLine(pid int) (string, error) {
	cmdLineFile := fmt.Sprintf("/proc/%v/cmdline", pid)
	cmdLine, err := os.ReadFile(cmdLineFile)
	if err != nil {
		return "", err
	}

	return string(cmdLine), nil
}
