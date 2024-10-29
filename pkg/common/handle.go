package common

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func HandleInvalidArgumentError(msg string, span trace.Span) error {
	err := status.Error(codes.InvalidArgument, msg)
	if span != nil {
		span.RecordError(err)
		span.SetStatus(1, err.Error())
	}
	return err
}

func HandleInternalError(err error, span trace.Span) error {
	innerErr := status.Error(codes.Internal, err.Error())
	span.RecordError(innerErr)
	span.SetStatus(1, innerErr.Error())
	return innerErr
}

func HandleUnimplementedError(name string, span trace.Span) error {
	err := status.Error(codes.Unimplemented, fmt.Sprintf("%s is not implemented", name))
	span.RecordError(err)
	span.SetStatus(1, err.Error())
	return err
}
