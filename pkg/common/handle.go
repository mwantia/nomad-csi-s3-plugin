package common

import (
	"fmt"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func HandleError(err error, span trace.Span) error {
	span.RecordError(err)
	span.SetStatus(1, err.Error())

	return err
}

func HandleErrorCode(err error, code codes.Code, span trace.Span) error {
	return HandleError(status.Error(code, err.Error()), span)
}

func HandleInternalError(err error, span trace.Span) error {
	return HandleErrorCode(err, codes.Internal, span)
}

func HandleInvalidArgumentError(msg string, span trace.Span) error {
	err := status.Error(codes.InvalidArgument, msg)

	span.RecordError(err)
	span.SetStatus(1, err.Error())

	return err
}

func HandleUnimplementedError(name string, span trace.Span) error {
	err := status.Error(codes.Unimplemented, fmt.Sprintf("%s is not implemented", name))

	span.RecordError(err)
	span.SetStatus(1, err.Error())

	return err
}
