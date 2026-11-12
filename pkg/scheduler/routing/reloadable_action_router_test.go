package routing_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-remote-execution/internal/mock"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/platform"
	"github.com/buildbarn/bb-remote-execution/pkg/scheduler/routing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestReloadableActionRouter(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
	initialActionRouter := mock.NewMockActionRouter(ctrl)
	actionRouter := routing.NewReloadableActionRouter(initialActionRouter)

	t.Run("InitialBackend", func(t *testing.T) {
		initialActionRouter.EXPECT().RouteAction(ctx, digestFunction, testutil.EqProto(t, &remoteexecution.Action{}), testutil.EqProto(t, &remoteexecution.RequestMetadata{})).
			Return(nil, platform.Key{}, nil, nil, status.Error(codes.Internal, "Routed by initial backend"))

		_, _, _, _, err := actionRouter.RouteAction(ctx, digestFunction, &remoteexecution.Action{}, &remoteexecution.RequestMetadata{})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Routed by initial backend"), err)
	})

	t.Run("ReplacedBackend", func(t *testing.T) {
		// After replacing the backend, calls must no longer
		// reach the initial backend.
		replacementActionRouter := mock.NewMockActionRouter(ctrl)
		actionRouter.Set(replacementActionRouter)

		replacementActionRouter.EXPECT().RouteAction(ctx, digestFunction, testutil.EqProto(t, &remoteexecution.Action{}), testutil.EqProto(t, &remoteexecution.RequestMetadata{})).
			Return(nil, platform.Key{}, nil, nil, status.Error(codes.Internal, "Routed by replacement backend"))

		_, _, _, _, err := actionRouter.RouteAction(ctx, digestFunction, &remoteexecution.Action{}, &remoteexecution.RequestMetadata{})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Routed by replacement backend"), err)
	})
}
