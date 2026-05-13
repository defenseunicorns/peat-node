// Copyright 2026 Defense Unicorns
// SPDX-License-Identifier: Apache-2.0

package peat

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	sidecarv1 "github.com/defenseunicorns/peat-node/sdk/go/gen/peat/sidecar/v1"
	"github.com/defenseunicorns/peat-node/sdk/go/gen/peat/sidecar/v1/sidecarv1connect"
)

// testSidecarClient is a minimal PeatSidecarClient implementation that returns
// CodeUnimplemented from every method. Override individual methods by embedding
// and promoting in a wrapper struct.
type testSidecarClient struct{}

func (t *testSidecarClient) GetStatus(_ context.Context, _ *connect.Request[sidecarv1.GetStatusRequest]) (*connect.Response[sidecarv1.GetStatusResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) ConnectPeer(_ context.Context, _ *connect.Request[sidecarv1.ConnectPeerRequest]) (*connect.Response[sidecarv1.ConnectPeerResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) DisconnectPeer(_ context.Context, _ *connect.Request[sidecarv1.DisconnectPeerRequest]) (*connect.Response[sidecarv1.DisconnectPeerResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) ListPeers(_ context.Context, _ *connect.Request[sidecarv1.ListPeersRequest]) (*connect.Response[sidecarv1.ListPeersResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) PutDocument(_ context.Context, _ *connect.Request[sidecarv1.PutDocumentRequest]) (*connect.Response[sidecarv1.PutDocumentResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetDocument(_ context.Context, _ *connect.Request[sidecarv1.GetDocumentRequest]) (*connect.Response[sidecarv1.GetDocumentResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) DeleteDocument(_ context.Context, _ *connect.Request[sidecarv1.DeleteDocumentRequest]) (*connect.Response[sidecarv1.DeleteDocumentResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) ListDocuments(_ context.Context, _ *connect.Request[sidecarv1.ListDocumentsRequest]) (*connect.Response[sidecarv1.ListDocumentsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) PutPlatform(_ context.Context, _ *connect.Request[sidecarv1.PutPlatformRequest]) (*connect.Response[sidecarv1.PutPlatformResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetPlatforms(_ context.Context, _ *connect.Request[sidecarv1.GetPlatformsRequest]) (*connect.Response[sidecarv1.GetPlatformsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) PutCell(_ context.Context, _ *connect.Request[sidecarv1.PutCellRequest]) (*connect.Response[sidecarv1.PutCellResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetCells(_ context.Context, _ *connect.Request[sidecarv1.GetCellsRequest]) (*connect.Response[sidecarv1.GetCellsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) PutTrack(_ context.Context, _ *connect.Request[sidecarv1.PutTrackRequest]) (*connect.Response[sidecarv1.PutTrackResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetTracks(_ context.Context, _ *connect.Request[sidecarv1.GetTracksRequest]) (*connect.Response[sidecarv1.GetTracksResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) PutCommand(_ context.Context, _ *connect.Request[sidecarv1.PutCommandRequest]) (*connect.Response[sidecarv1.PutCommandResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetCommands(_ context.Context, _ *connect.Request[sidecarv1.GetCommandsRequest]) (*connect.Response[sidecarv1.GetCommandsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) Subscribe(_ context.Context, _ *connect.Request[sidecarv1.SubscribeRequest]) (*connect.ServerStreamForClient[sidecarv1.DocumentChange], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) StartSync(_ context.Context, _ *connect.Request[sidecarv1.StartSyncRequest]) (*connect.Response[sidecarv1.StartSyncResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) StopSync(_ context.Context, _ *connect.Request[sidecarv1.StopSyncRequest]) (*connect.Response[sidecarv1.StopSyncResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetSyncStats(_ context.Context, _ *connect.Request[sidecarv1.GetSyncStatsRequest]) (*connect.Response[sidecarv1.GetSyncStatsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) PublishDeployment(_ context.Context, _ *connect.Request[sidecarv1.PublishDeploymentRequest]) (*connect.Response[sidecarv1.PublishDeploymentResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) GetDeploymentRequests(_ context.Context, _ *connect.Request[sidecarv1.GetDeploymentRequestsRequest]) (*connect.Response[sidecarv1.GetDeploymentRequestsResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}
func (t *testSidecarClient) ResetDeployment(_ context.Context, _ *connect.Request[sidecarv1.ResetDeploymentRequest]) (*connect.Response[sidecarv1.ResetDeploymentResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, nil)
}

// compile-time assertion that testSidecarClient implements PeatSidecarClient.
var _ sidecarv1connect.PeatSidecarClient = (*testSidecarClient)(nil)

// fakeSidecar records the last request for each deployment method and returns a canned response.
type fakeSidecar struct {
	testSidecarClient
	lastPublish *sidecarv1.PublishDeploymentRequest
	lastReset   *sidecarv1.ResetDeploymentRequest
	docs        []*sidecarv1.DeploymentRequestDoc
}

func (f *fakeSidecar) PublishDeployment(_ context.Context, req *connect.Request[sidecarv1.PublishDeploymentRequest]) (*connect.Response[sidecarv1.PublishDeploymentResponse], error) {
	f.lastPublish = req.Msg
	return connect.NewResponse(&sidecarv1.PublishDeploymentResponse{RequestId: "uuid-123"}), nil
}

func (f *fakeSidecar) GetDeploymentRequests(_ context.Context, _ *connect.Request[sidecarv1.GetDeploymentRequestsRequest]) (*connect.Response[sidecarv1.GetDeploymentRequestsResponse], error) {
	return connect.NewResponse(&sidecarv1.GetDeploymentRequestsResponse{Requests: f.docs}), nil
}

func (f *fakeSidecar) ResetDeployment(_ context.Context, req *connect.Request[sidecarv1.ResetDeploymentRequest]) (*connect.Response[sidecarv1.ResetDeploymentResponse], error) {
	f.lastReset = req.Msg
	return connect.NewResponse(&sidecarv1.ResetDeploymentResponse{}), nil
}

func TestDeploymentWrappers(t *testing.T) {
	fake := &fakeSidecar{docs: []*sidecarv1.DeploymentRequestDoc{{Id: "doc-1", ReceiverStatus: "pending"}}}
	c := &Client{sidecar: fake}

	t.Run("PublishDeployment forwards all fields and returns request_id", func(t *testing.T) {
		id, err := c.PublishDeployment(context.Background(), "/tmp/pkg.tar.zst", "peer-xyz", map[string]string{"FOO": "bar"}, "1.2.3", "arm64")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if id != "uuid-123" {
			t.Errorf("want request_id uuid-123, got %q", id)
		}
		if fake.lastPublish.PackagePath != "/tmp/pkg.tar.zst" {
			t.Errorf("package_path not forwarded")
		}
		if fake.lastPublish.TargetAgentId != "peer-xyz" {
			t.Errorf("target_agent_id not forwarded")
		}
		if fake.lastPublish.ZarfVars["FOO"] != "bar" {
			t.Errorf("zarf_vars not forwarded")
		}
		if fake.lastPublish.PackageVersion != "1.2.3" {
			t.Errorf("package_version not forwarded")
		}
		if fake.lastPublish.Architecture != "arm64" {
			t.Errorf("architecture not forwarded")
		}
	})

	t.Run("GetDeploymentRequests returns docs from response", func(t *testing.T) {
		docs, err := c.GetDeploymentRequests(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(docs) != 1 || docs[0].Id != "doc-1" {
			t.Errorf("docs not returned verbatim: %+v", docs)
		}
	})

	t.Run("ResetDeployment forwards request_id", func(t *testing.T) {
		if err := c.ResetDeployment(context.Background(), "doc-1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fake.lastReset.RequestId != "doc-1" {
			t.Errorf("request_id not forwarded: got %q", fake.lastReset.RequestId)
		}
	})
}
