// Package peat provides an idiomatic Go client for the peat-node Connect RPC API.
//
// The client communicates with a co-located peat-node process over Unix
// socket or TCP using the Connect protocol (HTTP+proto). The node is a
// full CRDT mesh participant; this client provides a thin, pure-Go wrapper
// with channels for subscriptions and context for cancellation.
//
// Usage:
//
//	client, err := peat.Connect("http://localhost:50051")
//	// or for Unix socket:
//	client, err := peat.Connect("unix:///var/run/peat.sock")
package peat

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	sidecarv1 "github.com/defenseunicorns/peat-node/test/go/gen/peat/sidecar/v1"
	"github.com/defenseunicorns/peat-node/test/go/gen/peat/sidecar/v1/sidecarv1connect"
	"golang.org/x/net/http2"
)

// Client connects to a peat-node instance over Unix socket or TCP.
type Client struct {
	sidecar sidecarv1connect.PeatSidecarClient
}

// Connect creates a new Client connected to the peat-node at the given
// address. Use "unix:///var/run/peat.sock" for Unix sockets or
// "http://localhost:50051" for TCP.
func Connect(target string) (*Client, error) {
	var httpClient *http.Client

	if strings.HasPrefix(target, "unix://") {
		socketPath := strings.TrimPrefix(target, "unix://")
		httpClient = &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(_ context.Context, _, _ string, _ *tls.Config) (net.Conn, error) {
					return net.Dial("unix", socketPath)
				},
			},
		}
		target = "http://localhost"
	} else {
		httpClient = http.DefaultClient
	}

	client := sidecarv1connect.NewPeatSidecarClient(
		httpClient,
		target,
	)
	return &Client{sidecar: client}, nil
}

// --- Lifecycle ---

// Status returns the current state of the sidecar node.
func (c *Client) Status(ctx context.Context) (*sidecarv1.GetStatusResponse, error) {
	resp, err := c.sidecar.GetStatus(ctx, connect.NewRequest(&sidecarv1.GetStatusRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

// --- Peer Management ---

// ConnectPeer establishes an authenticated connection to a mesh peer.
// At least one of addresses or relayURL must be non-empty.
func (c *Client) ConnectPeer(ctx context.Context, endpointID string, addresses []string, relayURL string) error {
	_, err := c.sidecar.ConnectPeer(ctx, connect.NewRequest(&sidecarv1.ConnectPeerRequest{
		EndpointId: endpointID,
		Addresses:  addresses,
		RelayUrl:   relayURL,
	}))
	return err
}

// DisconnectPeer drops the connection to a peer.
func (c *Client) DisconnectPeer(ctx context.Context, endpointID string) error {
	_, err := c.sidecar.DisconnectPeer(ctx, connect.NewRequest(&sidecarv1.DisconnectPeerRequest{
		EndpointId: endpointID,
	}))
	return err
}

// ListPeers returns currently connected peers.
func (c *Client) ListPeers(ctx context.Context) ([]*sidecarv1.PeerInfo, error) {
	resp, err := c.sidecar.ListPeers(ctx, connect.NewRequest(&sidecarv1.ListPeersRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.Peers, nil
}

// --- Generic Document CRUD ---

// PutDocument creates or updates a document in a collection.
func (c *Client) PutDocument(ctx context.Context, collection, docID, jsonData string) error {
	_, err := c.sidecar.PutDocument(ctx, connect.NewRequest(&sidecarv1.PutDocumentRequest{
		Collection: collection,
		DocId:      docID,
		JsonData:   jsonData,
	}))
	return err
}

// GetDocument retrieves a document by collection and ID. Returns nil if not found.
func (c *Client) GetDocument(ctx context.Context, collection, docID string) (*string, error) {
	resp, err := c.sidecar.GetDocument(ctx, connect.NewRequest(&sidecarv1.GetDocumentRequest{
		Collection: collection,
		DocId:      docID,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.JsonData, nil
}

// DeleteDocument removes a document from a collection.
func (c *Client) DeleteDocument(ctx context.Context, collection, docID string) error {
	_, err := c.sidecar.DeleteDocument(ctx, connect.NewRequest(&sidecarv1.DeleteDocumentRequest{
		Collection: collection,
		DocId:      docID,
	}))
	return err
}

// ListDocuments lists all document IDs in a collection.
func (c *Client) ListDocuments(ctx context.Context, collection string) ([]string, error) {
	resp, err := c.sidecar.ListDocuments(ctx, connect.NewRequest(&sidecarv1.ListDocumentsRequest{
		Collection: collection,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.DocIds, nil
}

// --- Typed Collections ---

// PutPlatform creates or updates a platform in the mesh.
func (c *Client) PutPlatform(ctx context.Context, platform *sidecarv1.Platform) error {
	_, err := c.sidecar.PutPlatform(ctx, connect.NewRequest(&sidecarv1.PutPlatformRequest{
		Platform: platform,
	}))
	return err
}

// GetPlatforms returns all platforms in the mesh.
func (c *Client) GetPlatforms(ctx context.Context) ([]*sidecarv1.Platform, error) {
	resp, err := c.sidecar.GetPlatforms(ctx, connect.NewRequest(&sidecarv1.GetPlatformsRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.Platforms, nil
}

// PutCell creates or updates a cell.
func (c *Client) PutCell(ctx context.Context, cell *sidecarv1.Cell) error {
	_, err := c.sidecar.PutCell(ctx, connect.NewRequest(&sidecarv1.PutCellRequest{
		Cell: cell,
	}))
	return err
}

// GetCells returns all cells in the mesh.
func (c *Client) GetCells(ctx context.Context) ([]*sidecarv1.Cell, error) {
	resp, err := c.sidecar.GetCells(ctx, connect.NewRequest(&sidecarv1.GetCellsRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.Cells, nil
}

// PutTrack creates or updates a track.
func (c *Client) PutTrack(ctx context.Context, track *sidecarv1.Track) error {
	_, err := c.sidecar.PutTrack(ctx, connect.NewRequest(&sidecarv1.PutTrackRequest{
		Track: track,
	}))
	return err
}

// GetTracks returns all tracks in the mesh.
func (c *Client) GetTracks(ctx context.Context) ([]*sidecarv1.Track, error) {
	resp, err := c.sidecar.GetTracks(ctx, connect.NewRequest(&sidecarv1.GetTracksRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.Tracks, nil
}

// PutCommand creates or updates a command.
func (c *Client) PutCommand(ctx context.Context, command *sidecarv1.Command) error {
	_, err := c.sidecar.PutCommand(ctx, connect.NewRequest(&sidecarv1.PutCommandRequest{
		Command: command,
	}))
	return err
}

// GetCommands returns all commands in the mesh.
func (c *Client) GetCommands(ctx context.Context) ([]*sidecarv1.Command, error) {
	resp, err := c.sidecar.GetCommands(ctx, connect.NewRequest(&sidecarv1.GetCommandsRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.Commands, nil
}

// --- Subscriptions ---

// DocumentChange represents a change event from the mesh.
type DocumentChange = sidecarv1.DocumentChange

// Subscribe streams document changes. If collections is empty, all collections
// are streamed. Cancel the context to stop the subscription.
func (c *Client) Subscribe(ctx context.Context, collections ...string) (<-chan *DocumentChange, <-chan error) {
	ch := make(chan *DocumentChange, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)

		stream, err := c.sidecar.Subscribe(ctx, connect.NewRequest(&sidecarv1.SubscribeRequest{
			Collections: collections,
		}))
		if err != nil {
			errCh <- err
			return
		}

		for stream.Receive() {
			select {
			case ch <- stream.Msg():
			case <-ctx.Done():
				return
			}
		}
		if err := stream.Err(); err != nil {
			errCh <- err
		}
	}()

	return ch, errCh
}

// --- Sync Control ---

// StartSync begins CRDT synchronization with connected peers.
func (c *Client) StartSync(ctx context.Context) error {
	_, err := c.sidecar.StartSync(ctx, connect.NewRequest(&sidecarv1.StartSyncRequest{}))
	return err
}

// StopSync pauses synchronization.
func (c *Client) StopSync(ctx context.Context) error {
	_, err := c.sidecar.StopSync(ctx, connect.NewRequest(&sidecarv1.StopSyncRequest{}))
	return err
}

// SyncStats returns current synchronization statistics.
func (c *Client) SyncStats(ctx context.Context) (*sidecarv1.GetSyncStatsResponse, error) {
	resp, err := c.sidecar.GetSyncStats(ctx, connect.NewRequest(&sidecarv1.GetSyncStatsRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}
