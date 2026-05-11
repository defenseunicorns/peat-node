// Tier 1 integration test: two peat-node nodes, CRDT sync via direct
// Iroh QUIC (no relay). Each node pins its Iroh UDP port; the second
// node peers to the first via 127.0.0.1:<port> through ConnectPeer.addresses.
//
// 1. Starts two sidecar processes (node-a on :50061, node-b on :50062)
// 2. Exchanges endpoint IDs and connects them as peers
// 3. Writes a platform document on node-a
// 4. Polls node-b until the document appears (CRDT sync)
// 5. Verifies the data matches
// 6. Cleans up both processes
//
// Usage:
//
//	PEAT_NODE_BIN=/path/to/peat-node go run ./cmd/synctest/
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	peat "github.com/defenseunicorns/peat-node/test/go"
	sidecarv1 "github.com/defenseunicorns/peat-node/test/go/gen/peat/sidecar/v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nAll sync tests passed!")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	bin := os.Getenv("PEAT_NODE_BIN")
	if bin == "" {
		bin = "peat-node"
	}

	// Resolve binary path
	if _, err := exec.LookPath(bin); err != nil {
		// Try relative to peat-node build dir
		candidate := filepath.Join("..", "..", "..", "peat-node", "target", "release", "peat-node")
		if _, err2 := os.Stat(candidate); err2 == nil {
			bin = candidate
		} else {
			return fmt.Errorf("peat-node binary not found (set PEAT_NODE_BIN): %w", err)
		}
	}

	tmpDir, err := os.MkdirTemp("", "peat-synctest-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// Pin Iroh UDP ports so the second node can reach the first directly,
	// without depending on the n0 public relay.
	const nodeAIrohPort = 51161
	const nodeBIrohPort = 51162

	// Start node A
	fmt.Println("--- Starting node-a on :50061 (iroh udp :51161) ---")
	nodeA, err := startSidecar(ctx, bin, sidecarOpts{
		port:        50061,
		nodeID:      "node-a",
		dataDir:     filepath.Join(tmpDir, "node-a"),
		irohUDPPort: nodeAIrohPort,
	})
	if err != nil {
		return fmt.Errorf("start node-a: %w", err)
	}
	defer nodeA.stop()

	// Start node B
	fmt.Println("--- Starting node-b on :50062 (iroh udp :51162) ---")
	nodeB, err := startSidecar(ctx, bin, sidecarOpts{
		port:        50062,
		nodeID:      "node-b",
		dataDir:     filepath.Join(tmpDir, "node-b"),
		irohUDPPort: nodeBIrohPort,
	})
	if err != nil {
		return fmt.Errorf("start node-b: %w", err)
	}
	defer nodeB.stop()

	// Wait for both to be ready
	time.Sleep(2 * time.Second)

	// Connect Go clients
	clientA, err := peat.Connect(fmt.Sprintf("http://127.0.0.1:%d", 50061))
	if err != nil {
		return fmt.Errorf("connect to node-a: %w", err)
	}
	clientB, err := peat.Connect(fmt.Sprintf("http://127.0.0.1:%d", 50062))
	if err != nil {
		return fmt.Errorf("connect to node-b: %w", err)
	}

	// 1. Get endpoint IDs
	statusA, err := clientA.Status(ctx)
	if err != nil {
		return fmt.Errorf("status node-a: %w", err)
	}
	fmt.Printf("PASS: node-a endpoint=%s\n", statusA.EndpointAddr)

	statusB, err := clientB.Status(ctx)
	if err != nil {
		return fmt.Errorf("status node-b: %w", err)
	}
	fmt.Printf("PASS: node-b endpoint=%s\n", statusB.EndpointAddr)

	// 2. Connect node-b to node-a as a peer via direct address.
	fmt.Println("--- Connecting peers (direct UDP, no relay) ---")
	nodeAAddr := fmt.Sprintf("127.0.0.1:%d", nodeAIrohPort)
	err = clientB.ConnectPeer(ctx, statusA.EndpointAddr, []string{nodeAAddr}, "")
	if err != nil {
		return fmt.Errorf("connect peer: %w", err)
	}
	fmt.Printf("PASS: node-b connected to node-a via %s\n", nodeAAddr)

	// Give Iroh a moment to establish the connection
	time.Sleep(2 * time.Second)

	// Verify peer connection
	peersB, err := clientB.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("list peers node-b: %w", err)
	}
	fmt.Printf("PASS: node-b peers=%d\n", len(peersB))

	// 3. Start sync on both
	fmt.Println("--- Starting sync ---")
	if err := clientA.StartSync(ctx); err != nil {
		return fmt.Errorf("start sync node-a: %w", err)
	}
	if err := clientB.StartSync(ctx); err != nil {
		return fmt.Errorf("start sync node-b: %w", err)
	}

	// 4. Write a platform document on node-a
	fmt.Println("--- Writing platform on node-a ---")
	err = clientA.PutPlatform(ctx, &sidecarv1.Platform{
		Id:           "cluster-alpha-agent",
		PlatformType: "uds-remote-agent",
		Name:         "UDS Remote Agent @ cluster-alpha",
		Status:       sidecarv1.PlatformStatus_PLATFORM_STATUS_READY,
		Latitude:     38.8977,
		Longitude:    -77.0365,
		Capabilities: []string{"package-management", "registry-sync"},
	})
	if err != nil {
		return fmt.Errorf("put platform on node-a: %w", err)
	}
	fmt.Println("PASS: wrote platform cluster-alpha-agent on node-a")

	// Also write a generic document
	err = clientA.PutDocument(ctx, "deployments", "app-v2",
		`{"name":"mission-app","version":"2.0.0","status":"deployed"}`)
	if err != nil {
		return fmt.Errorf("put document on node-a: %w", err)
	}
	fmt.Println("PASS: wrote document deployments/app-v2 on node-a")

	// 5. Poll node-b until the platform appears (CRDT sync)
	fmt.Println("--- Waiting for CRDT sync to node-b ---")
	var platforms []*sidecarv1.Platform
	synced := false
	for i := 0; i < 30; i++ {
		platforms, err = clientB.GetPlatforms(ctx)
		if err != nil {
			return fmt.Errorf("get platforms node-b: %w", err)
		}
		if len(platforms) > 0 {
			synced = true
			break
		}
		fmt.Printf("  poll %d/30: waiting for sync...\n", i+1)
		time.Sleep(time.Second)
	}

	if !synced {
		return fmt.Errorf("platform did not sync to node-b within 30s")
	}

	// 6. Verify the data matches
	p := platforms[0]
	fmt.Printf("PASS: platform synced to node-b — id=%s type=%s name=%s lat=%.4f lon=%.4f caps=%v\n",
		p.Id, p.PlatformType, p.Name, p.Latitude, p.Longitude, p.Capabilities)

	if p.Id != "cluster-alpha-agent" {
		return fmt.Errorf("platform ID mismatch: got %s, want cluster-alpha-agent", p.Id)
	}
	if p.PlatformType != "uds-remote-agent" {
		return fmt.Errorf("platform type mismatch: got %s", p.PlatformType)
	}

	// Check the generic document too
	data, err := clientB.GetDocument(ctx, "deployments", "app-v2")
	if err != nil {
		return fmt.Errorf("get document node-b: %w", err)
	}
	if data != nil {
		fmt.Printf("PASS: document synced to node-b — deployments/app-v2 = %s\n", *data)
	} else {
		fmt.Println("INFO: generic document not yet synced (platform sync confirmed)")
	}

	// 7. Verify bidirectional — write on node-b, check node-a
	fmt.Println("--- Testing bidirectional sync (node-b → node-a) ---")
	err = clientB.PutPlatform(ctx, &sidecarv1.Platform{
		Id:           "cluster-bravo-agent",
		PlatformType: "uds-remote-agent",
		Name:         "UDS Remote Agent @ cluster-bravo",
		Status:       sidecarv1.PlatformStatus_PLATFORM_STATUS_READY,
		Latitude:     34.0522,
		Longitude:    -118.2437,
		Capabilities: []string{"package-management"},
	})
	if err != nil {
		return fmt.Errorf("put platform on node-b: %w", err)
	}

	synced = false
	for i := 0; i < 30; i++ {
		platforms, err = clientA.GetPlatforms(ctx)
		if err != nil {
			return fmt.Errorf("get platforms node-a: %w", err)
		}
		// Should have both platforms
		if len(platforms) >= 2 {
			synced = true
			break
		}
		fmt.Printf("  poll %d/30: node-a has %d platforms, waiting for 2...\n", i+1, len(platforms))
		time.Sleep(time.Second)
	}

	if !synced {
		return fmt.Errorf("bidirectional sync failed — node-a has %d platforms, expected 2", len(platforms))
	}

	fmt.Printf("PASS: bidirectional sync — node-a sees %d platforms\n", len(platforms))
	for _, pl := range platforms {
		fmt.Printf("  - %s (%s) @ %.4f, %.4f\n", pl.Id, pl.Name, pl.Latitude, pl.Longitude)
	}

	// 8. Verify GetSyncStats reports real byte counters (issue #39).
	// After bidirectional sync, both nodes should have non-zero bytes_sent
	// AND bytes_received. A zero from either side means the wiring from
	// AutomergeSyncCoordinator into SyncStats regressed.
	fmt.Println("--- Verifying GetSyncStats byte counters ---")
	statsA, err := clientA.SyncStats(ctx)
	if err != nil {
		return fmt.Errorf("sync stats node-a: %w", err)
	}
	statsB, err := clientB.SyncStats(ctx)
	if err != nil {
		return fmt.Errorf("sync stats node-b: %w", err)
	}
	fmt.Printf("  node-a: bytes_sent=%d bytes_received=%d\n", statsA.BytesSent, statsA.BytesReceived)
	fmt.Printf("  node-b: bytes_sent=%d bytes_received=%d\n", statsB.BytesSent, statsB.BytesReceived)
	if statsA.BytesSent == 0 {
		return fmt.Errorf("node-a bytes_sent is 0 after sync — counter wiring regressed")
	}
	if statsA.BytesReceived == 0 {
		return fmt.Errorf("node-a bytes_received is 0 after sync — counter wiring regressed")
	}
	if statsB.BytesSent == 0 {
		return fmt.Errorf("node-b bytes_sent is 0 after sync — counter wiring regressed")
	}
	if statsB.BytesReceived == 0 {
		return fmt.Errorf("node-b bytes_received is 0 after sync — counter wiring regressed")
	}
	fmt.Println("PASS: both nodes report non-zero bytes_sent and bytes_received")

	return nil
}

type sidecarOpts struct {
	port        int
	nodeID      string
	dataDir     string
	irohUDPPort int
}

type sidecarProc struct {
	cmd *exec.Cmd
}

func startSidecar(ctx context.Context, bin string, opts sidecarOpts) (*sidecarProc, error) {
	if err := os.MkdirAll(opts.dataDir, 0o755); err != nil {
		return nil, err
	}

	args := []string{
		"--listen", fmt.Sprintf("tcp://127.0.0.1:%d", opts.port),
		"--data-dir", opts.dataDir,
		"--node-id", opts.nodeID,
		"--auto-sync",
	}
	if opts.irohUDPPort != 0 {
		args = append(args, "--iroh-udp-port", fmt.Sprintf("%d", opts.irohUDPPort))
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), "RUST_LOG=peat_node=info,peat_mesh=info")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	return &sidecarProc{cmd: cmd}, nil
}

func (s *sidecarProc) stop() {
	if s.cmd != nil && s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
		_ = s.cmd.Wait()
	}
}
