// Functional test: UDS Remote Agent + peat-node watcher + CRDT sync.
//
// 1. Starts a real UDS Remote Agent (insecure mode, no k8s required)
// 2. Starts two peat-node instances — node-a watches the agent, node-b is a peer
// 3. Waits for the watcher to poll agent state and write it to the CRDT store
// 4. Verifies agent state synced from node-a to node-b
//
// Usage:
//
//	UDS_AGENT_BIN=/tmp/uds-remote-agent \
//	PEAT_NODE_BIN=../../peat-node/target/release/peat-node \
//	go run ./cmd/watchertest/
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	peat "github.com/defenseunicorns/peat-node/test/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nAll watcher tests passed!")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	agentBin := envOr("UDS_AGENT_BIN", "/tmp/uds-remote-agent")
	sidecarBin := envOr("PEAT_NODE_BIN",
		filepath.Join("..", "..", "..", "peat-node", "target", "release", "peat-node"))

	if _, err := os.Stat(agentBin); err != nil {
		return fmt.Errorf("agent binary not found at %s (set UDS_AGENT_BIN)", agentBin)
	}
	if _, err := os.Stat(sidecarBin); err != nil {
		return fmt.Errorf("sidecar binary not found at %s (set PEAT_NODE_BIN)", sidecarBin)
	}

	tmpDir, err := os.MkdirTemp("", "peat-watchertest-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// 1. Start UDS Remote Agent (insecure, no k8s)
	fmt.Println("--- Starting UDS Remote Agent on :9080 (insecure) ---")
	agent, err := startProcess(ctx, agentBin, []string{
		"--disable-mtls",
		"--port", "9080",
		"--host", "127.0.0.1",
		"--data-dir", filepath.Join(tmpDir, "agent-data"),
		"--log-level", "warn",
	}, nil)
	if err != nil {
		return fmt.Errorf("start agent: %w", err)
	}
	defer agent.stop()

	// Give agent time to start
	time.Sleep(3 * time.Second)

	// Pin Iroh UDP ports for direct peering (no relay).
	const nodeAIrohPort = 51261
	const nodeBIrohPort = 51262

	// 2. Start peat-node node-a (watches the agent)
	fmt.Println("--- Starting peat-node node-a on :50061 (watching agent, iroh udp :51261) ---")
	nodeA, err := startProcess(ctx, sidecarBin, []string{
		"--listen", "tcp://127.0.0.1:50061",
		"--data-dir", filepath.Join(tmpDir, "node-a"),
		"--node-id", "node-a",
		"--agent-addr", "http://127.0.0.1:9080",
		"--agent-poll-interval", "3",
		"--auto-sync",
		"--iroh-udp-port", fmt.Sprintf("%d", nodeAIrohPort),
	}, []string{"RUST_LOG=peat_node=info,peat_mesh=info"})
	if err != nil {
		return fmt.Errorf("start node-a: %w", err)
	}
	defer nodeA.stop()

	// 3. Start peat-node node-b (peer, no agent)
	fmt.Println("--- Starting peat-node node-b on :50062 (peer only, iroh udp :51262) ---")
	nodeB, err := startProcess(ctx, sidecarBin, []string{
		"--listen", "tcp://127.0.0.1:50062",
		"--data-dir", filepath.Join(tmpDir, "node-b"),
		"--node-id", "node-b",
		"--auto-sync",
		"--iroh-udp-port", fmt.Sprintf("%d", nodeBIrohPort),
	}, []string{"RUST_LOG=peat_node=info,peat_mesh=info"})
	if err != nil {
		return fmt.Errorf("start node-b: %w", err)
	}
	defer nodeB.stop()

	// Wait for sidecars to start
	time.Sleep(3 * time.Second)

	// Connect Go clients
	clientA, err := peat.Connect("http://127.0.0.1:50061")
	if err != nil {
		return fmt.Errorf("connect to node-a: %w", err)
	}
	clientB, err := peat.Connect("http://127.0.0.1:50062")
	if err != nil {
		return fmt.Errorf("connect to node-b: %w", err)
	}

	// 4. Verify agent watcher populated node-a's platform collection
	fmt.Println("--- Waiting for watcher to poll agent ---")
	var platforms []string
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		platforms, err = clientA.ListDocuments(ctx, "platforms")
		if err != nil {
			return fmt.Errorf("node-a list platforms: %w", err)
		}
		if len(platforms) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(platforms) == 0 {
		return fmt.Errorf("watcher did not populate platforms within 20s")
	}
	fmt.Printf("PASS: watcher wrote %d platform(s) to node-a: %v\n", len(platforms), platforms)

	// Read the platform data
	data, err := clientA.GetDocument(ctx, "platforms", "node-a")
	if err != nil {
		return fmt.Errorf("node-a get platform: %w", err)
	}
	if data == nil {
		return fmt.Errorf("platform doc is nil")
	}
	fmt.Printf("PASS: platform data from agent watcher: %s\n", *data)

	// 5. Check deployments collection (agent has no k8s, so likely empty — but watcher should have tried)
	deployments, err := clientA.ListDocuments(ctx, "deployments")
	if err != nil {
		return fmt.Errorf("node-a list deployments: %w", err)
	}
	fmt.Printf("PASS: node-a has %d deployment(s) from agent\n", len(deployments))

	// 6. Peer the two sidecars and verify cross-node sync
	fmt.Println("--- Peering node-a and node-b ---")
	statusA, err := clientA.Status(ctx)
	if err != nil {
		return fmt.Errorf("node-a status: %w", err)
	}
	nodeAAddr := fmt.Sprintf("127.0.0.1:%d", nodeAIrohPort)
	err = clientB.ConnectPeer(ctx, statusA.EndpointAddr, []string{nodeAAddr}, "")
	if err != nil {
		return fmt.Errorf("connect peer: %w", err)
	}

	// Wait for connection + sync
	time.Sleep(3 * time.Second)
	if err := clientA.StartSync(ctx); err != nil {
		return fmt.Errorf("node-a start sync: %w", err)
	}
	if err := clientB.StartSync(ctx); err != nil {
		return fmt.Errorf("node-b start sync: %w", err)
	}

	// 7. Wait for agent state to sync from node-a to node-b
	fmt.Println("--- Waiting for agent state to sync to node-b ---")
	deadline = time.Now().Add(30 * time.Second)
	var remotePlatforms []string
	for time.Now().Before(deadline) {
		remotePlatforms, err = clientB.ListDocuments(ctx, "platforms")
		if err != nil {
			return fmt.Errorf("node-b list platforms: %w", err)
		}
		if len(remotePlatforms) > 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if len(remotePlatforms) == 0 {
		return fmt.Errorf("agent state did not sync to node-b within 30s")
	}

	// Read the synced data on node-b
	remoteData, err := clientB.GetDocument(ctx, "platforms", "node-a")
	if err != nil {
		return fmt.Errorf("node-b get platform: %w", err)
	}
	if remoteData == nil {
		return fmt.Errorf("synced platform doc is nil on node-b")
	}
	fmt.Printf("PASS: agent state synced to node-b: %s\n", *remoteData)

	fmt.Println("\n--- Summary ---")
	fmt.Println("1. UDS Remote Agent running (insecure, no k8s)")
	fmt.Println("2. peat-node node-a watched agent via Connect RPC")
	fmt.Println("3. Agent status written to CRDT 'platforms' collection")
	fmt.Println("4. node-a peered with node-b via Iroh QUIC")
	fmt.Println("5. Agent state synced from node-a to node-b via CRDT")

	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type process struct {
	cmd *exec.Cmd
}

func startProcess(ctx context.Context, bin string, args []string, extraEnv []string) (*process, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = append(os.Environ(), extraEnv...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &process{cmd: cmd}, nil
}

func (p *process) stop() {
	if p.cmd != nil && p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
		_ = p.cmd.Wait()
	}
}
