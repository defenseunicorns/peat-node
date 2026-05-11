// Cross-cluster sync test: verifies that two UDS Remote Agent deployments
// with peat-node sidecars can synchronize state via CRDT mesh.
//
// Requires:
//   - ALPHA_PEAT_ADDR: gRPC address of peat-node on cluster alpha (e.g. http://localhost:32551)
//   - BRAVO_PEAT_ADDR: gRPC address of peat-node on cluster bravo (e.g. http://localhost:33551)
package cluster

import (
	"context"
	"os"
	"testing"
	"time"

	peat "github.com/defenseunicorns/peat-node/test/go"
	sidecarv1 "github.com/defenseunicorns/peat-node/test/go/gen/peat/sidecar/v1"
)

func requiredEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("skipping: %s not set", key)
	}
	return v
}

func TestCrossClusterSync(t *testing.T) {
	alphaAddr := requiredEnv(t, "ALPHA_PEAT_ADDR")
	bravoAddr := requiredEnv(t, "BRAVO_PEAT_ADDR")
	// Direct Iroh UDP address (host:port) for the alpha sidecar — required
	// now that the public relay is off by default. Operators deploying the
	// cluster pair must pin alpha's --iroh-udp-port and expose it.
	alphaIrohAddr := requiredEnv(t, "ALPHA_PEAT_IROH_ADDR")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Connect to both sidecars
	alpha, err := peat.Connect(alphaAddr)
	if err != nil {
		t.Fatalf("connect to alpha: %v", err)
	}
	bravo, err := peat.Connect(bravoAddr)
	if err != nil {
		t.Fatalf("connect to bravo: %v", err)
	}

	// 1. Verify both sidecars are healthy
	t.Run("status", func(t *testing.T) {
		statusA, err := alpha.Status(ctx)
		if err != nil {
			t.Fatalf("alpha status: %v", err)
		}
		t.Logf("alpha: node=%s endpoint=%s sync=%v peers=%d",
			statusA.NodeId, statusA.EndpointAddr, statusA.SyncActive, statusA.ConnectedPeers)

		statusB, err := bravo.Status(ctx)
		if err != nil {
			t.Fatalf("bravo status: %v", err)
		}
		t.Logf("bravo: node=%s endpoint=%s sync=%v peers=%d",
			statusB.NodeId, statusB.EndpointAddr, statusB.SyncActive, statusB.ConnectedPeers)

		// Connect bravo to alpha as a peer via direct UDP address.
		t.Logf("connecting bravo → alpha (endpoint %s via %s)", statusA.EndpointAddr, alphaIrohAddr)
		if err := bravo.ConnectPeer(ctx, statusA.EndpointAddr, []string{alphaIrohAddr}, ""); err != nil {
			t.Fatalf("connect peer: %v", err)
		}

		// Wait for connection to establish
		time.Sleep(3 * time.Second)

		peersB, err := bravo.ListPeers(ctx)
		if err != nil {
			t.Fatalf("bravo list peers: %v", err)
		}
		if len(peersB) == 0 {
			t.Fatal("bravo has no peers after ConnectPeer")
		}
		t.Logf("bravo connected to %d peer(s)", len(peersB))

		// Start sync on both
		if err := alpha.StartSync(ctx); err != nil {
			t.Fatalf("alpha start sync: %v", err)
		}
		if err := bravo.StartSync(ctx); err != nil {
			t.Fatalf("bravo start sync: %v", err)
		}
	})

	// 2. Verify agent watcher populated platforms — alpha-agent should appear on bravo
	t.Run("watcher_platform_sync", func(t *testing.T) {
		// The agent watcher on each sidecar polls the local agent's /status
		// and writes a platform doc. After peering + sync, bravo should see
		// alpha-agent (populated by alpha's watcher, synced via CRDT).
		var platforms []*sidecarv1.Platform
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			platforms, err = bravo.GetPlatforms(ctx)
			if err != nil {
				t.Fatalf("bravo get platforms: %v", err)
			}
			// Look for alpha-agent (written by alpha's watcher)
			for _, p := range platforms {
				if p.Id == "alpha-agent" {
					t.Logf("alpha-agent synced to bravo: id=%s type=%s", p.Id, p.PlatformType)
					return
				}
			}
			time.Sleep(time.Second)
		}
		t.Fatalf("alpha-agent did not sync to bravo within 30s (bravo sees %d platforms)", len(platforms))
	})

	// 3. Verify bidirectional — alpha should see bravo-agent (from bravo's watcher)
	t.Run("bidirectional_watcher_sync", func(t *testing.T) {
		// Alpha's watcher writes alpha-agent, bravo's watcher writes bravo-agent.
		// After peering, alpha should see both.
		var platforms []*sidecarv1.Platform
		deadline := time.Now().Add(30 * time.Second)
		seenAlpha, seenBravo := false, false
		for time.Now().Before(deadline) {
			platforms, err = alpha.GetPlatforms(ctx)
			if err != nil {
				t.Fatalf("alpha get platforms: %v", err)
			}
			for _, p := range platforms {
				if p.Id == "alpha-agent" {
					seenAlpha = true
				}
				if p.Id == "bravo-agent" {
					seenBravo = true
				}
			}
			if seenAlpha && seenBravo {
				break
			}
			time.Sleep(time.Second)
		}

		if !seenAlpha || !seenBravo {
			t.Fatalf("bidirectional sync failed: alpha sees alpha-agent=%v bravo-agent=%v (total %d platforms)",
				seenAlpha, seenBravo, len(platforms))
		}

		t.Logf("alpha sees %d platforms:", len(platforms))
		for _, pl := range platforms {
			t.Logf("  - %s (type=%s)", pl.Id, pl.PlatformType)
		}
	})

	// 4. Test generic document sync (simulates deployment state sharing)
	t.Run("deployment_state_sync", func(t *testing.T) {
		// Alpha deploys an app — write deployment state
		err := alpha.PutDocument(ctx, "deployments", "mission-app-v2",
			`{"package":"mission-app","version":"2.0.0","status":"deployed","cluster":"alpha"}`)
		if err != nil {
			t.Fatalf("alpha put deployment doc: %v", err)
		}

		// Bravo deploys a different app
		err = bravo.PutDocument(ctx, "deployments", "monitoring-v1",
			`{"package":"monitoring","version":"1.0.0","status":"deployed","cluster":"bravo"}`)
		if err != nil {
			t.Fatalf("bravo put deployment doc: %v", err)
		}

		// Both clusters should eventually see both deployments
		deadline := time.Now().Add(30 * time.Second)
		var alphaOK, bravoOK bool
		for time.Now().Before(deadline) && (!alphaOK || !bravoOK) {
			if !alphaOK {
				ids, _ := alpha.ListDocuments(ctx, "deployments")
				if len(ids) >= 2 {
					alphaOK = true
					t.Logf("alpha sees deployments: %v", ids)
				}
			}
			if !bravoOK {
				ids, _ := bravo.ListDocuments(ctx, "deployments")
				if len(ids) >= 2 {
					bravoOK = true
					t.Logf("bravo sees deployments: %v", ids)
				}
			}
			if !alphaOK || !bravoOK {
				time.Sleep(time.Second)
			}
		}

		if !alphaOK {
			t.Error("alpha did not receive bravo's deployment doc within 30s")
		}
		if !bravoOK {
			t.Error("bravo did not receive alpha's deployment doc within 30s")
		}

		// Verify content fidelity
		data, err := bravo.GetDocument(ctx, "deployments", "mission-app-v2")
		if err != nil {
			t.Fatalf("bravo get deployment doc: %v", err)
		}
		if data == nil {
			t.Fatal("bravo missing mission-app-v2 document")
		}
		t.Logf("bravo received mission-app-v2: %s", *data)
	})

	// 5. Verify sync stats show active sync on both sides
	t.Run("sync_stats", func(t *testing.T) {
		statsA, err := alpha.SyncStats(ctx)
		if err != nil {
			t.Fatalf("alpha sync stats: %v", err)
		}
		t.Logf("alpha sync: active=%v peers=%d", statsA.SyncActive, statsA.ConnectedPeers)

		statsB, err := bravo.SyncStats(ctx)
		if err != nil {
			t.Fatalf("bravo sync stats: %v", err)
		}
		t.Logf("bravo sync: active=%v peers=%d", statsB.SyncActive, statsB.ConnectedPeers)

		if !statsA.SyncActive {
			t.Error("alpha sync not active")
		}
		if !statsB.SyncActive {
			t.Error("bravo sync not active")
		}
	})
}
