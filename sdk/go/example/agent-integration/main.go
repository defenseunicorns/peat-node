// Example: How UDS Remote Agent integrates with peat-sidecar.
//
// This demonstrates the three integration patterns:
//  1. Agent pushes heartbeats to sidecar (replaces watcher polling)
//  2. Agent queries fleet state from sidecar (autonomous operation)
//  3. Agent watches for commands via subscription (hub → agent via CRDT)
//
// Usage:
//
//	go run ./example/agent-integration/ [--addr http://localhost:50051] [--agent-id my-agent]
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	peat "github.com/defenseunicorns/peat-sidecar/sdk/go"
)

func main() {
	addr := flag.String("addr", envOrDefault("PEAT_SIDECAR_ADDR", "http://localhost:50051"), "peat-sidecar address")
	agentID := flag.String("agent-id", envOrDefault("PEAT_SIDECAR_NODE_ID", "example-agent"), "agent identifier")
	interval := flag.Duration("interval", 10*time.Second, "heartbeat interval")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// Connect to the co-located peat-sidecar
	client, err := peat.Connect(*addr)
	if err != nil {
		log.Fatalf("connect to sidecar: %v", err)
	}

	// Verify connectivity
	status, err := client.Status(ctx)
	if err != nil {
		log.Fatalf("sidecar status: %v", err)
	}
	fmt.Printf("Connected to sidecar: node=%s endpoint=%s\n", status.NodeId, status.EndpointAddr)

	// --- Pattern 1: Push heartbeats ---
	go func() {
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := client.Heartbeat(ctx, &peat.AgentHeartbeat{
					AgentID:       *agentID,
					Version:       "0.1.0",
					Architecture:  "amd64",
					K8sVersion:    "v1.33.0",
					K8sNodeStatus: "Ready",
					ZarfVersion:   "v0.50.0",
					RunMode:       "connected",
					Labels:        map[string]string{"region": "us-east-1", "env": "staging"},
				})
				if err != nil {
					log.Printf("heartbeat failed: %v", err)
				} else {
					fmt.Printf("[heartbeat] pushed status for %s\n", *agentID)
				}
			}
		}
	}()

	// Report a deployment
	err = client.ReportDeployment(ctx, &peat.DeploymentStatus{
		AgentID: *agentID,
		Package: "nginx",
		Version: "1.25.0",
		Status:  "deployed",
	})
	if err != nil {
		log.Printf("report deployment: %v", err)
	} else {
		fmt.Println("[deployment] reported nginx 1.25.0")
	}

	// --- Pattern 2: Query fleet state ---
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				platforms, err := client.FleetPlatforms(ctx)
				if err != nil {
					log.Printf("fleet query: %v", err)
					continue
				}
				fmt.Printf("[fleet] %d platforms visible:\n", len(platforms))
				for _, p := range platforms {
					age := time.Since(time.Unix(p.LastSeen, 0)).Round(time.Second)
					fmt.Printf("  - %s (%s, %s ago)\n", p.AgentID, p.RunMode, age)
				}
			}
		}
	}()

	// --- Pattern 3: Watch for commands ---
	changes, errCh := client.Subscribe(ctx, "commands")
	go func() {
		for change := range changes {
			fmt.Printf("[command] %s/%s: %s\n", change.Collection, change.DocId, change.GetJsonData())
		}
	}()
	go func() {
		for err := range errCh {
			if err != nil {
				log.Printf("subscription error: %v", err)
			}
		}
	}()

	fmt.Println("Agent integration running. Press Ctrl+C to stop.")
	<-ctx.Done()
	fmt.Println("\nShutting down.")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
