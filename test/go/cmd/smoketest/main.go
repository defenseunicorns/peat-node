// Smoke test: connects to peat-sidecar, does a round-trip put/get, verifies.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	peat "github.com/defenseunicorns/peat-sidecar/test/go"
	sidecarv1 "github.com/defenseunicorns/peat-sidecar/test/go/gen/peat/sidecar/v1"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	target := "http://127.0.0.1:50051"
	if env := os.Getenv("PEAT_SIDECAR_ADDR"); env != "" {
		target = env
	}

	client, err := peat.Connect(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: connect: %v\n", err)
		os.Exit(1)
	}

	// 1. Status
	status, err := client.Status(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: status: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: status — node=%s endpoint=%s sync=%v peers=%d\n",
		status.NodeId, status.EndpointAddr, status.SyncActive, status.ConnectedPeers)

	// 2. Put document
	err = client.PutDocument(ctx, "test", "doc-1", `{"hello":"world","count":42}`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: put document: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PASS: put document test/doc-1")

	// 3. Get document
	data, err := client.GetDocument(ctx, "test", "doc-1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: get document: %v\n", err)
		os.Exit(1)
	}
	if data == nil {
		fmt.Fprintf(os.Stderr, "FAIL: get document returned nil\n")
		os.Exit(1)
	}
	fmt.Printf("PASS: get document test/doc-1 = %s\n", *data)

	// 4. List documents
	ids, err := client.ListDocuments(ctx, "test")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: list documents: %v\n", err)
		os.Exit(1)
	}
	if len(ids) != 1 || ids[0] != "doc-1" {
		fmt.Fprintf(os.Stderr, "FAIL: list documents expected [doc-1], got %v\n", ids)
		os.Exit(1)
	}
	fmt.Printf("PASS: list documents test = %v\n", ids)

	// 5. Put platform (typed collection)
	err = client.PutPlatform(ctx, &sidecarv1.Platform{
		Id:           "agent-1",
		PlatformType: "uds-remote-agent",
		Name:         "Test Agent",
		Status:       sidecarv1.PlatformStatus_PLATFORM_STATUS_READY,
		Latitude:     37.7749,
		Longitude:    -122.4194,
		Capabilities: []string{"package-mgmt", "registry-sync"},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: put platform: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("PASS: put platform agent-1")

	// 6. Get platforms
	platforms, err := client.GetPlatforms(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: get platforms: %v\n", err)
		os.Exit(1)
	}
	if len(platforms) != 1 {
		fmt.Fprintf(os.Stderr, "FAIL: expected 1 platform, got %d\n", len(platforms))
		os.Exit(1)
	}
	p := platforms[0]
	fmt.Printf("PASS: get platforms — id=%s type=%s name=%s caps=%v\n",
		p.Id, p.PlatformType, p.Name, p.Capabilities)

	// 7. Sync stats
	stats, err := client.SyncStats(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: sync stats: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: sync stats — active=%v peers=%d\n", stats.SyncActive, stats.ConnectedPeers)

	// 8. Delete document
	err = client.DeleteDocument(ctx, "test", "doc-1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: delete document: %v\n", err)
		os.Exit(1)
	}
	data, err = client.GetDocument(ctx, "test", "doc-1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: get after delete: %v\n", err)
		os.Exit(1)
	}
	if data != nil {
		fmt.Fprintf(os.Stderr, "FAIL: document still exists after delete\n")
		os.Exit(1)
	}
	fmt.Println("PASS: delete document test/doc-1")

	// 9. List peers (should be empty since we're solo)
	peers, err := client.ListPeers(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: list peers: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("PASS: list peers — count=%d\n", len(peers))

	fmt.Println("\nAll tests passed!")
}
