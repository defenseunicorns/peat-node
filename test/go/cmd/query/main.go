// Quick query tool: shows all data in a peat-sidecar's CRDT store.
package main

import (
	"context"
	"fmt"
	"os"

	peat "github.com/defenseunicorns/peat-sidecar/test/go"
)

func main() {
	target := "http://localhost:32551"
	if env := os.Getenv("PEAT_SIDECAR_ADDR"); env != "" {
		target = env
	}
	client, err := peat.Connect(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	ctx := context.Background()

	status, _ := client.Status(ctx)
	fmt.Printf("Node: %s  Endpoint: %s  Sync: %v  Peers: %d\n\n",
		status.NodeId, status.EndpointAddr, status.SyncActive, status.ConnectedPeers)

	for _, collection := range []string{"platforms", "deployments", "packages"} {
		ids, _ := client.ListDocuments(ctx, collection)
		fmt.Printf("=== %s (%d) ===\n", collection, len(ids))
		for _, id := range ids {
			data, _ := client.GetDocument(ctx, collection, id)
			if data != nil {
				fmt.Printf("  %s = %s\n", id, *data)
			}
		}
		fmt.Println()
	}
}
