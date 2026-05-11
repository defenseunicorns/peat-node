// Comprehensive functional test for peat-node.
//
// Exercises every feature: transport modes (TCP + UDS), document CRUD,
// all four typed collections, encryption at rest, peer management,
// CRDT sync, subscriptions, sync control, and formation isolation.
//
// Usage:
//
//	PEAT_NODE_BIN=/path/to/peat-node go run ./cmd/functest/
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	peat "github.com/defenseunicorns/peat-node/test/go"
	sidecarv1 "github.com/defenseunicorns/peat-node/test/go/gen/peat/sidecar/v1"
)

// Shared keys for formation authentication.
var (
	sharedKeyAlpha = base64.StdEncoding.EncodeToString(repeatByte(0xAA, 32))
	sharedKeyBravo = base64.StdEncoding.EncodeToString(repeatByte(0xBB, 32))
	encryptionKey  = base64.StdEncoding.EncodeToString(repeatByte(0xCC, 32))
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("\nAll functional tests passed!")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	bin := resolveBinary()

	tmpDir, err := os.MkdirTemp("", "peat-functest-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	// ── Phase 1: Single node on TCP ──────────────────────────────────
	fmt.Println("\n═══ Phase 1: Single node on TCP ═══")
	nodeA, clientA, err := startAndConnect(ctx, bin, nodeOpts{
		listen:    "tcp://127.0.0.1:50070",
		nodeID:    "node-a",
		dataDir:   filepath.Join(tmpDir, "phase1-a"),
		appID:     "functest-alpha",
		sharedKey: sharedKeyAlpha,
	})
	if err != nil {
		return fmt.Errorf("phase1 start: %w", err)
	}

	phase1 := []section{
		{"status/fields", func(ctx context.Context) error { return testStatusFields(ctx, clientA) }},
		{"crud/put-get", func(ctx context.Context) error { return testCrudPutGet(ctx, clientA) }},
		{"crud/list", func(ctx context.Context) error { return testCrudList(ctx, clientA) }},
		{"crud/delete", func(ctx context.Context) error { return testCrudDelete(ctx, clientA) }},
		{"crud/overwrite", func(ctx context.Context) error { return testCrudOverwrite(ctx, clientA) }},
		{"crud/invalid-json", func(ctx context.Context) error { return testCrudInvalidJSON(ctx, clientA) }},
		{"crud/get-nonexistent", func(ctx context.Context) error { return testCrudGetNonexistent(ctx, clientA) }},
		{"crud/multi-collection", func(ctx context.Context) error { return testCrudMultiCollection(ctx, clientA) }},
		{"typed/platform", func(ctx context.Context) error { return testTypedPlatform(ctx, clientA) }},
		{"typed/cell", func(ctx context.Context) error { return testTypedCell(ctx, clientA) }},
		{"typed/track", func(ctx context.Context) error { return testTypedTrack(ctx, clientA) }},
		{"typed/command", func(ctx context.Context) error { return testTypedCommand(ctx, clientA) }},
		{"subscribe/all", func(ctx context.Context) error { return testSubscribeAll(ctx, clientA) }},
		{"subscribe/filtered", func(ctx context.Context) error { return testSubscribeFiltered(ctx, clientA) }},
		{"subscribe/delete", func(ctx context.Context) error { return testSubscribeDelete(ctx, clientA) }},
	}
	if err := runSections(ctx, phase1); err != nil {
		nodeA.stop()
		return err
	}
	nodeA.stop()

	// ── Phase 2: UDS Transport ───────────────────────────────────────
	fmt.Println("\n═══ Phase 2: UDS Transport ═══")
	udsPath := filepath.Join(tmpDir, "peat.sock")
	nodeUDS, clientUDS, err := startAndConnect(ctx, bin, nodeOpts{
		listen:    fmt.Sprintf("unix://%s", udsPath),
		nodeID:    "node-uds",
		dataDir:   filepath.Join(tmpDir, "phase2-uds"),
		appID:     "functest-alpha",
		sharedKey: sharedKeyAlpha,
	})
	if err != nil {
		return fmt.Errorf("phase2 start: %w", err)
	}

	phase2 := []section{
		{"transport/uds", func(ctx context.Context) error { return testTransportUDS(ctx, clientUDS) }},
	}
	if err := runSections(ctx, phase2); err != nil {
		nodeUDS.stop()
		return err
	}
	nodeUDS.stop()

	// ── Phase 3: Encryption at Rest ──────────────────────────────────
	fmt.Println("\n═══ Phase 3: Encryption at Rest ═══")
	const phase3EncIrohPort = 51173
	const phase3PlainIrohPort = 51174
	nodeEnc, clientEnc, err := startAndConnect(ctx, bin, nodeOpts{
		listen:        "tcp://127.0.0.1:50073",
		nodeID:        "node-enc",
		dataDir:       filepath.Join(tmpDir, "phase3-enc"),
		appID:         "functest-alpha",
		sharedKey:     sharedKeyAlpha,
		encryptionKey: encryptionKey,
		irohUDPPort:   phase3EncIrohPort,
	})
	if err != nil {
		return fmt.Errorf("phase3 start enc: %w", err)
	}
	defer nodeEnc.stop()

	nodePlain, clientPlain, err := startAndConnect(ctx, bin, nodeOpts{
		listen:      "tcp://127.0.0.1:50074",
		nodeID:      "node-plain",
		dataDir:     filepath.Join(tmpDir, "phase3-plain"),
		appID:       "functest-alpha",
		sharedKey:   sharedKeyAlpha,
		irohUDPPort: phase3PlainIrohPort,
	})
	if err != nil {
		return fmt.Errorf("phase3 start plain: %w", err)
	}
	defer nodePlain.stop()

	// Peer and sync them via direct UDP addressing.
	statusEnc, err := clientEnc.Status(ctx)
	if err != nil {
		return fmt.Errorf("phase3 status enc: %w", err)
	}
	encAddr := fmt.Sprintf("127.0.0.1:%d", phase3EncIrohPort)
	if err := clientPlain.ConnectPeer(ctx, statusEnc.EndpointAddr, []string{encAddr}, ""); err != nil {
		return fmt.Errorf("phase3 connect peer: %w", err)
	}
	time.Sleep(2 * time.Second)
	if err := clientEnc.StartSync(ctx); err != nil {
		return fmt.Errorf("phase3 start sync enc: %w", err)
	}
	if err := clientPlain.StartSync(ctx); err != nil {
		return fmt.Errorf("phase3 start sync plain: %w", err)
	}

	phase3 := []section{
		{"encrypt/transparent", func(ctx context.Context) error { return testEncryptTransparent(ctx, clientEnc) }},
		{"encrypt/at-rest", func(ctx context.Context) error { return testEncryptAtRest(ctx, clientEnc, clientPlain) }},
	}
	if err := runSections(ctx, phase3); err != nil {
		return err
	}
	nodeEnc.stop()
	nodePlain.stop()

	// ── Phase 4: Peer Sync ───────────────────────────────────────────
	fmt.Println("\n═══ Phase 4: Peer Sync ═══")
	const phase4SAIrohPort = 51170
	const phase4SBIrohPort = 51171
	nodeSA, clientSA, err := startAndConnect(ctx, bin, nodeOpts{
		listen:      "tcp://127.0.0.1:50070",
		nodeID:      "node-sa",
		dataDir:     filepath.Join(tmpDir, "phase4-sa"),
		appID:       "functest-alpha",
		sharedKey:   sharedKeyAlpha,
		irohUDPPort: phase4SAIrohPort,
	})
	if err != nil {
		return fmt.Errorf("phase4 start sa: %w", err)
	}
	defer nodeSA.stop()

	nodeSB, clientSB, err := startAndConnect(ctx, bin, nodeOpts{
		listen:      "tcp://127.0.0.1:50071",
		nodeID:      "node-sb",
		dataDir:     filepath.Join(tmpDir, "phase4-sb"),
		appID:       "functest-alpha",
		sharedKey:   sharedKeyAlpha,
		irohUDPPort: phase4SBIrohPort,
	})
	if err != nil {
		return fmt.Errorf("phase4 start sb: %w", err)
	}
	defer nodeSB.stop()

	saAddr := fmt.Sprintf("127.0.0.1:%d", phase4SAIrohPort)
	phase4 := []section{
		{"peer/connect", func(ctx context.Context) error { return testPeerConnect(ctx, clientSA, clientSB, saAddr) }},
		{"sync/a-to-b", func(ctx context.Context) error { return testSyncAtoB(ctx, clientSA, clientSB) }},
		{"sync/b-to-a", func(ctx context.Context) error { return testSyncBtoA(ctx, clientSA, clientSB) }},
		{"sync/stats", func(ctx context.Context) error { return testSyncStats(ctx, clientSA) }},
		{"sync-control/stop", func(ctx context.Context) error { return testSyncControlStop(ctx, clientSA, clientSB) }},
		{"sync-control/resume", func(ctx context.Context) error { return testSyncControlResume(ctx, clientSA, clientSB) }},
		{"peer/disconnect", func(ctx context.Context) error { return testPeerDisconnect(ctx, clientSA, clientSB) }},
	}
	if err := runSections(ctx, phase4); err != nil {
		return err
	}
	nodeSA.stop()
	nodeSB.stop()

	// ── Phase 5: Formation Isolation ─────────────────────────────────
	fmt.Println("\n═══ Phase 5: Formation Isolation ═══")
	const phase5FAIrohPort = 51175
	const phase5FCIrohPort = 51176
	nodeFA, clientFA, err := startAndConnect(ctx, bin, nodeOpts{
		listen:      "tcp://127.0.0.1:50070",
		nodeID:      "node-fa",
		dataDir:     filepath.Join(tmpDir, "phase5-fa"),
		appID:       "functest-alpha",
		sharedKey:   sharedKeyAlpha,
		irohUDPPort: phase5FAIrohPort,
	})
	if err != nil {
		return fmt.Errorf("phase5 start fa: %w", err)
	}
	defer nodeFA.stop()

	nodeFC, clientFC, err := startAndConnect(ctx, bin, nodeOpts{
		listen:      "tcp://127.0.0.1:50072",
		nodeID:      "node-fc",
		dataDir:     filepath.Join(tmpDir, "phase5-fc"),
		appID:       "functest-bravo",
		sharedKey:   sharedKeyBravo,
		irohUDPPort: phase5FCIrohPort,
	})
	if err != nil {
		return fmt.Errorf("phase5 start fc: %w", err)
	}
	defer nodeFC.stop()

	faAddr := fmt.Sprintf("127.0.0.1:%d", phase5FAIrohPort)
	phase5 := []section{
		{"formation/isolation", func(ctx context.Context) error {
			return testFormationIsolation(ctx, clientFA, clientFC, faAddr)
		}},
	}
	if err := runSections(ctx, phase5); err != nil {
		return err
	}

	return nil
}

// ═══════════════════════════════════════════════════════════════════════
// Test Sections
// ═══════════════════════════════════════════════════════════════════════

func testStatusFields(ctx context.Context, c *peat.Client) error {
	s, err := c.Status(ctx)
	if err != nil {
		return err
	}
	if s.NodeId == "" {
		return fmt.Errorf("node_id is empty")
	}
	if s.EndpointAddr == "" {
		return fmt.Errorf("endpoint_addr is empty")
	}
	if s.Phase == sidecarv1.NodePhase_NODE_PHASE_UNSPECIFIED {
		return fmt.Errorf("phase is unspecified")
	}
	return nil
}

func testCrudPutGet(ctx context.Context, c *peat.Client) error {
	json := `{"name":"hello","value":42}`
	if err := c.PutDocument(ctx, "test-crud", "doc-1", json); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	got, err := c.GetDocument(ctx, "test-crud", "doc-1")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if got == nil {
		return fmt.Errorf("get returned nil")
	}
	if *got != json {
		return fmt.Errorf("data mismatch: got %s, want %s", *got, json)
	}
	return nil
}

func testCrudList(ctx context.Context, c *peat.Client) error {
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("list-doc-%d", i)
		if err := c.PutDocument(ctx, "test-list", id, `{"i":true}`); err != nil {
			return fmt.Errorf("put %s: %w", id, err)
		}
	}
	ids, err := c.ListDocuments(ctx, "test-list")
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if len(ids) != 3 {
		return fmt.Errorf("expected 3 docs, got %d: %v", len(ids), ids)
	}
	return nil
}

func testCrudDelete(ctx context.Context, c *peat.Client) error {
	if err := c.PutDocument(ctx, "test-del", "to-delete", `{"x":1}`); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	if err := c.DeleteDocument(ctx, "test-del", "to-delete"); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	got, err := c.GetDocument(ctx, "test-del", "to-delete")
	if err != nil {
		return fmt.Errorf("get after delete: %w", err)
	}
	if got != nil {
		return fmt.Errorf("expected nil after delete, got %s", *got)
	}
	ids, err := c.ListDocuments(ctx, "test-del")
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}
	if len(ids) != 0 {
		return fmt.Errorf("expected 0 docs after delete, got %d", len(ids))
	}
	return nil
}

func testCrudOverwrite(ctx context.Context, c *peat.Client) error {
	if err := c.PutDocument(ctx, "test-ow", "doc-1", `{"v":1}`); err != nil {
		return fmt.Errorf("put v1: %w", err)
	}
	if err := c.PutDocument(ctx, "test-ow", "doc-1", `{"v":2}`); err != nil {
		return fmt.Errorf("put v2: %w", err)
	}
	got, err := c.GetDocument(ctx, "test-ow", "doc-1")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if got == nil || *got != `{"v":2}` {
		return fmt.Errorf("expected v2, got %v", got)
	}
	return nil
}

func testCrudInvalidJSON(ctx context.Context, c *peat.Client) error {
	err := c.PutDocument(ctx, "test-invalid", "bad", "not-json{{{")
	if err == nil {
		return fmt.Errorf("expected error for invalid JSON, got nil")
	}
	return nil
}

func testCrudGetNonexistent(ctx context.Context, c *peat.Client) error {
	got, err := c.GetDocument(ctx, "nonexistent-col", "no-such-doc")
	if err != nil {
		return fmt.Errorf("get returned error: %w", err)
	}
	if got != nil {
		return fmt.Errorf("expected nil, got %s", *got)
	}
	return nil
}

func testCrudMultiCollection(ctx context.Context, c *peat.Client) error {
	if err := c.PutDocument(ctx, "col-alpha", "d1", `{"a":1}`); err != nil {
		return fmt.Errorf("put alpha: %w", err)
	}
	if err := c.PutDocument(ctx, "col-beta", "d1", `{"b":1}`); err != nil {
		return fmt.Errorf("put beta: %w", err)
	}
	alphaIDs, err := c.ListDocuments(ctx, "col-alpha")
	if err != nil {
		return fmt.Errorf("list alpha: %w", err)
	}
	betaIDs, err := c.ListDocuments(ctx, "col-beta")
	if err != nil {
		return fmt.Errorf("list beta: %w", err)
	}
	if len(alphaIDs) != 1 || len(betaIDs) != 1 {
		return fmt.Errorf("expected 1 doc each, got alpha=%d beta=%d", len(alphaIDs), len(betaIDs))
	}
	// Verify content is independent
	gotA, _ := c.GetDocument(ctx, "col-alpha", "d1")
	gotB, _ := c.GetDocument(ctx, "col-beta", "d1")
	if gotA == nil || *gotA != `{"a":1}` {
		return fmt.Errorf("alpha content wrong: %v", gotA)
	}
	if gotB == nil || *gotB != `{"b":1}` {
		return fmt.Errorf("beta content wrong: %v", gotB)
	}
	return nil
}

func testTypedPlatform(ctx context.Context, c *peat.Client) error {
	want := &sidecarv1.Platform{
		Id:           "plat-001",
		PlatformType: "uds-remote-agent",
		Name:         "Functest Platform",
		Status:       sidecarv1.PlatformStatus_PLATFORM_STATUS_READY,
		Latitude:     38.8977,
		Longitude:    -77.0365,
		AltitudeM:    150.5,
		Readiness:    0.95,
		Capabilities: []string{"pkg-mgmt", "registry-sync"},
		UnitId:       strPtr("unit-42"),
		Callsign:     strPtr("EAGLE-1"),
	}
	if err := c.PutPlatform(ctx, want); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	platforms, err := c.GetPlatforms(ctx)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	got := findByID(platforms, "plat-001")
	if got == nil {
		return fmt.Errorf("platform plat-001 not found in %d platforms", len(platforms))
	}
	if got.Id != want.Id || got.PlatformType != want.PlatformType || got.Name != want.Name {
		return fmt.Errorf("basic fields mismatch")
	}
	if got.Status != want.Status {
		return fmt.Errorf("status: got %v, want %v", got.Status, want.Status)
	}
	if !floatEq(got.Latitude, want.Latitude) || !floatEq(got.Longitude, want.Longitude) {
		return fmt.Errorf("lat/lon mismatch: got %.4f,%.4f", got.Latitude, got.Longitude)
	}
	if !floatEq(got.AltitudeM, want.AltitudeM) || !floatEq(got.Readiness, want.Readiness) {
		return fmt.Errorf("altitude/readiness mismatch")
	}
	if len(got.Capabilities) != 2 {
		return fmt.Errorf("capabilities: got %v", got.Capabilities)
	}
	if got.UnitId == nil || *got.UnitId != "unit-42" {
		return fmt.Errorf("unit_id: got %v", got.UnitId)
	}
	if got.Callsign == nil || *got.Callsign != "EAGLE-1" {
		return fmt.Errorf("callsign: got %v", got.Callsign)
	}
	return nil
}

func testTypedCell(ctx context.Context, c *peat.Client) error {
	want := &sidecarv1.Cell{
		Id:              "cell-001",
		Name:            "Alpha Cell",
		Status:          sidecarv1.CellStatus_CELL_STATUS_ACTIVE,
		PlatformCount:   5,
		CenterLatitude:  34.0522,
		CenterLongitude: -118.2437,
		Capabilities:    []string{"recon", "relay"},
		FormationId:     strPtr("form-001"),
		LeaderId:        strPtr("plat-001"),
	}
	if err := c.PutCell(ctx, want); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	cells, err := c.GetCells(ctx)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if len(cells) == 0 {
		return fmt.Errorf("no cells returned")
	}
	got := cells[0]
	if got.Id != want.Id || got.Name != want.Name {
		return fmt.Errorf("basic fields mismatch: id=%s name=%s", got.Id, got.Name)
	}
	if got.Status != want.Status {
		return fmt.Errorf("status: got %v, want %v", got.Status, want.Status)
	}
	if got.PlatformCount != want.PlatformCount {
		return fmt.Errorf("platform_count: got %d", got.PlatformCount)
	}
	if !floatEq(got.CenterLatitude, want.CenterLatitude) || !floatEq(got.CenterLongitude, want.CenterLongitude) {
		return fmt.Errorf("center lat/lon mismatch")
	}
	if len(got.Capabilities) != 2 {
		return fmt.Errorf("capabilities: got %v", got.Capabilities)
	}
	if got.FormationId == nil || *got.FormationId != "form-001" {
		return fmt.Errorf("formation_id: got %v", got.FormationId)
	}
	if got.LeaderId == nil || *got.LeaderId != "plat-001" {
		return fmt.Errorf("leader_id: got %v", got.LeaderId)
	}
	return nil
}

func testTypedTrack(ctx context.Context, c *peat.Client) error {
	want := &sidecarv1.Track{
		Id:             "trk-001",
		SourcePlatform: "plat-001",
		CellId:         strPtr("cell-001"),
		FormationId:    strPtr("form-001"),
		Latitude:       35.0,
		Longitude:      -120.0,
		AltitudeM:      f64Ptr(3000.0),
		CepM:           f64Ptr(15.0),
		HeadingDeg:     f64Ptr(270.0),
		SpeedMps:       f64Ptr(250.0),
		Classification: "UNCLASSIFIED",
		Confidence:     0.92,
		Category:       sidecarv1.TrackCategory_TRACK_CATEGORY_AIR,
	}
	if err := c.PutTrack(ctx, want); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	tracks, err := c.GetTracks(ctx)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if len(tracks) == 0 {
		return fmt.Errorf("no tracks returned")
	}
	got := tracks[0]
	if got.Id != want.Id || got.SourcePlatform != want.SourcePlatform {
		return fmt.Errorf("basic fields mismatch")
	}
	if got.CellId == nil || *got.CellId != "cell-001" {
		return fmt.Errorf("cell_id: got %v", got.CellId)
	}
	if got.FormationId == nil || *got.FormationId != "form-001" {
		return fmt.Errorf("formation_id: got %v", got.FormationId)
	}
	if !floatEq(got.Latitude, want.Latitude) || !floatEq(got.Longitude, want.Longitude) {
		return fmt.Errorf("lat/lon mismatch")
	}
	if got.AltitudeM == nil || !floatEq(*got.AltitudeM, 3000.0) {
		return fmt.Errorf("altitude_m: got %v", got.AltitudeM)
	}
	if got.CepM == nil || !floatEq(*got.CepM, 15.0) {
		return fmt.Errorf("cep_m: got %v", got.CepM)
	}
	if got.HeadingDeg == nil || !floatEq(*got.HeadingDeg, 270.0) {
		return fmt.Errorf("heading_deg: got %v", got.HeadingDeg)
	}
	if got.SpeedMps == nil || !floatEq(*got.SpeedMps, 250.0) {
		return fmt.Errorf("speed_mps: got %v", got.SpeedMps)
	}
	if got.Classification != want.Classification {
		return fmt.Errorf("classification: got %s", got.Classification)
	}
	if !floatEq(got.Confidence, want.Confidence) {
		return fmt.Errorf("confidence: got %f", got.Confidence)
	}
	if got.Category != want.Category {
		return fmt.Errorf("category: got %v, want %v", got.Category, want.Category)
	}
	return nil
}

func testTypedCommand(ctx context.Context, c *peat.Client) error {
	now := time.Now().Unix()
	want := &sidecarv1.Command{
		Id:          "cmd-001",
		TargetId:    "plat-001",
		CommandType: "deploy-package",
		Status:      sidecarv1.CommandStatus_COMMAND_STATUS_PENDING,
		CreatedAt:   now,
		ExpiresAt:   now + 3600,
		PayloadJson: `{"package":"nginx","version":"1.25"}`,
	}
	if err := c.PutCommand(ctx, want); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	commands, err := c.GetCommands(ctx)
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if len(commands) == 0 {
		return fmt.Errorf("no commands returned")
	}
	got := commands[0]
	if got.Id != want.Id || got.TargetId != want.TargetId || got.CommandType != want.CommandType {
		return fmt.Errorf("basic fields mismatch: id=%s target=%s type=%s", got.Id, got.TargetId, got.CommandType)
	}
	if got.Status != want.Status {
		return fmt.Errorf("status: got %v, want %v", got.Status, want.Status)
	}
	if got.CreatedAt != want.CreatedAt || got.ExpiresAt != want.ExpiresAt {
		return fmt.Errorf("timestamps mismatch: created=%d expires=%d", got.CreatedAt, got.ExpiresAt)
	}
	if got.PayloadJson != want.PayloadJson {
		return fmt.Errorf("payload_json: got %s", got.PayloadJson)
	}
	return nil
}

func testSubscribeAll(ctx context.Context, c *peat.Client) error {
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()

	ch, _ := c.Subscribe(subCtx)
	// Give the stream a moment to establish
	time.Sleep(200 * time.Millisecond)

	if err := c.PutDocument(ctx, "sub-all", "doc-1", `{"sub":"all"}`); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	select {
	case evt := <-ch:
		if evt == nil {
			return fmt.Errorf("received nil event")
		}
		if evt.Collection != "sub-all" || evt.DocId != "doc-1" {
			return fmt.Errorf("unexpected event: col=%s id=%s", evt.Collection, evt.DocId)
		}
		if evt.ChangeType != sidecarv1.ChangeType_CHANGE_TYPE_UPSERT {
			return fmt.Errorf("expected UPSERT, got %v", evt.ChangeType)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for subscription event")
	}
}

func testSubscribeFiltered(ctx context.Context, c *peat.Client) error {
	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()

	ch, _ := c.Subscribe(subCtx, "filtered-col")
	time.Sleep(200 * time.Millisecond)

	// Write to non-filtered collection first
	if err := c.PutDocument(ctx, "other-col", "noise", `{"x":1}`); err != nil {
		return fmt.Errorf("put other: %w", err)
	}
	// Write to filtered collection
	if err := c.PutDocument(ctx, "filtered-col", "signal", `{"x":2}`); err != nil {
		return fmt.Errorf("put filtered: %w", err)
	}

	select {
	case evt := <-ch:
		if evt == nil {
			return fmt.Errorf("received nil event")
		}
		if evt.Collection != "filtered-col" {
			return fmt.Errorf("expected filtered-col, got %s (filter leak)", evt.Collection)
		}
		if evt.DocId != "signal" {
			return fmt.Errorf("expected doc signal, got %s", evt.DocId)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for filtered event")
	}
}

func testSubscribeDelete(ctx context.Context, c *peat.Client) error {
	// Pre-create a document to delete
	if err := c.PutDocument(ctx, "sub-del", "to-delete", `{"temp":true}`); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	subCtx, subCancel := context.WithTimeout(ctx, 10*time.Second)
	defer subCancel()

	ch, _ := c.Subscribe(subCtx, "sub-del")
	time.Sleep(200 * time.Millisecond)

	if err := c.DeleteDocument(ctx, "sub-del", "to-delete"); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	select {
	case evt := <-ch:
		if evt == nil {
			return fmt.Errorf("received nil event")
		}
		if evt.ChangeType != sidecarv1.ChangeType_CHANGE_TYPE_DELETE {
			return fmt.Errorf("expected DELETE, got %v", evt.ChangeType)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for delete event")
	}
}

func testTransportUDS(ctx context.Context, c *peat.Client) error {
	s, err := c.Status(ctx)
	if err != nil {
		return fmt.Errorf("status: %w", err)
	}
	if s.NodeId == "" {
		return fmt.Errorf("node_id empty over UDS")
	}
	// CRUD round-trip over UDS
	if err := c.PutDocument(ctx, "uds-test", "doc-1", `{"uds":true}`); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	got, err := c.GetDocument(ctx, "uds-test", "doc-1")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if got == nil || *got != `{"uds":true}` {
		return fmt.Errorf("data mismatch: %v", got)
	}
	return nil
}

func testEncryptTransparent(ctx context.Context, c *peat.Client) error {
	json := `{"secret":"classified-data"}`
	if err := c.PutDocument(ctx, "enc-test", "doc-1", json); err != nil {
		return fmt.Errorf("put: %w", err)
	}
	got, err := c.GetDocument(ctx, "enc-test", "doc-1")
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if got == nil {
		return fmt.Errorf("get returned nil")
	}
	if *got != json {
		return fmt.Errorf("transparent decrypt failed: got %s", *got)
	}
	return nil
}

func testEncryptAtRest(ctx context.Context, encClient, plainClient *peat.Client) error {
	// Write on encrypted node (doc already written by transparent test)
	// Wait for sync to plaintext peer
	var data *string
	err := pollUntil(ctx, time.Second, 30*time.Second, func() (bool, error) {
		var err error
		data, err = plainClient.GetDocument(ctx, "enc-test", "doc-1")
		if err != nil {
			return false, err
		}
		return data != nil, nil
	})
	if err != nil {
		return fmt.Errorf("sync to plain peer: %w", err)
	}
	if !strings.HasPrefix(*data, "ENC:v1:") {
		return fmt.Errorf("expected ENC:v1: prefix on plain peer, got: %s", *data)
	}
	return nil
}

func testPeerConnect(ctx context.Context, clientA, clientB *peat.Client, aAddr string) error {
	statusA, err := clientA.Status(ctx)
	if err != nil {
		return fmt.Errorf("status a: %w", err)
	}
	if err := clientB.ConnectPeer(ctx, statusA.EndpointAddr, []string{aAddr}, ""); err != nil {
		return fmt.Errorf("connect peer: %w", err)
	}
	time.Sleep(2 * time.Second)

	peers, err := clientB.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("list peers b: %w", err)
	}
	if len(peers) == 0 {
		return fmt.Errorf("node-b has 0 peers after connect")
	}

	// Start sync on both for subsequent tests
	if err := clientA.StartSync(ctx); err != nil {
		return fmt.Errorf("start sync a: %w", err)
	}
	if err := clientB.StartSync(ctx); err != nil {
		return fmt.Errorf("start sync b: %w", err)
	}
	return nil
}

func testSyncAtoB(ctx context.Context, clientA, clientB *peat.Client) error {
	// Generic document
	if err := clientA.PutDocument(ctx, "sync-test", "from-a", `{"origin":"a"}`); err != nil {
		return fmt.Errorf("put doc: %w", err)
	}
	// Typed platform
	if err := clientA.PutPlatform(ctx, &sidecarv1.Platform{
		Id:           "sync-plat-a",
		PlatformType: "test",
		Name:         "Sync Platform A",
		Status:       sidecarv1.PlatformStatus_PLATFORM_STATUS_READY,
		Latitude:     40.0,
		Longitude:    -75.0,
		Capabilities: []string{"sync"},
	}); err != nil {
		return fmt.Errorf("put platform: %w", err)
	}

	// Poll node-b for generic doc
	var data *string
	err := pollUntil(ctx, time.Second, 30*time.Second, func() (bool, error) {
		var err error
		data, err = clientB.GetDocument(ctx, "sync-test", "from-a")
		if err != nil {
			return false, err
		}
		return data != nil, nil
	})
	if err != nil {
		return fmt.Errorf("doc sync a->b: %w", err)
	}
	if *data != `{"origin":"a"}` {
		return fmt.Errorf("doc content mismatch: %s", *data)
	}

	// Poll node-b for platform
	err = pollUntil(ctx, time.Second, 30*time.Second, func() (bool, error) {
		platforms, err := clientB.GetPlatforms(ctx)
		if err != nil {
			return false, err
		}
		return findByID(platforms, "sync-plat-a") != nil, nil
	})
	if err != nil {
		return fmt.Errorf("platform sync a->b: %w", err)
	}
	return nil
}

func testSyncBtoA(ctx context.Context, clientA, clientB *peat.Client) error {
	if err := clientB.PutDocument(ctx, "sync-test", "from-b", `{"origin":"b"}`); err != nil {
		return fmt.Errorf("put: %w", err)
	}

	var data *string
	err := pollUntil(ctx, time.Second, 30*time.Second, func() (bool, error) {
		var err error
		data, err = clientA.GetDocument(ctx, "sync-test", "from-b")
		if err != nil {
			return false, err
		}
		return data != nil, nil
	})
	if err != nil {
		return fmt.Errorf("doc sync b->a: %w", err)
	}
	if *data != `{"origin":"b"}` {
		return fmt.Errorf("doc content mismatch: %s", *data)
	}
	return nil
}

func testSyncStats(ctx context.Context, c *peat.Client) error {
	stats, err := c.SyncStats(ctx)
	if err != nil {
		return fmt.Errorf("sync stats: %w", err)
	}
	if !stats.SyncActive {
		return fmt.Errorf("sync_active is false")
	}
	if stats.ConnectedPeers < 1 {
		return fmt.Errorf("connected_peers=%d, expected >= 1", stats.ConnectedPeers)
	}
	return nil
}

func testSyncControlStop(ctx context.Context, clientA, clientB *peat.Client) error {
	// StopSync sets the sync_active flag to false (reported in stats)
	if err := clientB.StopSync(ctx); err != nil {
		return fmt.Errorf("stop sync b: %w", err)
	}
	stats, err := clientB.SyncStats(ctx)
	if err != nil {
		return fmt.Errorf("sync stats: %w", err)
	}
	if stats.SyncActive {
		return fmt.Errorf("sync_active should be false after StopSync")
	}
	return nil
}

func testSyncControlResume(ctx context.Context, clientA, clientB *peat.Client) error {
	// StartSync sets the flag back and triggers a full sync with peers
	if err := clientB.StartSync(ctx); err != nil {
		return fmt.Errorf("start sync b: %w", err)
	}
	stats, err := clientB.SyncStats(ctx)
	if err != nil {
		return fmt.Errorf("sync stats: %w", err)
	}
	if !stats.SyncActive {
		return fmt.Errorf("sync_active should be true after StartSync")
	}
	return nil
}

func testPeerDisconnect(ctx context.Context, clientA, clientB *peat.Client) error {
	statusA, err := clientA.Status(ctx)
	if err != nil {
		return fmt.Errorf("status a: %w", err)
	}
	if err := clientB.DisconnectPeer(ctx, statusA.EndpointAddr); err != nil {
		return fmt.Errorf("disconnect: %w", err)
	}
	peers, err := clientB.ListPeers(ctx)
	if err != nil {
		return fmt.Errorf("list peers: %w", err)
	}
	if len(peers) != 0 {
		return fmt.Errorf("expected 0 peers after disconnect, got %d", len(peers))
	}
	return nil
}

func testFormationIsolation(ctx context.Context, clientA, clientC *peat.Client, aAddr string) error {
	statusA, err := clientA.Status(ctx)
	if err != nil {
		return fmt.Errorf("status a: %w", err)
	}

	// Attempt to connect across formations — may succeed at transport level or fail
	_ = clientC.ConnectPeer(ctx, statusA.EndpointAddr, []string{aAddr}, "")
	time.Sleep(2 * time.Second)

	_ = clientC.StartSync(ctx)
	_ = clientA.StartSync(ctx)

	// Write on node-c (different formation)
	if err := clientC.PutDocument(ctx, "isolation-test", "from-c", `{"formation":"bravo"}`); err != nil {
		return fmt.Errorf("put on c: %w", err)
	}

	// Wait 5 seconds, verify node-a does NOT have it
	time.Sleep(5 * time.Second)
	got, err := clientA.GetDocument(ctx, "isolation-test", "from-c")
	if err != nil {
		return fmt.Errorf("get on a: %w", err)
	}
	if got != nil {
		return fmt.Errorf("data leaked across formations: %s", *got)
	}
	return nil
}

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

type section struct {
	name string
	fn   func(context.Context) error
}

func runSections(ctx context.Context, sections []section) error {
	for _, s := range sections {
		fmt.Printf("--- %s ---\n", s.name)
		if err := s.fn(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL: %s: %v\n", s.name, err)
			return fmt.Errorf("%s: %w", s.name, err)
		}
		fmt.Printf("PASS: %s\n", s.name)
	}
	return nil
}

type nodeOpts struct {
	listen        string
	nodeID        string
	dataDir       string
	appID         string
	sharedKey     string
	encryptionKey string
	irohUDPPort   int
}

type nodeProc struct {
	cmd *exec.Cmd
}

func startNode(ctx context.Context, bin string, opts nodeOpts) (*nodeProc, error) {
	if err := os.MkdirAll(opts.dataDir, 0o755); err != nil {
		return nil, err
	}

	args := []string{
		"--listen", opts.listen,
		"--data-dir", opts.dataDir,
		"--node-id", opts.nodeID,
	}
	if opts.appID != "" {
		args = append(args, "--app-id", opts.appID)
	}
	if opts.sharedKey != "" {
		args = append(args, "--shared-key", opts.sharedKey)
	}
	if opts.encryptionKey != "" {
		args = append(args, "--encryption-key", opts.encryptionKey)
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

	return &nodeProc{cmd: cmd}, nil
}

func (n *nodeProc) stop() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
}

func startAndConnect(ctx context.Context, bin string, opts nodeOpts) (*nodeProc, *peat.Client, error) {
	proc, err := startNode(ctx, bin, opts)
	if err != nil {
		return nil, nil, err
	}

	target := opts.listen
	if strings.HasPrefix(target, "tcp://") {
		target = "http://" + strings.TrimPrefix(target, "tcp://")
	} else if strings.HasPrefix(target, "unix://") {
		// Keep as-is for peat.Connect
	}

	client, err := waitReady(ctx, target)
	if err != nil {
		proc.stop()
		return nil, nil, err
	}

	return proc, client, nil
}

func waitReady(ctx context.Context, target string) (*peat.Client, error) {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		client, err := peat.Connect(target)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if _, err := client.Status(ctx); err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return client, nil
	}
	return nil, fmt.Errorf("node not ready at %s: %v", target, lastErr)
}

func pollUntil(ctx context.Context, interval, maxWait time.Duration, check func() (bool, error)) error {
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		ok, err := check()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
	return fmt.Errorf("condition not met within %s", maxWait)
}

func resolveBinary() string {
	bin := os.Getenv("PEAT_NODE_BIN")
	if bin == "" {
		bin = "peat-node"
	}
	if _, err := exec.LookPath(bin); err != nil {
		candidate := filepath.Join("..", "..", "target", "release", "peat-node")
		if _, err2 := os.Stat(candidate); err2 == nil {
			return candidate
		}
	}
	return bin
}

func findByID(platforms []*sidecarv1.Platform, id string) *sidecarv1.Platform {
	for _, p := range platforms {
		if p.Id == id {
			return p
		}
	}
	return nil
}

func floatEq(a, b float64) bool {
	return math.Abs(a-b) < 0.001
}

func strPtr(s string) *string   { return &s }
func f64Ptr(f float64) *float64 { return &f }

func repeatByte(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}
