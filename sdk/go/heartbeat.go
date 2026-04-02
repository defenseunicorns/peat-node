package peat

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// AgentHeartbeat contains the status fields an agent pushes to its sidecar.
// This is the primary integration point for UDS Remote Agent.
type AgentHeartbeat struct {
	AgentID        string            `json:"agent_id"`
	PlatformType   string            `json:"platform_type,omitempty"`
	Version        string            `json:"version,omitempty"`
	Architecture   string            `json:"architecture,omitempty"`
	Classification string            `json:"classification,omitempty"`
	K8sVersion     string            `json:"k8s_version,omitempty"`
	K8sNodeStatus  string            `json:"k8s_node_status,omitempty"`
	ZarfVersion    string            `json:"zarf_version,omitempty"`
	RunMode        string            `json:"run_mode,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	LastSeen       int64             `json:"last_seen"`
}

// DeploymentStatus describes a deployed package on this agent.
type DeploymentStatus struct {
	AgentID           string            `json:"agent_id"`
	Package           string            `json:"package"`
	Version           string            `json:"version,omitempty"`
	Status            string            `json:"status,omitempty"`
	Flavor            string            `json:"flavor,omitempty"`
	NamespaceOverride string            `json:"namespace_override,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	LastSeen          int64             `json:"last_seen"`
}

// Heartbeat pushes the agent's current status to the sidecar's CRDT mesh.
// The status is written to the "platforms/{agentID}" collection and
// automatically replicates to all connected peers.
func (c *Client) Heartbeat(ctx context.Context, hb *AgentHeartbeat) error {
	hb.LastSeen = time.Now().Unix()
	if hb.PlatformType == "" {
		hb.PlatformType = "uds-remote-agent"
	}
	data, err := json.Marshal(hb)
	if err != nil {
		return fmt.Errorf("marshal heartbeat: %w", err)
	}
	return c.PutDocument(ctx, "platforms", hb.AgentID, string(data))
}

// ReportDeployment pushes a single package deployment status to the mesh.
// Written to "deployments/{agentID}:{package}" and replicates to peers.
func (c *Client) ReportDeployment(ctx context.Context, ds *DeploymentStatus) error {
	ds.LastSeen = time.Now().Unix()
	data, err := json.Marshal(ds)
	if err != nil {
		return fmt.Errorf("marshal deployment: %w", err)
	}
	docID := fmt.Sprintf("%s:%s", ds.AgentID, ds.Package)
	return c.PutDocument(ctx, "deployments", docID, string(data))
}

// FleetPlatforms returns the status of all agents visible in the CRDT mesh.
// Each entry was written by a different agent's Heartbeat() call and
// replicated via peer-to-peer CRDT sync.
func (c *Client) FleetPlatforms(ctx context.Context) ([]AgentHeartbeat, error) {
	docIDs, err := c.ListDocuments(ctx, "platforms")
	if err != nil {
		return nil, err
	}
	var platforms []AgentHeartbeat
	for _, id := range docIDs {
		raw, err := c.GetDocument(ctx, "platforms", id)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		var hb AgentHeartbeat
		if err := json.Unmarshal([]byte(*raw), &hb); err != nil {
			continue // skip malformed
		}
		platforms = append(platforms, hb)
	}
	return platforms, nil
}

// FleetDeployments returns all deployments visible in the mesh, optionally
// filtered to a specific agent. Pass "" for agentID to get all.
func (c *Client) FleetDeployments(ctx context.Context, agentID string) ([]DeploymentStatus, error) {
	docIDs, err := c.ListDocuments(ctx, "deployments")
	if err != nil {
		return nil, err
	}
	var deployments []DeploymentStatus
	for _, id := range docIDs {
		raw, err := c.GetDocument(ctx, "deployments", id)
		if err != nil {
			return nil, err
		}
		if raw == nil {
			continue
		}
		var ds DeploymentStatus
		if err := json.Unmarshal([]byte(*raw), &ds); err != nil {
			continue
		}
		if agentID != "" && ds.AgentID != agentID {
			continue
		}
		deployments = append(deployments, ds)
	}
	return deployments, nil
}
