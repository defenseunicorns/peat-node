# peat-sidecar Go SDK

Go client library for integrating with peat-sidecar's Connect RPC API.

## Install

```bash
go get github.com/defenseunicorns/peat-sidecar/sdk/go@latest
```

## Usage

```go
import peat "github.com/defenseunicorns/peat-sidecar/sdk/go"

// Connect to the co-located sidecar
client, err := peat.Connect("http://localhost:50051")
// or Unix socket:
client, err := peat.Connect("unix:///var/run/peat.sock")

// Push heartbeat (agent → mesh)
err = client.Heartbeat(ctx, &peat.AgentHeartbeat{
    AgentID:      "my-agent",
    Version:      "0.1.0",
    Architecture: "amd64",
    K8sVersion:   "v1.33.0",
    RunMode:      "connected",
    Labels:       map[string]string{"region": "us-east-1"},
})

// Report a deployment
err = client.ReportDeployment(ctx, &peat.DeploymentStatus{
    AgentID: "my-agent",
    Package: "nginx",
    Version: "1.25.0",
    Status:  "deployed",
})

// Query fleet-wide state (all agents in the mesh)
platforms, err := client.FleetPlatforms(ctx)
deployments, err := client.FleetDeployments(ctx, "") // all agents

// Watch for commands
changes, errCh := client.Subscribe(ctx, "commands")
for change := range changes {
    fmt.Printf("command: %s\n", change.GetJsonData())
}
```

## API

### Agent → Sidecar (push state into the mesh)

| Method | Description |
|--------|-------------|
| `Heartbeat(ctx, *AgentHeartbeat)` | Push agent status to `platforms/{agentID}` |
| `ReportDeployment(ctx, *DeploymentStatus)` | Push package status to `deployments/{agentID}:{pkg}` |
| `PutDocument(ctx, collection, docID, json)` | Write any JSON document |
| `PutPlatform(ctx, *Platform)` | Write typed platform (proto) |
| `PutCommand(ctx, *Command)` | Write a command |

### Sidecar → Agent (query fleet state)

| Method | Description |
|--------|-------------|
| `FleetPlatforms(ctx)` | All agents' heartbeats from the mesh |
| `FleetDeployments(ctx, agentID)` | Deployments (filter by agent or all) |
| `GetDocument(ctx, collection, docID)` | Read any document |
| `GetPlatforms(ctx)` | All platforms (proto typed) |
| `GetCommands(ctx)` | All commands in the mesh |
| `ListDocuments(ctx, collection)` | List doc IDs in a collection |

### Subscriptions

| Method | Description |
|--------|-------------|
| `Subscribe(ctx, ...collections)` | Stream changes (returns channels) |

### Mesh control

| Method | Description |
|--------|-------------|
| `Status(ctx)` | Node ID, endpoint, sync state |
| `ConnectPeer(ctx, endpointID)` | Connect to a mesh peer |
| `ListPeers(ctx)` | Connected peers |
| `StartSync(ctx)` / `StopSync(ctx)` | Sync lifecycle |

## Example

See [`example/agent-integration/`](example/agent-integration/) for a runnable demo showing heartbeats, fleet queries, and command subscriptions.
