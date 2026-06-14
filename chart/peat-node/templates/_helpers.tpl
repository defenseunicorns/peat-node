{{/*
Standard name helpers
*/}}
{{- define "peat-node.name" -}}
peat-node
{{- end -}}

{{- define "peat-node.fullname" -}}
{{ .Release.Name }}-peat-node
{{- end -}}

{{- define "peat-node.labels" -}}
app.kubernetes.io/name: peat-node
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "peat-node.selectorLabels" -}}
app.kubernetes.io/name: peat-node
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
peat-node container spec — inject this into any pod as an additional container.

Usage in a parent chart:
  containers:
    - name: my-app
      ...
    {{- include "peat-node.container" .Subcharts.peat-node | nindent 4 }}
*/}}

{{- define "peat-node.container" -}}
- name: peat-node
  image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  env:
    - name: PEAT_NODE_LISTEN
      value: {{ .Values.listen | quote }}
    - name: PEAT_NODE_DATA_DIR
      value: /data/peat-node
    {{- if .Values.nodeId }}
    - name: PEAT_NODE_NODE_ID
      value: {{ .Values.nodeId | quote }}
    {{- end }}
    - name: PEAT_NODE_APP_ID
      value: {{ .Values.appId | quote }}
    {{- if .Values.sharedKey }}
    - name: PEAT_NODE_SHARED_KEY
      value: {{ .Values.sharedKey | quote }}
    {{- end }}
    {{- if .Values.encryptionKey }}
    - name: PEAT_NODE_ENCRYPTION_KEY
      value: {{ .Values.encryptionKey | quote }}
    {{- else if and .Values.encryptionKeySecret.name .Values.encryptionKeySecret.key }}
    - name: PEAT_NODE_ENCRYPTION_KEY
      valueFrom:
        secretKeyRef:
          name: {{ .Values.encryptionKeySecret.name }}
          key: {{ .Values.encryptionKeySecret.key }}
    {{- end }}
    {{- if .Values.peers }}
    - name: PEAT_NODE_PEERS
      value: {{ join "," .Values.peers | quote }}
    {{- end }}
    {{- if .Values.autoSync }}
    - name: PEAT_NODE_AUTO_SYNC
      value: "true"
    {{- end }}
    {{- if gt (int .Values.irohUdpPort) 0 }}
    - name: PEAT_NODE_IROH_UDP_PORT
      value: {{ .Values.irohUdpPort | quote }}
    {{- end }}
    {{- if .Values.agentAddr }}
    - name: PEAT_NODE_AGENT_ADDR
      value: {{ .Values.agentAddr | quote }}
    - name: PEAT_NODE_AGENT_POLL_INTERVAL
      value: {{ .Values.agentPollInterval | quote }}
    {{- if .Values.agentTls.enabled }}
    - name: PEAT_NODE_AGENT_TLS_CERT
      value: /etc/peat-node/agent-tls/tls.crt
    - name: PEAT_NODE_AGENT_TLS_KEY
      value: /etc/peat-node/agent-tls/tls.key
    - name: PEAT_NODE_AGENT_TLS_CA
      value: /etc/peat-node/agent-tls/ca.crt
    {{- end }}
    {{- end }}
    {{- if .Values.verbose }}
    - name: RUST_LOG
      value: "peat_node=debug,peat_mesh=debug"
    {{- end }}
    {{- /* PRD-006 attachment distribution.
           Sender-side vars (roots, caps, priority) are only emitted when
           roots are configured. Receiver-side vars (inbox) are emitted
           independently — a receive-only node needs no roots. Shared vars
           (handle retention, bundle cap) are emitted when either side is
           active. */}}
    {{- if .Values.attachment.roots }}
    - name: PEAT_NODE_ATTACHMENT_ROOT
      value: "{{- $first := true -}}
        {{- range $name, $path := .Values.attachment.roots -}}
          {{- if not $first -}},{{- end -}}{{- $name -}}={{- $path -}}{{- $first = false -}}
        {{- end -}}"
    {{- /* int64 coercion before quote — YAML-parsed numbers come in as
           float64 in Helm/Sprig, and `quote` on a float produces
           scientific notation (`2.68435456e+08`) which clap rejects. */}}
    - name: PEAT_NODE_ATTACHMENT_MAX_FILE_BYTES
      value: {{ .Values.attachment.maxFileBytes | int64 | quote }}
    - name: PEAT_NODE_ATTACHMENT_MAX_BUNDLE_BYTES
      value: {{ .Values.attachment.maxBundleBytes | int64 | quote }}
    - name: PEAT_NODE_ATTACHMENT_MAX_FILES_PER_BUNDLE
      value: {{ .Values.attachment.maxFilesPerBundle | int64 | quote }}
    - name: PEAT_NODE_ATTACHMENT_MAX_NODE_LIST_LEN
      value: {{ .Values.attachment.maxNodeListLen | int64 | quote }}
    - name: PEAT_NODE_ATTACHMENT_MAX_CONCURRENT_DISTRIBUTIONS
      value: {{ .Values.attachment.maxConcurrentDistributions | int64 | quote }}
    {{- if .Values.attachment.queueWhenFull }}
    - name: PEAT_NODE_ATTACHMENT_QUEUE_WHEN_FULL
      value: "true"
    {{- end }}
    - name: PEAT_NODE_ATTACHMENT_DEFAULT_PRIORITY
      value: {{ .Values.attachment.defaultPriority | quote }}
    - name: PEAT_NODE_ATTACHMENT_DISCOVERY_GRACE_SECS
      value: {{ .Values.attachment.discoveryGraceSecs | int64 | quote }}
    {{- end }}
    {{- /* Shared retention knobs — active whenever send or receive is on. */}}
    {{- if or .Values.attachment.roots .Values.attachment.inbox }}
    - name: PEAT_NODE_ATTACHMENT_HANDLE_RETENTION_SECS
      value: {{ .Values.attachment.handleRetentionSecs | int64 | quote }}
    - name: PEAT_NODE_ATTACHMENT_MAX_KNOWN_BUNDLES
      value: {{ .Values.attachment.maxKnownBundles | int64 | quote }}
    {{- end }}
    {{- /* Receiver-side inbox (PRD-006 v1.1). */}}
    {{- if .Values.attachment.inbox }}
    - name: PEAT_NODE_ATTACHMENT_INBOX
      value: {{ .Values.attachment.inbox | quote }}
    - name: PEAT_NODE_ATTACHMENT_INBOX_POLL_SECS
      value: {{ .Values.attachment.inboxPollSecs | int64 | quote }}
    {{- end }}
    {{- /* mDNS — default true (disabled) for k8s; set disableMdns: false
           for bare-metal chart deployments that want local discovery. */}}
    {{- if .Values.disableMdns }}
    - name: PEAT_NODE_DISABLE_MDNS
      value: "true"
    {{- end }}
    {{- /* POD_NAME — downward API; always injected (useful for logs and
           required by K8s peer discovery for deterministic iroh keypair
           derivation). */}}
    - name: POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name
    {{- /* Kubernetes peer discovery (peat-node#63). */}}
    {{- if .Values.discovery.enabled }}
    - name: PEAT_NODE_ENABLE_KUBERNETES_DISCOVERY
      value: "true"
    - name: PEAT_NODE_DISCOVERY_LABEL_SELECTOR
      value: {{ .Values.discovery.labelSelector | quote }}
    - name: PEAT_NODE_DISCOVERY_ANNOTATION_PREFIX
      value: {{ .Values.discovery.annotationPrefix | quote }}
    - name: PEAT_NODE_DISCOVERY_INTERVAL_SECS
      value: {{ .Values.discovery.intervalSecs | int64 | quote }}
    {{- if .Values.discovery.namespace }}
    - name: PEAT_NODE_DISCOVERY_NAMESPACE
      value: {{ .Values.discovery.namespace | quote }}
    {{- end }}
    {{- end }}
  ports:
    {{- if hasPrefix "tcp://" .Values.listen }}
    - name: grpc
      containerPort: {{ (split ":" .Values.listen)._2 }}
      protocol: TCP
    {{- end }}
    {{- if gt (int .Values.irohUdpPort) 0 }}
    - name: iroh-quic
      containerPort: {{ .Values.irohUdpPort }}
      protocol: UDP
      {{- if .Values.irohUdpHostPort }}
      hostPort: {{ .Values.irohUdpPort }}
      {{- end }}
    {{- end }}
  livenessProbe:
    {{- if hasPrefix "tcp://" .Values.listen }}
    tcpSocket:
      port: grpc
    {{- else }}
    exec:
      command: ["test", "-S", {{ trimPrefix "unix://" .Values.listen | quote }}]
    {{- end }}
    initialDelaySeconds: 5
    periodSeconds: 30
  readinessProbe:
    {{- if hasPrefix "tcp://" .Values.listen }}
    tcpSocket:
      port: grpc
    {{- else }}
    exec:
      command: ["test", "-S", {{ trimPrefix "unix://" .Values.listen | quote }}]
    {{- end }}
    initialDelaySeconds: 3
    periodSeconds: 10
  resources:
    {{- toYaml .Values.resources | nindent 4 }}
  volumeMounts:
    - name: peat-node-data
      mountPath: /data/peat-node
    {{- if hasPrefix "unix://" .Values.listen }}
    - name: peat-node-socket
      mountPath: {{ dir (trimPrefix "unix://" .Values.listen) }}
    {{- end }}
    {{- if and .Values.agentTls.enabled .Values.agentTls.secretName }}
    - name: peat-node-agent-tls
      mountPath: /etc/peat-node/agent-tls
      readOnly: true
    {{- end }}
    {{- /* PRD-006 attachment-root mounts. Operator-supplied — the chart
           just appends them. Each mount path should match the
           corresponding `attachment.roots` value. */}}
    {{- with .Values.attachment.extraVolumeMounts }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
{{- end -}}

{{/*
peat-node volumes — add these to the pod spec.

Usage:
  volumes:
    {{- include "peat-node.volumes" .Subcharts.peat-node | nindent 4 }}
*/}}
{{- define "peat-node.volumes" -}}
- name: peat-node-data
  {{- if .Values.persistence.enabled }}
  persistentVolumeClaim:
    claimName: {{ include "peat-node.fullname" . }}-data
  {{- else }}
  emptyDir: {}
  {{- end }}
{{- if hasPrefix "unix://" .Values.listen }}
- name: peat-node-socket
  emptyDir: {}
{{- end }}
{{- if and .Values.agentTls.enabled .Values.agentTls.secretName }}
- name: peat-node-agent-tls
  secret:
    secretName: {{ .Values.agentTls.secretName }}
{{- end }}
{{- /* PRD-006 attachment-root volumes (operator-supplied). */}}
{{- with .Values.attachment.extraVolumes }}
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}
