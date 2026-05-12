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
{{- end -}}
