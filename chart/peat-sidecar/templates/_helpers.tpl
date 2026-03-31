{{/*
Standard name helpers
*/}}
{{- define "peat-sidecar.name" -}}
peat-sidecar
{{- end -}}

{{- define "peat-sidecar.fullname" -}}
{{ .Release.Name }}-peat-sidecar
{{- end -}}

{{- define "peat-sidecar.labels" -}}
app.kubernetes.io/name: peat-sidecar
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "peat-sidecar.selectorLabels" -}}
app.kubernetes.io/name: peat-sidecar
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
peat-sidecar container spec — inject this into any pod as an additional container.

Usage in a parent chart:
  containers:
    - name: my-app
      ...
    {{- include "peat-sidecar.container" .Subcharts.peat-sidecar | nindent 4 }}
*/}}

{{- define "peat-sidecar.container" -}}
- name: peat-sidecar
  image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  env:
    - name: PEAT_SIDECAR_LISTEN
      value: {{ .Values.listen | quote }}
    - name: PEAT_SIDECAR_DATA_DIR
      value: /data/peat-sidecar
    {{- if .Values.nodeId }}
    - name: PEAT_SIDECAR_NODE_ID
      value: {{ .Values.nodeId | quote }}
    {{- end }}
    - name: PEAT_SIDECAR_APP_ID
      value: {{ .Values.appId | quote }}
    {{- if .Values.sharedKey }}
    - name: PEAT_SIDECAR_SHARED_KEY
      value: {{ .Values.sharedKey | quote }}
    {{- end }}
    {{- if .Values.encryptionKey }}
    - name: PEAT_SIDECAR_ENCRYPTION_KEY
      value: {{ .Values.encryptionKey | quote }}
    {{- else if and .Values.encryptionKeySecret.name .Values.encryptionKeySecret.key }}
    - name: PEAT_SIDECAR_ENCRYPTION_KEY
      valueFrom:
        secretKeyRef:
          name: {{ .Values.encryptionKeySecret.name }}
          key: {{ .Values.encryptionKeySecret.key }}
    {{- end }}
    {{- if .Values.peers }}
    - name: PEAT_SIDECAR_PEERS
      value: {{ join "," .Values.peers | quote }}
    {{- end }}
    {{- if .Values.autoSync }}
    - name: PEAT_SIDECAR_AUTO_SYNC
      value: "true"
    {{- end }}
    {{- if .Values.agentAddr }}
    - name: PEAT_SIDECAR_AGENT_ADDR
      value: {{ .Values.agentAddr | quote }}
    - name: PEAT_SIDECAR_AGENT_POLL_INTERVAL
      value: {{ .Values.agentPollInterval | quote }}
    {{- if .Values.agentTls.enabled }}
    - name: PEAT_SIDECAR_AGENT_TLS_CERT
      value: /etc/peat-sidecar/agent-tls/tls.crt
    - name: PEAT_SIDECAR_AGENT_TLS_KEY
      value: /etc/peat-sidecar/agent-tls/tls.key
    - name: PEAT_SIDECAR_AGENT_TLS_CA
      value: /etc/peat-sidecar/agent-tls/ca.crt
    {{- end }}
    {{- end }}
    {{- if .Values.verbose }}
    - name: RUST_LOG
      value: "peat_sidecar=debug,peat_mesh=debug"
    {{- end }}
  ports:
    {{- if hasPrefix "tcp://" .Values.listen }}
    - name: grpc
      containerPort: {{ (split ":" .Values.listen)._2 }}
      protocol: TCP
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
    - name: peat-sidecar-data
      mountPath: /data/peat-sidecar
    {{- if hasPrefix "unix://" .Values.listen }}
    - name: peat-sidecar-socket
      mountPath: {{ dir (trimPrefix "unix://" .Values.listen) }}
    {{- end }}
    {{- if and .Values.agentTls.enabled .Values.agentTls.secretName }}
    - name: peat-sidecar-agent-tls
      mountPath: /etc/peat-sidecar/agent-tls
      readOnly: true
    {{- end }}
{{- end -}}

{{/*
peat-sidecar volumes — add these to the pod spec.

Usage:
  volumes:
    {{- include "peat-sidecar.volumes" .Subcharts.peat-sidecar | nindent 4 }}
*/}}
{{- define "peat-sidecar.volumes" -}}
- name: peat-sidecar-data
  {{- if .Values.persistence.enabled }}
  persistentVolumeClaim:
    claimName: {{ include "peat-sidecar.fullname" . }}-data
  {{- else }}
  emptyDir: {}
  {{- end }}
{{- if hasPrefix "unix://" .Values.listen }}
- name: peat-sidecar-socket
  emptyDir: {}
{{- end }}
{{- if and .Values.agentTls.enabled .Values.agentTls.secretName }}
- name: peat-sidecar-agent-tls
  secret:
    secretName: {{ .Values.agentTls.secretName }}
{{- end }}
{{- end -}}
