{{- define "js.name" -}}jobscheduler{{- end -}}

{{- define "js.fullname" -}}
{{- if contains "jobscheduler" .Release.Name -}}{{ .Release.Name | trunc 40 | trimSuffix "-" }}
{{- else -}}{{ printf "%s-jobscheduler" .Release.Name | trunc 40 | trimSuffix "-" }}{{- end -}}
{{- end -}}

{{- define "js.labels" -}}
app.kubernetes.io/name: {{ include "js.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* selector labels for one component: call with (dict "ctx" . "component" "worker") */}}
{{- define "js.selector" -}}
app.kubernetes.io/name: {{ include "js.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* image reference: call with (dict "ctx" . "app" "worker") */}}
{{- define "js.image" -}}
{{ .ctx.Values.image.registry }}/{{ .app }}:{{ .ctx.Values.image.tag | default .ctx.Chart.AppVersion }}
{{- end -}}

{{- define "js.secretName" -}}
{{- if .Values.database.existingSecret -}}{{ .Values.database.existingSecret }}{{- else -}}{{ include "js.fullname" . }}-db{{- end -}}
{{- end -}}

{{- define "js.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}{{ include "js.fullname" . }}{{- else -}}default{{- end -}}
{{- end -}}

{{/* Env shared by every component: call with (dict "ctx" . "maxConns" .Values.<component>.maxConns). */}}
{{- define "js.commonEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "js.secretName" .ctx }}
      key: DATABASE_URL
- name: DB_MAX_CONNS
  value: {{ .maxConns | quote }}
- name: LOG_LEVEL
  value: {{ .ctx.Values.logLevel | quote }}
{{- if .ctx.Values.otlpEndpoint }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .ctx.Values.otlpEndpoint | quote }}
{{- end }}
{{- end -}}

{{/* Hardened container security context: non-root, read-only filesystem, no capabilities. */}}
{{- define "js.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* Seconds Kubernetes waits after SIGTERM before SIGKILL for a worker. */}}
{{- define "js.workerTerminationGrace" -}}
{{ add (int .Values.worker.maxJobTimeoutSeconds) 15 }}
{{- end -}}
