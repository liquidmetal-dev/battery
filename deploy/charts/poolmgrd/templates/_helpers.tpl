{{/*
Chart name, truncated/sanitized for use in resource names.
*/}}
{{- define "poolmgrd.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified app name: "<release>-poolmgrd", or just the release name if
it already contains "poolmgrd".
*/}}
{{- define "poolmgrd.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "poolmgrd.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "poolmgrd.labels" -}}
helm.sh/chart: {{ include "poolmgrd.chart" . }}
{{ include "poolmgrd.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "poolmgrd.selectorLabels" -}}
app.kubernetes.io/name: {{ include "poolmgrd.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "poolmgrd.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- .Values.serviceAccount.name | default (include "poolmgrd.fullname" .) -}}
{{- else -}}
{{- .Values.serviceAccount.name | default "default" -}}
{{- end -}}
{{- end -}}

{{/*
Extract the numeric port from a "host:port" or ":port" address string, as
used by config.api_server.addr and config.metrics_addr.
*/}}
{{- define "poolmgrd.portFromAddr" -}}
{{- $parts := splitList ":" . -}}
{{- last $parts -}}
{{- end -}}

{{/*
Whether any TLS material is configured (either the chart creates a Secret,
or an existing one is referenced) -- controls whether the tls volume/mount
is added at all.
*/}}
{{- define "poolmgrd.hasTLS" -}}
{{- if or .Values.tls.create .Values.tls.existingSecret -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{- define "poolmgrd.tlsSecretName" -}}
{{- if .Values.tls.existingSecret -}}
{{- .Values.tls.existingSecret -}}
{{- else -}}
{{- printf "%s-tls" (include "poolmgrd.fullname" .) -}}
{{- end -}}
{{- end -}}
