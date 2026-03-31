{{/*
Expand the name of the chart.
*/}}
{{- define "deltaforge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "deltaforge.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "deltaforge.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "deltaforge.labels" -}}
helm.sh/chart: {{ include "deltaforge.chart" . }}
{{ include "deltaforge.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "deltaforge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "deltaforge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name
*/}}
{{- define "deltaforge.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "deltaforge.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Image reference
*/}}
{{- define "deltaforge.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
ConfigMap name for pipeline config
*/}}
{{- define "deltaforge.configMapName" -}}
{{- if .Values.pipeline.existingConfigMap }}
{{- .Values.pipeline.existingConfigMap }}
{{- else }}
{{- include "deltaforge.fullname" . }}-config
{{- end }}
{{- end }}

{{/*
Secret name for credentials
*/}}
{{- define "deltaforge.secretName" -}}
{{- include "deltaforge.fullname" . }}-secrets
{{- end }}
