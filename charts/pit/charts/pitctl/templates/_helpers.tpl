{{- define "pitctl.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pitctl.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The tail Deployment's name, and it is a fixed string rather than derived from the
release.

Three things already address it by this exact name: the Makefile's `reload` and
`logs-tail` targets, and -- the one that matters -- M7's snapshot CronJob, which
has to scale this Deployment to zero before `CREATE DATABASE ... TEMPLATE
pit_base` can take its clone. The Role below grants `deployments/scale` on this
name only, so a derived name would have to be threaded into the RBAC, the CronJob
and the Makefile in step, and the failure when it drifted would be a snapshot that
silently could not quiesce the tail.
*/}}
{{- define "pitctl.tailName" -}}
{{- .Values.tail.name -}}
{{- end -}}

{{- define "pitctl.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "pitctl.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels for the tail. `app.kubernetes.io/name: pit-tail` rather than the
chart name, because `make logs-tail` selects on it and a pod labelled `pitctl`
would return nothing while looking like it should have worked.
*/}}
{{- define "pitctl.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pitctl.tailName" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "pitctl.serviceAccountName" -}}
{{- default (printf "%s" (include "pitctl.tailName" .)) .Values.serviceAccount.name -}}
{{- end -}}

{{/*
The sink DSN, assembled from the sink-pg subchart's own values so the two cannot
drift. Deliberately without a dbname: `pit tail --db` supplies it, and M6's restore
Job will supply a different one from the same image.
*/}}
{{- define "pitctl.sinkDsn" -}}
{{- printf "host=%s port=%v user=%s" .Values.sink.host (.Values.sink.port | int) .Values.sink.user -}}
{{- end -}}
