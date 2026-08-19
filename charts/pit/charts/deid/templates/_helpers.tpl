{{- define "deid.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "deid.fullname" -}}
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

{{- define "deid.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "deid.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "deid.selectorLabels" -}}
app.kubernetes.io/name: {{ include "deid.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- /*
The Secret the salt is read from: an existing one if given, else the one this
chart renders. Named in two places (the Secret and the Deployment's env), so it
resolves in one.
*/ -}}
{{- define "deid.saltSecret" -}}
{{- default (include "deid.fullname" .) .Values.salt.existingSecret -}}
{{- end -}}

{{- define "deid.policyPath" -}}
{{- printf "%s/%s" (trimSuffix "/" .Values.policy.mountPath) .Values.policy.fileName -}}
{{- end -}}

{{- /*
The policy file's contents, or a render-time failure naming the flag that
supplies them.

Deliberately hard rather than defaulted. An empty policy covers no tables, so
every topic would halt -- which is the safe direction, but it would arrive as a
crashlooping pod several minutes after an install that reported success. This
turns it into one sentence at render time.
*/ -}}
{{- define "deid.policyContents" -}}
{{- required "deid.policy.contents is empty. The policy is a single file in this repo and is supplied at install time: helm ... --set-file deid.policy.contents=deid/policy/clinic.yml (make install/lint/template all do this)." .Values.policy.contents -}}
{{- end -}}
