{{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}
{{- define "hudi-spark-connect.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hudi-spark-connect.master" -}}
{{- default (printf "local[%v]" .Values.spark.cores) .Values.spark.master -}}
{{- end -}}

{{/* Preserve decimal integer spelling before applying each loader's rules. */}}
{{- define "hudi-spark-connect.propertyText" -}}
{{- $text := toString . -}}
{{- if or (kindIs "float64" .) (kindIs "float32" .) -}}
{{- if eq (float64 .) (floor .) -}}
{{- $text = printf "%.0f" (float64 .) -}}
{{- end -}}
{{- end -}}
{{- if regexMatch "[\r\n]" $text -}}
{{- fail "property values must be a single line" -}}
{{- end -}}
{{- $text -}}
{{- end -}}

{{/* Spark uses java.util.Properties, which consumes backslash escapes. */}}
{{- define "hudi-spark-connect.propertyValue" -}}
{{- include "hudi-spark-connect.propertyText" . | replace "\\" "\\\\" | replace " " "\\ " | replace "\t" "\\t" | replace "\f" "\\f" -}}
{{- end -}}

{{/* Hudi 1.2 DFSPropertiesConfiguration keeps backslashes literally but folds
     whitespace. Reject values it cannot preserve instead of silently changing them. */}}
{{- define "hudi-spark-connect.hudiPropertyValue" -}}
{{- $text := include "hudi-spark-connect.propertyText" . -}}
{{- if ne $text (trim (regexReplaceAll "\\s+" $text " ")) -}}
{{- fail "Hudi defaults cannot preserve leading/trailing or repeated whitespace" -}}
{{- end -}}
{{- $text -}}
{{- end -}}

{{- define "hudi-spark-connect.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "hudi-spark-connect.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "hudi-spark-connect.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hudi-spark-connect.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "hudi-spark-connect.labels" -}}
{{ include "hudi-spark-connect.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hudi-lakehouse
{{- end -}}
