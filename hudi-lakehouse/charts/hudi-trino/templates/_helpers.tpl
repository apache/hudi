{{- /*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/ -}}

{{- define "hudi-trino.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hudi-trino.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "hudi-trino.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "hudi-trino.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/name: {{ include "hudi-trino.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: hudi-lakehouse
{{- end -}}

{{- define "hudi-trino.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hudi-trino.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- /* Name of the Secret carrying S3 credentials, if any. */ -}}
{{- define "hudi-trino.s3SecretName" -}}
{{- if .Values.catalog.filesystem.s3.existingSecret -}}
{{- .Values.catalog.filesystem.s3.existingSecret -}}
{{- else if and .Values.catalog.filesystem.s3.accessKey .Values.catalog.filesystem.s3.secretKey -}}
{{- printf "%s-s3" (include "hudi-trino.fullname" .) -}}
{{- end -}}
{{- end -}}
