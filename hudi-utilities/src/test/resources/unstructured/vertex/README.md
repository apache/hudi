<!--
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
-->

Recorded response shapes for the Vertex AI multimodalembedding@001 client tests. No live endpoint was called (no credential was
available): each body follows the shape the provider documents at
https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/multimodal-embeddings-api (fetched 2026-10-12):
`predictions[].imageEmbedding` (predict-image-response.json) / `textEmbedding`,
`deployedModelId`. Vectors carry 128 deterministic floats (`parameters.dimension` = 128, one of 128/256/512/1408). Error bodies use the
`{"error":{"code","message","status"}}` envelope of Google's API error model; that envelope was NOT verified for this endpoint, and the
client decides on the HTTP status, and for a 400 also on `error.status` (it quotes the body in its error text, truncated).

`error-400-failed-precondition.json` follows the same envelope with `status` FAILED_PRECONDITION, the 400 the Vertex API errors page documents for a model that needs allowlisting or is disallowed by the organization's policy (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/api-errors, read 2026-10-27); the client reads that `error.status`.
