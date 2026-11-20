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

Recorded response shapes for the OpenAI-compatible embedding client tests. No live endpoint was called (no API key was
available): each body follows the shape the provider documents, for the response JSON and the errors and rate limits
(the OpenAI embeddings API reference, the OpenAI error codes guide and the Azure OpenAI v1 reference, and the Hugging
Face TEI documentation for the /info and error bodies).
Vectors are shortened to 4 floats (a text-embedding-3-small request with dimensions=4).
