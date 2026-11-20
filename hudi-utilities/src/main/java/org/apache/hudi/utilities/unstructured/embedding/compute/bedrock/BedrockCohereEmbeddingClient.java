/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.compute.bedrock;

/**
 * Cohere Embed v4 on Bedrock ({@code cohere.embed-v4}): text embeddings; texts are packed up to 96 per request.
 * See {@link BedrockEmbeddingClient} and {@link BedrockModelFamily#COHERE_EMBED_V4}. Chosen for provider
 * {@code bedrock} when the model id names this family; for an id that does not (an application inference profile or a
 * provisioned-throughput ARN), set {@code .client.class} to this class.
 */
public class BedrockCohereEmbeddingClient extends BedrockEmbeddingClient {

  @Override
  public BedrockModelFamily family() {
    return BedrockModelFamily.COHERE_EMBED_V4;
  }
}
