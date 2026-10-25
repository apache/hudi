/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * Embedding Transformer Configs.
 */
@Immutable
@ConfigClassProperty(name = "Embedding Transformer Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_TRANSFORMER,
    description = "Configurations for the embedding transformer, which populates a VECTOR "
        + "column by calling an embedding API for each batch of ingested records.")
public class EmbeddingTransformerConfig extends HoodieConfig {

  private static final String PREFIX = STREAMER_CONFIG_PREFIX + "transformer.embedding.";

  public static final ConfigProperty<String> PROVIDER_CLASS = ConfigProperty
      .key(PREFIX + "provider.class")
      .defaultValue("org.apache.hudi.utilities.transform.embedding.OpenAICompatibleEmbeddingProvider")
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Implementation of EmbeddingProvider used to embed record text. The "
          + "default calls any OpenAI-compatible /v1/embeddings endpoint (Ollama, TEI, vLLM, "
          + "OpenAI, Voyage).");

  public static final ConfigProperty<String> ENDPOINT_URL = ConfigProperty
      .key(PREFIX + "endpoint.url")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .withDocumentation("Embeddings API endpoint, e.g. http://localhost:11434/v1/embeddings "
          + "for a local Ollama.");

  public static final ConfigProperty<String> MODEL = ConfigProperty
      .key(PREFIX + "model")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .withDocumentation("Embedding model name passed to the API, e.g. nomic-embed-text or "
          + "text-embedding-3-small.");

  public static final ConfigProperty<String> API_KEY_ENV = ConfigProperty
      .key(PREFIX + "api.key.env")
      .defaultValue("")
      .sinceVersion("1.2.0")
      .withDocumentation("Name of the environment variable holding the API key, sent as a "
          + "Bearer token. Empty for unauthenticated endpoints (local Ollama/TEI). The key "
          + "itself is never placed in configuration.");

  public static final ConfigProperty<String> DIMENSION = ConfigProperty
      .key(PREFIX + "dimension")
      .noDefaultValue()
      .sinceVersion("1.2.0")
      .withDocumentation("Dimension of the embedding vectors; declared as VECTOR(dimension) on "
          + "the target column and validated against API responses.");

  public static final ConfigProperty<Integer> BATCH_SIZE = ConfigProperty
      .key(PREFIX + "batch.size")
      .defaultValue(128)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Number of records embedded per API request. Batching happens at the "
          + "record level within each Spark partition, and the batch of rows awaiting the API "
          + "response is the transformer's only resident state, so this also bounds memory to "
          + "batch.size x average row size (including inline blobs) per partition. Raise for "
          + "high-throughput remote APIs; lower toward 32 for single-node local endpoints "
          + "(Ollama, TEI) so individual requests stay well inside the request timeout.");

  public static final ConfigProperty<String> SOURCE_COLUMN = ConfigProperty
      .key(PREFIX + "source.column")
      .defaultValue("extracted_text")
      .sinceVersion("1.2.0")
      .withDocumentation("Column whose text is embedded. Rows where it is null or empty get a "
          + "null vector.");

  public static final ConfigProperty<String> TARGET_COLUMN = ConfigProperty
      .key(PREFIX + "target.column")
      .defaultValue("embedding")
      .sinceVersion("1.2.0")
      .withDocumentation("Name of the VECTOR column appended by the transformer.");

  public static final ConfigProperty<Integer> MAX_INFLIGHT_REQUESTS = ConfigProperty
      .key(PREFIX + "max.inflight.requests")
      .defaultValue(2)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Number of embedding API requests kept in flight per Spark partition: "
          + "the next batches are prefetched and sent while earlier ones stream out, hiding API "
          + "latency. Rows resident per partition = batch.size x this value, so raise it only "
          + "with the memory headroom to match.");

  public static final ConfigProperty<Integer> INPUT_MAX_CHARS = ConfigProperty
      .key(PREFIX + "input.max.chars")
      .defaultValue(8000)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Input text is truncated to this many characters before embedding, "
          + "keeping requests inside model context limits.");

  public static final ConfigProperty<Long> TIMEOUT_MS = ConfigProperty
      .key(PREFIX + "timeout.ms")
      .defaultValue(120_000L)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Per-request timeout for the embeddings API.");
}
