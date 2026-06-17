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

package org.apache.hudi.client;

import org.apache.hudi.HoodieVersion;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Enriches the {@code extraMetadata} map persisted with every commit, with version, engine, and
 * (optionally) engine-specific properties and a configurable subset of {@link HoodieWriteConfig}
 * values.
 *
 * <p>Key namespacing:
 * <ul>
 *   <li>{@code hudi.version} — writer version. Always emitted.</li>
 *   <li>{@code engine} — engine type (SPARK/FLINK/JAVA). Always emitted.</li>
 *   <li>Engine-supplied keys (Spark: {@code spark.*}, Java: {@code java.*}/{@code os.*}, etc.)
 *       — gated by {@link #EMBED_ENGINE_PROPERTIES_IN_COMMIT_METADATA}.</li>
 *   <li>{@code config.<key>} — values of {@link HoodieWriteConfig} entries whose keys are listed
 *       in {@link #WRITE_CONFIG_KEYS_TO_SERIALIZE_TO_COMMIT_METADATA}.</li>
 * </ul>
 */
public class CommitMetadataProperties {

  static final String HUDI_VERSION_KEY = "hudi.version";
  static final String ENGINE_KEY = "engine";
  static final String CONFIG_KEY_PREFIX = "config.";

  /**
   * Default allowlist of write-config keys serialized into commit metadata. These are values that
   * change across jobs/runs but aren't already captured in {@code hoodie.properties}, so they're
   * useful for after-the-fact debugging. Intentionally excludes immutable table identity
   * (already in {@code hoodie.properties}) and per-record/sensitive values.
   */
  private static final String DEFAULT_WRITE_CONFIG_KEYS = String.join(",",
      Arrays.asList(
          "hoodie.datasource.write.operation",
          "hoodie.insert.shuffle.parallelism",
          "hoodie.upsert.shuffle.parallelism",
          "hoodie.bulkinsert.shuffle.parallelism",
          "hoodie.delete.shuffle.parallelism",
          "hoodie.write.concurrency.mode",
          "hoodie.metadata.enable"));

  /**
   * When enabled, engine-specific properties supplied by
   * {@link HoodieEngineContext#getEngineProperties()} are embedded into commit metadata for
   * debugging (e.g. {@code spark.application.id}, {@code spark.user}). {@code hudi.version} and
   * {@code engine} are always embedded regardless of this flag.
   *
   * <p>Default is {@code false} since these add per-commit growth to the timeline. Long-running
   * ingestion workloads writing many commits should leave this off unless debugging.
   */
  public static final ConfigProperty<Boolean> EMBED_ENGINE_PROPERTIES_IN_COMMIT_METADATA =
      ConfigProperty
          .key("hoodie.commit.metadata.engine.properties.embed.enable")
          .defaultValue(false)
          .markAdvanced()
          .sinceVersion("1.3.0")
          .withDocumentation("When enabled, engine-specific properties (e.g. spark.application.id, "
              + "spark.user, java.version) are embedded into commit metadata for debugging. "
              + "hudi.version and engine name are always embedded regardless of this flag.");

  /**
   * Comma-separated list of {@link HoodieWriteConfig} keys whose values should be serialized into
   * commit metadata under the {@code config.<key>} prefix. Use with care: every key listed here
   * adds an entry to every commit, which lives forever in the active and archived timeline.
   *
   * <p>Empty value disables config-key serialization entirely (only {@code hudi.version} and
   * {@code engine} are emitted).
   */
  public static final ConfigProperty<String> WRITE_CONFIG_KEYS_TO_SERIALIZE_TO_COMMIT_METADATA =
      ConfigProperty
          .key("hoodie.write.config.keys.to.serialize.to.commit.metadata")
          .defaultValue(DEFAULT_WRITE_CONFIG_KEYS)
          .markAdvanced()
          .sinceVersion("1.3.0")
          .withDocumentation("Comma-separated list of write-config keys whose values are "
              + "serialized into the extraMetadata map of every commit (under the 'config.' "
              + "prefix). Set to empty to skip config-key serialization entirely. Avoid adding "
              + "keys whose values may contain credentials or large payloads, since commit "
              + "metadata is persisted in the timeline.");

  public static Option<Map<String, String>> enrich(Option<Map<String, String>> extraMetadata,
                                                   HoodieWriteConfig config,
                                                   HoodieEngineContext context) {
    Map<String, String> newMetadata = new HashMap<>();
    if (extraMetadata.isPresent()) {
      newMetadata.putAll(extraMetadata.get());
    }

    newMetadata.put(HUDI_VERSION_KEY, HoodieVersion.get());
    newMetadata.put(ENGINE_KEY, config.getEngineType().name());

    if (config.getBoolean(EMBED_ENGINE_PROPERTIES_IN_COMMIT_METADATA)) {
      newMetadata.putAll(context.getEngineProperties());
    }

    for (String key : parseConfigKeys(config.getString(WRITE_CONFIG_KEYS_TO_SERIALIZE_TO_COMMIT_METADATA))) {
      String value = config.getString(key);
      if (!StringUtils.isNullOrEmpty(value)) {
        newMetadata.put(CONFIG_KEY_PREFIX + key, value);
      }
    }

    return Option.of(newMetadata);
  }

  private static List<String> parseConfigKeys(String csv) {
    if (StringUtils.isNullOrEmpty(csv)) {
      return Collections.emptyList();
    }
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }
}
