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

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.client.CommitMetadataProperties.CONFIG_KEY_PREFIX;
import static org.apache.hudi.client.CommitMetadataProperties.EMBED_ENGINE_PROPERTIES_IN_COMMIT_METADATA;
import static org.apache.hudi.client.CommitMetadataProperties.ENGINE_KEY;
import static org.apache.hudi.client.CommitMetadataProperties.HUDI_VERSION_KEY;
import static org.apache.hudi.client.CommitMetadataProperties.WRITE_CONFIG_KEYS_TO_SERIALIZE_TO_COMMIT_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCommitMetadataProperties {

  /** Always-emitted keys (hudi.version, engine) are present even when input is empty. */
  @Test
  void enrich_emptyInput_emitsVersionAndEngine() {
    HoodieWriteConfig config = newConfig(new Properties());
    HoodieEngineContext context = newContext(Collections.emptyMap());

    Map<String, String> result = CommitMetadataProperties.enrich(Option.empty(), config, context).get();

    assertNotNull(result.get(HUDI_VERSION_KEY));
    assertEquals(EngineType.SPARK.name(), result.get(ENGINE_KEY));
  }

  /** Passing an immutable map must not throw — yihua's defensive-copy fix. */
  @Test
  void enrich_immutableInputMap_doesNotThrow() {
    HoodieWriteConfig config = newConfig(new Properties());
    HoodieEngineContext context = newContext(Collections.emptyMap());
    Map<String, String> input = Collections.unmodifiableMap(
        Collections.singletonMap("caller.key", "caller.value"));

    Map<String, String> result = CommitMetadataProperties.enrich(Option.of(input), config, context).get();

    assertEquals("caller.value", result.get("caller.key"));
    assertTrue(input.equals(Collections.singletonMap("caller.key", "caller.value")),
        "Input map must not be mutated");
  }

  /** Default (opt-in flag = false) suppresses engine-supplied keys. */
  @Test
  void enrich_engineEmbedFlagOff_omitsEngineSuppliedKeys() {
    HoodieWriteConfig config = newConfig(new Properties());
    Map<String, String> engineProps = new HashMap<>();
    engineProps.put("spark.application.id", "app-123");
    HoodieEngineContext context = newContext(engineProps);

    Map<String, String> result = CommitMetadataProperties.enrich(Option.empty(), config, context).get();

    assertFalse(result.containsKey("spark.application.id"),
        "Engine-supplied keys must be omitted when embed flag is off");
    assertNotNull(result.get(HUDI_VERSION_KEY));
    assertEquals(EngineType.SPARK.name(), result.get(ENGINE_KEY));
  }

  /** Opt-in flag = true emits engine-supplied keys. */
  @Test
  void enrich_engineEmbedFlagOn_emitsEngineSuppliedKeys() {
    Properties props = new Properties();
    props.put(EMBED_ENGINE_PROPERTIES_IN_COMMIT_METADATA.key(), "true");
    HoodieWriteConfig config = newConfig(props);
    Map<String, String> engineProps = new HashMap<>();
    engineProps.put("spark.application.id", "app-123");
    engineProps.put("spark.user", "sivabalan");
    HoodieEngineContext context = newContext(engineProps);

    Map<String, String> result = CommitMetadataProperties.enrich(Option.empty(), config, context).get();

    assertEquals("app-123", result.get("spark.application.id"));
    assertEquals("sivabalan", result.get("spark.user"));
  }

  /** Config-key allowlist serializes present values; skips truly absent keys (no default). */
  @Test
  void enrich_writeConfigKeyAllowlist_emitsPresentNonEmptyValues() {
    Properties props = new Properties();
    props.put(WRITE_CONFIG_KEYS_TO_SERIALIZE_TO_COMMIT_METADATA.key(),
        "hoodie.metadata.enable,hoodie.does.not.exist");
    props.put("hoodie.metadata.enable", "true");
    HoodieWriteConfig config = newConfig(props);
    HoodieEngineContext context = newContext(Collections.emptyMap());

    Map<String, String> result = CommitMetadataProperties.enrich(Option.empty(), config, context).get();

    assertEquals("true", result.get(CONFIG_KEY_PREFIX + "hoodie.metadata.enable"));
    assertFalse(result.containsKey(CONFIG_KEY_PREFIX + "hoodie.does.not.exist"),
        "Keys absent from the config must not be serialized");
  }

  /** Empty allowlist disables config-key serialization entirely. */
  @Test
  void enrich_emptyConfigKeyList_emitsNoConfigKeys() {
    Properties props = new Properties();
    props.put(WRITE_CONFIG_KEYS_TO_SERIALIZE_TO_COMMIT_METADATA.key(), "");
    props.put("hoodie.metadata.enable", "true");
    HoodieWriteConfig config = newConfig(props);
    HoodieEngineContext context = newContext(Collections.emptyMap());

    Map<String, String> result = CommitMetadataProperties.enrich(Option.empty(), config, context).get();

    long configKeyCount = result.keySet().stream().filter(k -> k.startsWith(CONFIG_KEY_PREFIX)).count();
    assertEquals(0L, configKeyCount, "No config.* keys when allowlist is empty");
    // hudi.version and engine still always emitted
    assertNotNull(result.get(HUDI_VERSION_KEY));
    assertEquals(EngineType.SPARK.name(), result.get(ENGINE_KEY));
  }

  /** Caller-provided extra metadata is preserved alongside enrichment. */
  @Test
  void enrich_preservesCallerProvidedKeys() {
    HoodieWriteConfig config = newConfig(new Properties());
    HoodieEngineContext context = newContext(Collections.emptyMap());
    Map<String, String> input = new HashMap<>();
    input.put("caller.key1", "caller.value1");
    input.put("caller.key2", "caller.value2");

    Map<String, String> result = CommitMetadataProperties.enrich(Option.of(input), config, context).get();

    assertEquals("caller.value1", result.get("caller.key1"));
    assertEquals("caller.value2", result.get("caller.key2"));
    assertNotNull(result.get(HUDI_VERSION_KEY));
  }

  private static HoodieWriteConfig newConfig(Properties overrides) {
    Properties props = new Properties();
    props.put(HoodieWriteConfig.BASE_PATH.key(), "/tmp/test-commit-metadata-properties");
    props.putAll(overrides);
    return HoodieWriteConfig.newBuilder().withProperties(props).build();
  }

  private static HoodieEngineContext newContext(Map<String, String> engineProperties) {
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    when(context.getEngineProperties()).thenReturn(engineProperties);
    return context;
  }
}
