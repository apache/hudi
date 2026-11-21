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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hudi.utilities.config.SensitiveConfigUtils.SENSITIVE_VALUES_MASKED;
import static org.apache.hudi.utilities.config.SensitiveConfigUtils.maskSensitiveValues;
import static org.apache.hudi.utilities.config.SensitiveConfigUtils.maskUrlCredentials;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSensitiveConfigUtils {

  private static final String JAAS_CONFIG =
      "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"top-secret\";";
  private static final String SCHEMA_REGISTRY_USER_INFO = "sr-user:sr-secret";

  private static Map<String, Object> kafkaParams() {
    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "broker-1:9092");
    kafkaParams.put("group.id", "test-group");
    kafkaParams.put("security.protocol", "SASL_SSL");
    kafkaParams.put("sasl.mechanism", "SCRAM-SHA-512");
    kafkaParams.put("sasl.jaas.config", JAAS_CONFIG);
    kafkaParams.put("basic.auth.user.info", SCHEMA_REGISTRY_USER_INFO);
    kafkaParams.put("ssl.truststore.password", "truststore-secret");
    kafkaParams.put("schema.registry.password", "registry-secret");
    kafkaParams.put("custom.client.secret", "client-secret");
    kafkaParams.put("oauth.bearer.token", "bearer-token");
    kafkaParams.put("schema.registry.url", "https://sr-key:sr-url-secret@registry:8081/subjects/s");
    kafkaParams.put("max.poll.records", 500);
    return kafkaParams;
  }

  @Test
  void testMaskSensitiveValuesWithDefaultFilters() {
    Map<String, Object> kafkaParams = kafkaParams();
    Map<String, Object> originalParams = new HashMap<>(kafkaParams);

    Map<String, Object> expected = new TreeMap<>();
    expected.put("bootstrap.servers", "broker-1:9092");
    expected.put("group.id", "test-group");
    expected.put("max.poll.records", 500);
    // Every key containing a default filter is masked, including non-secret ones like sasl.mechanism.
    expected.put("security.protocol", "SASL_SSL");
    expected.put("sasl.mechanism", SENSITIVE_VALUES_MASKED);
    expected.put("sasl.jaas.config", SENSITIVE_VALUES_MASKED);
    expected.put("basic.auth.user.info", SENSITIVE_VALUES_MASKED);
    expected.put("ssl.truststore.password", SENSITIVE_VALUES_MASKED);
    expected.put("schema.registry.password", SENSITIVE_VALUES_MASKED);
    expected.put("custom.client.secret", SENSITIVE_VALUES_MASKED);
    expected.put("oauth.bearer.token", SENSITIVE_VALUES_MASKED);
    // Not a sensitive key, but the credentials embedded in the URL are removed.
    expected.put("schema.registry.url", "https://registry:8081/subjects/s");

    Map<String, Object> masked = maskSensitiveValues(kafkaParams, new TypedProperties());

    assertEquals(expected, masked);
    String logged = masked.toString();
    assertFalse(logged.contains("top-secret"));
    assertFalse(logged.contains("sr-secret"));
    assertFalse(logged.contains("truststore-secret"));
    assertFalse(logged.contains("registry-secret"));
    assertFalse(logged.contains("client-secret"));
    assertFalse(logged.contains("bearer-token"));
    assertFalse(logged.contains("sr-url-secret"));
    // The input map is left untouched, since it is still passed to the Kafka consumer.
    assertEquals(originalParams, kafkaParams);
  }

  @Test
  void testMaskSensitiveValuesWithCustomFilters() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.SENSITIVE_CONFIG_KEYS_FILTER.key(), "jaas,group");

    Map<String, Object> expected = new TreeMap<>();
    expected.put("bootstrap.servers", "broker-1:9092");
    expected.put("group.id", SENSITIVE_VALUES_MASKED);
    expected.put("max.poll.records", 500);
    expected.put("security.protocol", "SASL_SSL");
    expected.put("sasl.mechanism", "SCRAM-SHA-512");
    expected.put("sasl.jaas.config", SENSITIVE_VALUES_MASKED);
    expected.put("basic.auth.user.info", SCHEMA_REGISTRY_USER_INFO);
    expected.put("ssl.truststore.password", "truststore-secret");
    expected.put("schema.registry.password", "registry-secret");
    expected.put("custom.client.secret", "client-secret");
    expected.put("oauth.bearer.token", "bearer-token");
    expected.put("schema.registry.url", "https://registry:8081/subjects/s");

    assertEquals(expected, maskSensitiveValues(kafkaParams(), props));
  }

  @Test
  void testMaskSensitiveValuesOfTypedProperties() {
    TypedProperties configs = new TypedProperties();
    configs.setProperty("hoodie.table.name", "trips");
    configs.setProperty("hoodie.meta.sync.unity.credentials", "unity-secret");

    Map<String, Object> expected = new TreeMap<>();
    expected.put("hoodie.table.name", "trips");
    expected.put("hoodie.meta.sync.unity.credentials", SENSITIVE_VALUES_MASKED);

    assertEquals(expected, maskSensitiveValues(configs, configs));
  }

  @Test
  void testMaskUrlCredentials() {
    assertEquals("https://host/subjects/s", maskUrlCredentials("https://key:secret@host/subjects/s"));
    // Secrets are not URL-encoded by the writer, so they may contain '/' or '@'.
    assertEquals("https://host/subjects/s", maskUrlCredentials("https://key:se/cr@et@host/subjects/s"));
    assertEquals("https://h1/x,https://h2/x", maskUrlCredentials("https://k1:s1@h1/x,https://k2:s2@h2/x"));
    assertEquals("https://host/subjects/s", maskUrlCredentials("https://host/subjects/s"));
    assertEquals("user@example.com", maskUrlCredentials("user@example.com"));
    assertNull(maskUrlCredentials(null));
  }

  @Test
  void testStreamerConfigDumpRemovesUrlCredentials() {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.schemaprovider.registry.url",
        "https://api-key:api-secret@registry.example.com/subjects/topic-value/versions/latest");

    String logged = HoodieStreamer.toSortedTruncatedString(props);

    assertTrue(logged.contains("hoodie.deltastreamer.schemaprovider.registry.url: "
        + "https://registry.example.com/subjects/topic-value/versions/latest\n"));
    assertFalse(logged.contains("api-key"));
    assertFalse(logged.contains("api-secret"));
  }
}
