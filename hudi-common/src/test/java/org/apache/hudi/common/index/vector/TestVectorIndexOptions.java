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

package org.apache.hudi.common.index.vector;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVectorIndexOptions {

  @Test
  void testDefaultsAreCanonicalAndComplete() {
    assertEquals(
        opts(
            VectorIndexOptions.METRIC, "cosine",
            VectorIndexOptions.QUANTIZER, "IVF_RABITQ",
            VectorIndexOptions.NUM_CLUSTERS, "256",
            VectorIndexOptions.MAX_ITER, "20",
            VectorIndexOptions.RABITQ_BITS, "4",
            VectorIndexOptions.RABITQ_SEED, "42",
            VectorIndexOptions.RABITQ_ASSUME_NORMALIZED, "false",
            VectorIndexOptions.QUERY_NUM_PROBES, "32",
            VectorIndexOptions.QUERY_REFINE_FACTOR, "50",
            VectorIndexOptions.QUERY_MODE, "exact_rerank",
            VectorIndexOptions.QUERY_STALE_POLICY, "fail"),
        VectorIndexOptions.validateAndNormalize(opts()));
  }

  @Test
  void testValuesAreNormalizedForPersistence() {
    Map<String, String> normalized = VectorIndexOptions.validateAndNormalize(opts(
        VectorIndexOptions.METRIC, "DOT-PRODUCT",
        VectorIndexOptions.QUANTIZER, "ivf-rabitq",
        VectorIndexOptions.RABITQ_ASSUME_NORMALIZED, "TRUE",
        VectorIndexOptions.QUERY_MODE, "EXACT-RERANK",
        VectorIndexOptions.QUERY_STALE_POLICY, "WARN"));

    assertEquals("dot_product", normalized.get(VectorIndexOptions.METRIC));
    assertEquals("IVF_RABITQ", normalized.get(VectorIndexOptions.QUANTIZER));
    assertEquals("true", normalized.get(VectorIndexOptions.RABITQ_ASSUME_NORMALIZED));
    assertEquals("exact_rerank", normalized.get(VectorIndexOptions.QUERY_MODE));
    assertEquals("warn", normalized.get(VectorIndexOptions.QUERY_STALE_POLICY));
    assertThrows(
        UnsupportedOperationException.class,
        () -> normalized.put(VectorIndexOptions.METRIC, "l2"));
  }

  @Test
  void testEveryMetricQueryModeAndStalePolicyIsAccepted() {
    assertCanonical(VectorIndexOptions.METRIC, "cosine", "cosine");
    assertCanonical(VectorIndexOptions.METRIC, "l2", "l2");
    assertCanonical(VectorIndexOptions.METRIC, "dot_product", "dot_product");
    assertCanonical(VectorIndexOptions.QUERY_MODE, "approximate", "approximate");
    assertCanonical(VectorIndexOptions.QUERY_MODE, "exact_rerank", "exact_rerank");
    assertCanonical(VectorIndexOptions.QUERY_STALE_POLICY, "fail", "fail");
    assertCanonical(VectorIndexOptions.QUERY_STALE_POLICY, "warn", "warn");
    assertCanonical(VectorIndexOptions.QUERY_STALE_POLICY, "fallback", "fallback");
  }

  @Test
  void testUnknownRetiredAndMisspelledOptionsAreRejected() {
    assertInvalidOption("vector.dimension", "128");
    assertInvalidOption("vector.query.nprobe", "8");
    assertInvalidOption("vector.unknown", "value");
  }

  @Test
  void testUnsupportedEnumValuesAreRejectedWithOptionContext() {
    assertInvalidValueContainsKey(VectorIndexOptions.METRIC, "manhattan");
    assertInvalidValueContainsKey(VectorIndexOptions.QUANTIZER, "pq");
    assertInvalidValueContainsKey(VectorIndexOptions.QUERY_MODE, "fast-ish");
    assertInvalidValueContainsKey(VectorIndexOptions.QUERY_STALE_POLICY, "ignore");
    assertInvalidValueContainsKey(VectorIndexOptions.RABITQ_ASSUME_NORMALIZED, "yes");
  }

  @Test
  void testNumericOptionsAreValidatedWithOptionContext() {
    assertCanonical(VectorIndexOptions.RABITQ_BITS, "1", "1");
    assertCanonical(VectorIndexOptions.RABITQ_BITS, "8", "8");
    assertInvalidValueContainsKey(VectorIndexOptions.NUM_CLUSTERS, "0");
    assertInvalidValueContainsKey(VectorIndexOptions.MAX_ITER, "-1");
    assertInvalidValueContainsKey(VectorIndexOptions.QUERY_NUM_PROBES, "0");
    assertInvalidValueContainsKey(VectorIndexOptions.QUERY_REFINE_FACTOR, "-1");
    assertInvalidValueContainsKey(VectorIndexOptions.RABITQ_BITS, "0");
    assertInvalidValueContainsKey(VectorIndexOptions.RABITQ_BITS, "9");
    assertInvalidValueContainsKey(VectorIndexOptions.RABITQ_SEED, "many");
  }

  @Test
  void testNumProbesMustNotExceedNumClusters() {
    assertCanonical(
        opts(
            VectorIndexOptions.NUM_CLUSTERS, "32",
            VectorIndexOptions.QUERY_NUM_PROBES, "32"),
        VectorIndexOptions.QUERY_NUM_PROBES,
        "32");

    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> VectorIndexOptions.validateAndNormalize(opts(
            VectorIndexOptions.NUM_CLUSTERS, "4",
            VectorIndexOptions.QUERY_NUM_PROBES, "5")));
    assertTrue(error.getMessage().contains(VectorIndexOptions.QUERY_NUM_PROBES));
    assertTrue(error.getMessage().contains(VectorIndexOptions.NUM_CLUSTERS));
  }

  private static void assertCanonical(String key, String input, String expected) {
    assertCanonical(opts(key, input), key, expected);
  }

  private static void assertCanonical(
      Map<String, String> options, String key, String expected) {
    assertEquals(expected, VectorIndexOptions.validateAndNormalize(options).get(key));
  }

  private static void assertInvalidOption(String key, String value) {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> VectorIndexOptions.validateAndNormalize(opts(key, value)));
    assertTrue(error.getMessage().contains(key));
  }

  private static void assertInvalidValueContainsKey(String key, String value) {
    IllegalArgumentException error = assertThrows(
        IllegalArgumentException.class,
        () -> VectorIndexOptions.validateAndNormalize(opts(key, value)));
    assertTrue(error.getMessage().contains(key));
  }

  private static Map<String, String> opts(String... keyValues) {
    Map<String, String> options = new HashMap<>();
    for (int i = 0; i < keyValues.length; i += 2) {
      options.put(keyValues[i], keyValues[i + 1]);
    }
    return options;
  }
}
