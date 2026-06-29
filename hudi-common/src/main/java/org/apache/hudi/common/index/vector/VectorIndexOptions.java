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

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Option keys accepted in the {@code OPTIONS} clause of
 * {@code CREATE INDEX ... USING VECTOR}.
 */
public final class VectorIndexOptions {

  private static final Set<String> SUPPORTED_METRICS = Set.of("cosine", "l2", "dot_product");
  private static final Set<String> SUPPORTED_ALGORITHMS = Set.of("ivfflat", "ivfpq_hnsw", "ivf_rabitq_mdt");
  private static final Set<String> SUPPORTED_QUANTIZERS = Set.of("IVF_RABITQ", "IVF_PQ");

  private VectorIndexOptions() {
  }

  public static final String DIMENSION = "vector.dimension";
  public static final String METRIC = "vector.metric";
  public static final String ALGORITHM = "vector.algorithm";
  public static final String QUANTIZER = "vector.quantizer";
  public static final String NUM_CLUSTERS = "vector.num_clusters";
  public static final String NUM_PROBES = "vector.num_probes";
  public static final String REFINE_FACTOR = "vector.refine_factor";
  public static final String MAX_ITER = "vector.max_iter";
  public static final String RABITQ_RANDOM_SEED = "vector.rabitq.random_seed";
  public static final String RABITQ_TOTAL_BITS = "vector.rabitq.total_bits";
  public static final String RABITQ_BITS = "vector.rabitq.bits";
  public static final String RABITQ_ASSUME_NORMALIZED = "vector.rabitq.assume_normalized";

  public static final String DEFAULT_METRIC = "cosine";
  public static final String DEFAULT_ALGORITHM = "ivfflat";
  public static final String DEFAULT_QUANTIZER = "IVF_RABITQ";
  public static final int DEFAULT_NUM_CLUSTERS = 256;
  public static final int DEFAULT_NUM_PROBES = 8;
  public static final int DEFAULT_REFINE_FACTOR = 10;
  public static final int DEFAULT_MAX_ITER = 20;
  public static final long DEFAULT_RABITQ_RANDOM_SEED = 42L;
  public static final int DEFAULT_RABITQ_TOTAL_BITS = 1;

  public static void validate(Map<String, String> options) {
    getDimension(options);
    getMetric(options);
    getAlgorithm(options);
    getQuantizer(options);
    getNumClusters(options);
    getNumProbes(options);
    getRefineFactor(options);
    getMaxIter(options);
    getRaBitQRandomSeed(options);
    getRaBitQTotalBits(options);
    getRaBitQAssumeNormalized(options);

    if (getNumProbes(options) > getNumClusters(options)) {
      throw new IllegalArgumentException(String.format(
          "Vector index option '%s' (%s) cannot exceed '%s' (%s)",
          NUM_PROBES, getNumProbes(options), NUM_CLUSTERS, getNumClusters(options)));
    }
  }

  public static int getDimension(Map<String, String> options) {
    String value = options.get(DIMENSION);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Vector index requires '" + DIMENSION + "' in OPTIONS");
    }
    return parsePositiveInt(DIMENSION, value);
  }

  public static String getMetric(Map<String, String> options) {
    return requireOneOf(METRIC, options.getOrDefault(METRIC, DEFAULT_METRIC), SUPPORTED_METRICS);
  }

  public static String getAlgorithm(Map<String, String> options) {
    return requireOneOf(ALGORITHM, options.getOrDefault(ALGORITHM, DEFAULT_ALGORITHM), SUPPORTED_ALGORITHMS);
  }

  public static String getQuantizer(Map<String, String> options) {
    String value = options.getOrDefault(QUANTIZER, DEFAULT_QUANTIZER).toUpperCase(Locale.ROOT);
    if (!SUPPORTED_QUANTIZERS.contains(value)) {
      throw new IllegalArgumentException(String.format(
          "Vector index option '%s' must be one of %s, got: %s", QUANTIZER, SUPPORTED_QUANTIZERS, value));
    }
    return value;
  }

  public static int getNumClusters(Map<String, String> options) {
    return parsePositiveInt(NUM_CLUSTERS,
        options.getOrDefault(NUM_CLUSTERS, String.valueOf(DEFAULT_NUM_CLUSTERS)));
  }

  public static int getNumProbes(Map<String, String> options) {
    return parsePositiveInt(NUM_PROBES,
        options.getOrDefault(NUM_PROBES, String.valueOf(DEFAULT_NUM_PROBES)));
  }

  public static int getRefineFactor(Map<String, String> options) {
    return parsePositiveInt(REFINE_FACTOR,
        options.getOrDefault(REFINE_FACTOR, String.valueOf(DEFAULT_REFINE_FACTOR)));
  }

  public static int getMaxIter(Map<String, String> options) {
    return parsePositiveInt(MAX_ITER,
        options.getOrDefault(MAX_ITER, String.valueOf(DEFAULT_MAX_ITER)));
  }

  public static long getRaBitQRandomSeed(Map<String, String> options) {
    return parseLong(RABITQ_RANDOM_SEED,
        options.getOrDefault(RABITQ_RANDOM_SEED, String.valueOf(DEFAULT_RABITQ_RANDOM_SEED)));
  }

  public static int getRaBitQTotalBits(Map<String, String> options) {
    String rawValue = options.containsKey(RABITQ_TOTAL_BITS)
        ? options.get(RABITQ_TOTAL_BITS)
        : options.getOrDefault(RABITQ_BITS, String.valueOf(DEFAULT_RABITQ_TOTAL_BITS));
    int bits = parsePositiveInt(RABITQ_TOTAL_BITS, rawValue);
    if (bits > 8) {
      throw new IllegalArgumentException("RaBitQ total bits must be in [1, 8], got: " + bits);
    }
    return bits;
  }

  public static boolean getRaBitQAssumeNormalized(Map<String, String> options) {
    String value = options.getOrDefault(RABITQ_ASSUME_NORMALIZED, "false");
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      throw new IllegalArgumentException(String.format(
          "Vector index option '%s' must be either 'true' or 'false', got: %s",
          RABITQ_ASSUME_NORMALIZED, value));
    }
    return Boolean.parseBoolean(value);
  }

  private static int parsePositiveInt(String key, String value) {
    try {
      int parsed = Integer.parseInt(value);
      if (parsed <= 0) {
        throw new IllegalArgumentException(
            String.format("Vector index option '%s' must be > 0, got: %s", key, value));
      }
      return parsed;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Vector index option '%s' must be an integer, got: %s", key, value), e);
    }
  }

  private static long parseLong(String key, String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Vector index option '%s' must be a long, got: %s", key, value), e);
    }
  }

  private static String requireOneOf(String key, String value, Set<String> allowedValues) {
    String normalized = value.toLowerCase(Locale.ROOT);
    if (!allowedValues.contains(normalized)) {
      throw new IllegalArgumentException(String.format(
          "Vector index option '%s' must be one of %s, got: %s", key, allowedValues, value));
    }
    return normalized;
  }
}
