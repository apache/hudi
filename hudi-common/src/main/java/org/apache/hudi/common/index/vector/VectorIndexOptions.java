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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Options accepted by {@code CREATE INDEX ... USING VECTOR}.
 *
 * <p>The indexed column's Hudi {@code VECTOR(D[, elementType])} schema is authoritative for
 * dimension and element type. Index options configure only the acceleration structure. DDL
 * implementations must call {@link #validateAndNormalize(Map)} before persisting an index
 * definition; individual parsing helpers are intentionally private so aggregate validation cannot
 * be bypassed.
 */
public final class VectorIndexOptions {

  public static final String METRIC = "vector.metric";
  public static final String QUANTIZER = "vector.quantizer";
  public static final String NUM_CLUSTERS = "vector.num_clusters";
  public static final String MAX_ITER = "vector.max_iter";
  public static final String RABITQ_BITS = "vector.rabitq.bits";
  public static final String RABITQ_SEED = "vector.rabitq.seed";
  public static final String RABITQ_ASSUME_NORMALIZED = "vector.rabitq.assume_normalized";
  public static final String QUERY_NUM_PROBES = "vector.query.nprobes";
  public static final String QUERY_REFINE_FACTOR = "vector.query.refine_factor";
  public static final String QUERY_MODE = "vector.query.mode";
  public static final String FRESHNESS_POLICY = "vector.freshness.policy";
  public static final String STALE_LOCATOR_POLICY = "vector.stale.locator.policy";
  public static final String FETCH_VERIFY_KEYS = "vector.fetch.verify.keys";

  public static final VectorDistanceMetric DEFAULT_METRIC = VectorDistanceMetric.COSINE;
  public static final VectorQuantizer DEFAULT_QUANTIZER = VectorQuantizer.IVF_RABITQ;
  public static final int DEFAULT_NUM_CLUSTERS = 256;
  public static final int DEFAULT_MAX_ITER = 20;
  public static final int DEFAULT_RABITQ_BITS = 4;
  public static final long DEFAULT_RABITQ_SEED = 42L;
  public static final int DEFAULT_NUM_PROBES = 32;
  public static final int DEFAULT_REFINE_FACTOR = 50;
  public static final VectorQueryMode DEFAULT_QUERY_MODE = VectorQueryMode.EXACT_RERANK;
  public static final VectorStalePolicy DEFAULT_EXACT_FRESHNESS_POLICY = VectorStalePolicy.FAIL;
  public static final VectorStalePolicy DEFAULT_APPROXIMATE_FRESHNESS_POLICY = VectorStalePolicy.WARN;
  public static final VectorStalePolicy DEFAULT_STALE_LOCATOR_POLICY = VectorStalePolicy.FALLBACK;
  public static final boolean DEFAULT_FETCH_VERIFY_KEYS = false;

  /** Parsed immutable view used by bootstrap and maintenance after aggregate validation. */
  public static final class ResolvedOptions {
    public final VectorDistanceMetric metric;
    public final VectorQuantizer quantizer;
    public final int numClusters;
    public final int maxIterations;
    public final int rabitqBits;
    public final long rabitqSeed;
    public final boolean assumeNormalized;

    private ResolvedOptions(Map<String, String> options) {
      this.metric = getMetric(options);
      this.quantizer = getQuantizer(options);
      this.numClusters = getNumClusters(options);
      this.maxIterations = getMaxIter(options);
      this.rabitqBits = getRaBitQBits(options);
      this.rabitqSeed = getRaBitQSeed(options);
      this.assumeNormalized = shouldAssumeNormalizedVectors(options);
    }
  }

  /** Validates the complete map before exposing parsed writer-side settings. */
  public static ResolvedOptions resolve(Map<String, String> options) {
    return new ResolvedOptions(validateAndNormalize(options));
  }

  private static final Set<String> SUPPORTED_OPTIONS = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          METRIC,
          QUANTIZER,
          NUM_CLUSTERS,
          MAX_ITER,
          RABITQ_BITS,
          RABITQ_SEED,
          RABITQ_ASSUME_NORMALIZED,
          QUERY_NUM_PROBES,
          QUERY_REFINE_FACTOR,
          QUERY_MODE,
          FRESHNESS_POLICY,
          STALE_LOCATOR_POLICY,
          FETCH_VERIFY_KEYS)));

  private VectorIndexOptions() {
  }

  /**
   * Validates the complete option map and returns canonical values for persistence.
   *
   * <p>The returned map contains every supported option, including explicit defaults. Unknown,
   * retired, misspelled, and invalid options are rejected instead of being silently ignored.
   */
  public static Map<String, String> validateAndNormalize(Map<String, String> options) {
    Set<String> unknownOptions = new HashSet<>(options.keySet());
    unknownOptions.removeAll(SUPPORTED_OPTIONS);
    if (!unknownOptions.isEmpty()) {
      throw new IllegalArgumentException("Unsupported vector index options: " + unknownOptions);
    }

    VectorDistanceMetric metric = getMetric(options);
    VectorQuantizer quantizer = getQuantizer(options);
    int numClusters = getNumClusters(options);
    int maxIter = getMaxIter(options);
    int bits = getRaBitQBits(options);
    long seed = getRaBitQSeed(options);
    boolean assumeNormalized = shouldAssumeNormalizedVectors(options);
    int numProbes = getNumProbes(options);
    int refineFactor = getRefineFactor(options);
    VectorQueryMode queryMode = getQueryMode(options);
    VectorStalePolicy freshnessPolicy = getFreshnessPolicy(options);
    VectorStalePolicy staleLocatorPolicy = getStaleLocatorPolicy(options);
    boolean verifyFetchKeys = shouldVerifyFetchKeys(options);

    if (numProbes > numClusters) {
      throw new IllegalArgumentException(
          "Option '" + QUERY_NUM_PROBES + "' must not exceed '" + NUM_CLUSTERS + "': "
              + numProbes + " > " + numClusters);
    }

    Map<String, String> normalized = new LinkedHashMap<>();
    normalized.put(METRIC, metric.name().toLowerCase(Locale.ROOT));
    normalized.put(QUANTIZER, quantizer.name());
    normalized.put(NUM_CLUSTERS, String.valueOf(numClusters));
    normalized.put(MAX_ITER, String.valueOf(maxIter));
    normalized.put(RABITQ_BITS, String.valueOf(bits));
    normalized.put(RABITQ_SEED, String.valueOf(seed));
    normalized.put(RABITQ_ASSUME_NORMALIZED, String.valueOf(assumeNormalized));
    normalized.put(QUERY_NUM_PROBES, String.valueOf(numProbes));
    normalized.put(QUERY_REFINE_FACTOR, String.valueOf(refineFactor));
    normalized.put(QUERY_MODE, queryMode.name().toLowerCase(Locale.ROOT));
    normalized.put(FRESHNESS_POLICY, freshnessPolicy.name().toLowerCase(Locale.ROOT));
    normalized.put(STALE_LOCATOR_POLICY, staleLocatorPolicy.name().toLowerCase(Locale.ROOT));
    normalized.put(FETCH_VERIFY_KEYS, String.valueOf(verifyFetchKeys));
    return Collections.unmodifiableMap(normalized);
  }

  public static VectorDistanceMetric getMetric(Map<String, String> options) {
    String value = getOption(options, METRIC, DEFAULT_METRIC.name());
    try {
      return VectorDistanceMetric.fromString(value);
    } catch (IllegalArgumentException e) {
      throw unsupportedValue(METRIC, value, e);
    }
  }

  private static VectorQuantizer getQuantizer(Map<String, String> options) {
    String value = getOption(options, QUANTIZER, DEFAULT_QUANTIZER.name());
    try {
      return VectorQuantizer.fromString(value);
    } catch (IllegalArgumentException e) {
      throw unsupportedValue(QUANTIZER, value, e);
    }
  }

  private static int getNumClusters(Map<String, String> options) {
    return getPositiveInt(options, NUM_CLUSTERS, DEFAULT_NUM_CLUSTERS);
  }

  private static int getMaxIter(Map<String, String> options) {
    return getPositiveInt(options, MAX_ITER, DEFAULT_MAX_ITER);
  }

  private static int getRaBitQBits(Map<String, String> options) {
    int bits = getInt(options, RABITQ_BITS, DEFAULT_RABITQ_BITS);
    if (bits < 1 || bits > 8) {
      throw new IllegalArgumentException(
          "Option '" + RABITQ_BITS + "' must be between 1 and 8: " + bits);
    }
    return bits;
  }

  private static long getRaBitQSeed(Map<String, String> options) {
    String value = getOption(options, RABITQ_SEED, String.valueOf(DEFAULT_RABITQ_SEED));
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw invalidNumber(RABITQ_SEED, value, e);
    }
  }

  private static boolean shouldAssumeNormalizedVectors(Map<String, String> options) {
    return getBoolean(options, RABITQ_ASSUME_NORMALIZED, false);
  }

  public static int getNumProbes(Map<String, String> options) {
    return getPositiveInt(options, QUERY_NUM_PROBES, DEFAULT_NUM_PROBES);
  }

  public static int getRefineFactor(Map<String, String> options) {
    return getPositiveInt(options, QUERY_REFINE_FACTOR, DEFAULT_REFINE_FACTOR);
  }

  private static VectorQueryMode getQueryMode(Map<String, String> options) {
    String value = getOption(options, QUERY_MODE, DEFAULT_QUERY_MODE.name());
    try {
      return VectorQueryMode.fromString(value);
    } catch (IllegalArgumentException e) {
      throw unsupportedValue(QUERY_MODE, value, e);
    }
  }

  public static VectorStalePolicy getFreshnessPolicy(Map<String, String> options) {
    VectorStalePolicy defaultPolicy = getQueryMode(options) == VectorQueryMode.APPROXIMATE
        ? DEFAULT_APPROXIMATE_FRESHNESS_POLICY : DEFAULT_EXACT_FRESHNESS_POLICY;
    return getStalePolicy(options, FRESHNESS_POLICY, defaultPolicy);
  }

  public static VectorStalePolicy getStaleLocatorPolicy(Map<String, String> options) {
    return getStalePolicy(options, STALE_LOCATOR_POLICY, DEFAULT_STALE_LOCATOR_POLICY);
  }

  private static VectorStalePolicy getStalePolicy(
      Map<String, String> options, String key, VectorStalePolicy defaultPolicy) {
    String value = getOption(options, key, defaultPolicy.name());
    try {
      return VectorStalePolicy.fromString(value);
    } catch (IllegalArgumentException e) {
      throw unsupportedValue(key, value, e);
    }
  }

  public static boolean isApproximateSearchMode(Map<String, String> options) {
    return getQueryMode(options) == VectorQueryMode.APPROXIMATE;
  }

  /** The current posting format uses symmetric scoring. */
  public static boolean isRaBitQAsymmetricScoring(Map<String, String> options) {
    return false;
  }

  /** RLI arbitration is mandatory for correctness in both query modes. */
  public static boolean isFinalistArbiterEnabled(Map<String, String> options) {
    return true;
  }

  public static boolean isStaleLocatorPolicyFail(Map<String, String> options) {
    return getStaleLocatorPolicy(options) == VectorStalePolicy.FAIL;
  }

  public static boolean shouldVerifyFetchKeys(Map<String, String> options) {
    return getBoolean(options, FETCH_VERIFY_KEYS, DEFAULT_FETCH_VERIFY_KEYS);
  }

  private static boolean getBoolean(Map<String, String> options, String key, boolean defaultValue) {
    String value = getOption(options, key, String.valueOf(defaultValue)).toLowerCase(Locale.ROOT);
    if (!"true".equals(value) && !"false".equals(value)) {
      throw new IllegalArgumentException(
          "Option '" + key + "' must be either 'true' or 'false': " + value);
    }
    return Boolean.parseBoolean(value);
  }

  private static int getPositiveInt(Map<String, String> options, String key, int defaultValue) {
    int value = getInt(options, key, defaultValue);
    if (value <= 0) {
      throw new IllegalArgumentException("Option '" + key + "' must be greater than 0: " + value);
    }
    return value;
  }

  private static int getInt(Map<String, String> options, String key, int defaultValue) {
    String value = getOption(options, key, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw invalidNumber(key, value, e);
    }
  }

  private static IllegalArgumentException invalidNumber(
      String key, String value, NumberFormatException cause) {
    return new IllegalArgumentException(
        "Option '" + key + "' must be a valid number: " + value, cause);
  }

  private static IllegalArgumentException unsupportedValue(
      String key, String value, IllegalArgumentException cause) {
    return new IllegalArgumentException(
        "Unsupported value for option '" + key + "': " + value, cause);
  }

  private static String getOption(Map<String, String> options, String key, String defaultValue) {
    String value = options.getOrDefault(key, defaultValue);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Option '" + key + "' must not be empty");
    }
    return value;
  }
}
