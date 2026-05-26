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

import java.util.Map;

/**
 * Option keys accepted in the OPTIONS clause of {@code CREATE INDEX … USING VECTOR}.
 *
 * <pre>{@code
 * CREATE INDEX my_idx ON products USING VECTOR (embedding)
 * OPTIONS (
 *   'vector.dimension'              = '768',
 *   'vector.metric'                 = 'cosine',
 *   'vector.algorithm'              = 'ivfflat',
 *   'vector.quantizer'              = 'IVF_RABITQ',
 *   'vector.num_clusters'           = '4096',
 *   'vector.num_probes'             = '40',
 *   'vector.refine_factor'          = '10',
 *   'vector.max_iter'               = '20',
 *   'vector.rabitq.random_seed'     = '42',
 *   'vector.rabitq.assume_normalized' = 'false'
 * );
 * }</pre>
 */
public final class VectorIndexOptions {

  private VectorIndexOptions() {
  }

  // ---- core options -------------------------------------------------------

  /** Number of dimensions in the embedding vector. Required. */
  public static final String DIMENSION = "vector.dimension";

  /** Distance metric: {@code cosine} | {@code l2} | {@code dot_product}. Default: cosine. */
  public static final String METRIC = "vector.metric";

  /** Routing algorithm: {@code ivfflat} | {@code ivfpq_hnsw}. Default: ivfflat. */
  public static final String ALGORITHM = "vector.algorithm";

  /**
   * Fine-grained quantizer within clusters.
   * {@code IVF_RABITQ} (default) uses binary code popcount scan.
   * {@code IVF_PQ} uses product quantization (legacy, low-dimensional tables).
   */
  public static final String QUANTIZER = "vector.quantizer";

  /** Number of IVF clusters K. Default: min(sqrt(N), 16384). Set explicitly for large tables. */
  public static final String NUM_CLUSTERS = "vector.num_clusters";

  /** Number of clusters probed at query time. Default: 8. */
  public static final String NUM_PROBES = "vector.num_probes";

  /**
   * Re-rank factor: fetch refine_factor * K candidates from RaBitQ scan,
   * then exact-distance re-rank to produce final top-K. Default: 10.
   */
  public static final String REFINE_FACTOR = "vector.refine_factor";

  /** K-Means max iterations during index build. Default: 20. */
  public static final String MAX_ITER = "vector.max_iter";

  // ---- RaBitQ-specific options --------------------------------------------

  /**
   * Seed for the random orthogonal rotation matrix R.
   * R is deterministic given (seed, dimension) — no retraining ever needed.
   * Default: 42.
   */
  public static final String RABITQ_RANDOM_SEED = "vector.rabitq.random_seed";

  /**
   * When {@code true}, skips writing the scalar_factor (||v||) alongside binary codes.
   * Only safe when all vectors are guaranteed to have unit norm.
   * Default: false.
   */
  public static final String RABITQ_ASSUME_NORMALIZED = "vector.rabitq.assume_normalized";

  /**
   * When {@code true}, {@code CREATE INDEX ... USING VECTOR} will trigger a full-table rewrite
   * after bootstrap completes so RaBitQ hidden columns are materialized immediately for existing rows.
   * Default: false.
   */
  public static final String RABITQ_MATERIALIZE_ON_CREATE = "vector.rabitq.materialize.on.create";

  /**
   * Controls where RaBitQ codes are persisted for POC comparisons.
   * {@code hidden_columns} keeps the existing base-file hidden-column path.
   * {@code mdt_lookup} stores codes only in MDT posting rows.
   * {@code both} writes MDT posting rows in addition to the hidden-column path.
   * Default: hidden_columns.
   */
  public static final String RABITQ_STORAGE = "vector.rabitq.storage";

  /**
   * Target number of posting rows per MDT shard inside a single IVF cluster.
   * The actual shard count per cluster is computed as
   * {@code ceil(cluster_population / target_rows_per_shard)}, bounded by
   * {@link #RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER}.
   */
  public static final String RABITQ_POSTING_TARGET_ROWS_PER_SHARD = "vector.rabitq.posting.target_rows_per_shard";

  /**
   * Maximum number of MDT posting shards to create for a single IVF cluster.
   */
  public static final String RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER = "vector.rabitq.posting.max_shards_per_cluster";

  // ---- PQ-specific options (legacy, low-dimensional only) ----------------

  /** Number of PQ sub-quantizers M. Default: dimension / 8. */
  public static final String PQ_NUM_SUB_QUANTIZERS = "vector.pq.num_sub_quantizers";

  /** Bits per PQ sub-quantizer (256 centroids when 8). Default: 8. */
  public static final String PQ_BITS = "vector.pq.bits";

  // ---- defaults ----------------------------------------------------------

  public static final String DEFAULT_METRIC      = "cosine";
  public static final String DEFAULT_ALGORITHM   = "ivfflat";
  public static final String DEFAULT_QUANTIZER   = "IVF_RABITQ";
  public static final int    DEFAULT_NUM_CLUSTERS = 256;
  public static final int    DEFAULT_NUM_PROBES   = 8;
  public static final int    DEFAULT_REFINE_FACTOR = 10;
  public static final int    DEFAULT_MAX_ITER     = 20;
  public static final long   DEFAULT_RABITQ_SEED  = 42L;
  public static final String DEFAULT_RABITQ_STORAGE = "hidden_columns";
  public static final int    DEFAULT_RABITQ_POSTING_TARGET_ROWS_PER_SHARD = 4096;
  public static final int    DEFAULT_RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER = 64;
  public static final int    DEFAULT_PQ_BITS      = 8;

  // ---- helpers -----------------------------------------------------------

  public static int getDimension(Map<String, String> opts) {
    String v = opts.get(DIMENSION);
    if (v == null || v.isEmpty()) {
      throw new IllegalArgumentException(
          "Vector index requires '" + DIMENSION + "' in OPTIONS");
    }
    return Integer.parseInt(v);
  }

  public static VectorDistanceMetric getMetric(Map<String, String> opts) {
    return VectorDistanceMetric.fromString(opts.getOrDefault(METRIC, DEFAULT_METRIC));
  }

  public static VectorIndexType getAlgorithm(Map<String, String> opts) {
    return VectorIndexType.fromString(opts.getOrDefault(ALGORITHM, DEFAULT_ALGORITHM));
  }

  public static String getQuantizer(Map<String, String> opts) {
    return opts.getOrDefault(QUANTIZER, DEFAULT_QUANTIZER).toUpperCase();
  }

  public static int getNumClusters(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(NUM_CLUSTERS, String.valueOf(DEFAULT_NUM_CLUSTERS)));
  }

  public static int getNumProbes(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(NUM_PROBES, String.valueOf(DEFAULT_NUM_PROBES)));
  }

  public static int getRefineFactor(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(REFINE_FACTOR, String.valueOf(DEFAULT_REFINE_FACTOR)));
  }

  public static int getMaxIter(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(MAX_ITER, String.valueOf(DEFAULT_MAX_ITER)));
  }

  public static long getRaBitQSeed(Map<String, String> opts) {
    return Long.parseLong(
        opts.getOrDefault(RABITQ_RANDOM_SEED, String.valueOf(DEFAULT_RABITQ_SEED)));
  }

  public static boolean isRaBitQAssumeNormalized(Map<String, String> opts) {
    return Boolean.parseBoolean(opts.getOrDefault(RABITQ_ASSUME_NORMALIZED, "false"));
  }

  public static boolean shouldMaterializeRaBitQColumnsOnCreate(Map<String, String> opts) {
    return Boolean.parseBoolean(opts.getOrDefault(RABITQ_MATERIALIZE_ON_CREATE, "false"));
  }

  public static String getRaBitQStorage(Map<String, String> opts) {
    return opts.getOrDefault(RABITQ_STORAGE, DEFAULT_RABITQ_STORAGE).toLowerCase();
  }

  public static boolean shouldStoreRaBitQCodesInMdt(Map<String, String> opts) {
    String storage = getRaBitQStorage(opts);
    return "mdt_lookup".equals(storage) || "both".equals(storage);
  }

  public static int getRaBitQPostingTargetRowsPerShard(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(
            RABITQ_POSTING_TARGET_ROWS_PER_SHARD,
            String.valueOf(DEFAULT_RABITQ_POSTING_TARGET_ROWS_PER_SHARD)));
  }

  public static int getRaBitQPostingMaxShardsPerCluster(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(
            RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER,
            String.valueOf(DEFAULT_RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER)));
  }

  public static int getPqBits(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(PQ_BITS, String.valueOf(DEFAULT_PQ_BITS)));
  }

  public static int getPqNumSubQuantizers(Map<String, String> opts, int dimension) {
    return Integer.parseInt(
        opts.getOrDefault(PQ_NUM_SUB_QUANTIZERS, String.valueOf(dimension / 8)));
  }
}