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
 * Option keys accepted in the OPTIONS clause of
 * {@code CREATE INDEX … USING VECTOR}.
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

  /**
   * Distance metric: {@code cosine} | {@code l2} | {@code dot_product}. Default:
   * cosine.
   */
  public static final String METRIC = "vector.metric";

  /**
   * Routing algorithm: {@code ivfflat} | {@code ivfpq_hnsw}. Default: ivfflat.
   */
  public static final String ALGORITHM = "vector.algorithm";

  /**
   * Fine-grained quantizer within clusters.
   * {@code IVF_RABITQ} (default) uses binary code popcount scan.
   * {@code IVF_PQ} uses product quantization.
   */
  public static final String QUANTIZER = "vector.quantizer";

  /**
   * Number of IVF clusters K. Default: min(sqrt(N), 16384). Set explicitly for
   * large tables.
   */
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
   * Total RaBitQ bit width per dimension. {@code 1} is sign-only RaBitQ;
   * {@code >1} enables packed lower-bit planes for higher-accuracy MDT scoring.
   * Default: 2.
   */
  public static final String RABITQ_BITS = "vector.rabitq.bits";

  /**
   * When {@code true}, skips writing the scalar_factor (||v||) alongside binary
   * codes.
   * Only safe when all vectors are guaranteed to have unit norm.
   * Default: false.
   */
  public static final String RABITQ_ASSUME_NORMALIZED = "vector.rabitq.assume_normalized";

  /**
   * When {@code true}, {@code CREATE INDEX ... USING VECTOR} will trigger a
   * full-table rewrite
   * after bootstrap completes so RaBitQ hidden columns are materialized
   * immediately for existing rows.
   * Default: false.
   */
  public static final String RABITQ_MATERIALIZE_ON_CREATE = "vector.rabitq.materialize.on.create";

  /**
   * Controls where RaBitQ codes are persisted.
   * {@code hidden_columns} keeps the base-file hidden-column path.
   * {@code mdt_lookup} stores codes only in MDT posting rows.
   * {@code both} writes MDT posting rows in addition to the hidden-column path.
   * Default: mdt_lookup for the fresh MDT-native multibit implementation.
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

  /**
   * Target encoded posting-block payload size. The writer resolves this into a power-of-two
   * vectors-per-block value and freezes that value in the generation manifest.
   */
  public static final String RABITQ_POSTING_TARGET_BLOCK_BYTES = "vector.rabitq.posting.target_block_bytes";

  /**
   * Number of IVF clusters packed into a single MDT vector-index file group.
   *
   * <p>The MDT vector partition is laid out so that posting rows are physically co-located by
   * IVF cluster: a query that probes cluster {@code c} opens only the file group(s) holding
   * {@code c}'s postings. The number of MDT file groups is derived as
   * {@code ceil(num_clusters / clusters_per_file_group)} and posting rows are routed by
   * {@code clusterId % fileGroupCount} (see
   * {@code HoodieTableMetadataUtil.mapVectorPostingKeyToFileGroupIndex}).
   *
   * <ul>
   *   <li>{@code 1} (default) — one IVF cluster per MDT file group: tightest probe pruning
   *       and the most localized LIRE maintenance, at the cost of {@code num_clusters} file
   *       groups.</li>
   *   <li>{@code >1} — pack several clusters per file group to bound the total file count
   *       for very large {@code num_clusters} (e.g. the 1B-vector case), trading a little
   *       pruning precision for fewer/larger files.</li>
   * </ul>
   *
   */
  public static final String CLUSTERS_PER_FILE_GROUP = "vector.clusters_per_file_group";

  // ---- search-time options ------------------------------------------------

  /**
   * Controls the exact-read behaviour of {@code hudi_vector_search}.
   * <ul>
   * <li>{@code exact} (default) — reads base-table rows and computes exact
   * vector distances for the top-K candidates.</li>
   * <li>{@code approximate} — skips the base-table read entirely and returns
   * RaBitQ approximate distances. Much faster (no Parquet I/O) but
   * distances are estimates and only record-key / partition-path columns
   * are returned.</li>
   * </ul>
   */
  public static final String SEARCH_MODE = "vector.search.mode";
  public static final String DEFAULT_SEARCH_MODE = "exact";

  /**
   * Controls exact positional fetch behavior when a base Parquet file is missing page indexes.
   * <ul>
   * <li>{@code fallback} (default) — read the target row group and count
   * {@code offsetIndexMissing}; availability is preferred over page-level pruning.</li>
   * <li>{@code fail} — fail the query immediately. Useful for validation runs that require
   * page-index honoring.</li>
   * </ul>
   */
  public static final String EXACT_FETCH_OFFSET_INDEX_MISSING_POLICY = "vector.exact_fetch.offset_index_missing_policy";
  public static final String DEFAULT_EXACT_FETCH_OFFSET_INDEX_MISSING_POLICY = "fallback";

  /**
   * When {@code true}, Hudi clustering on the <b>main data table</b> will sort
   * records by their IVF K-Means cluster assignment (read from vector index
   * posting records in the MDT). This co-locates vectors that belong to the
   * same IVF cluster in the same data-table file groups, so the exact-read
   * phase of {@code hudi_vector_search} opens far fewer files (probed clusters
   * map to a handful of file groups instead of being scattered across all of
   * them).
   *
   * <p>
   * To use this, set this option on the vector index definition and then
   * configure Hudi clustering on the main data table. The clustering
   * execution strategy will look up each record's cluster ID from the MDT and
   * use it as the sort key.
   */
  public static final String CLUSTERING_DATA_TABLE_BY_IVF = "vector.data_table.cluster_by_ivf";

  /**
   * When {@code true}, RaBitQ stores and scores IVF-cluster residuals ({@code x - centroid})
   * instead of full vectors. This is often a better fit for non-normalized L2 data because
   * approximate ranking happens inside a probed cluster, where residual directions/magnitudes can
   * be much more faithful than full-vector directions dominated by global norms. Default: false.
   */
  public static final String RABITQ_RESIDUAL_ENCODING = "vector.rabitq.residual";

  /**
   * When {@code true}, RaBitQ approximate scoring uses the lower-variance <i>asymmetric</i>
   * estimator (full-precision rotated query dotted against the ±1 data signs) instead of the
   * default binary-symmetric Hamming estimator. Higher recall for the same posting scan, at the
   * cost of ~D adds/posting instead of D/8 popcount bytes (compute, not the IO bottleneck).
   * Independent of {@code vector.metric}: both estimators feed the same metric reconstruction.
   * Default: false.
   */
  public static final String RABITQ_ASYMMETRIC_SCORING = "vector.rabitq.asymmetric";

  /**
   * Read-side policy for stale finalists — a posting whose locator no longer matches the record's
   * current RLI location (rewritten by compaction/clustering/upsert on the base table):
   * {@code fallback} (default) routes the candidate to a key-based fallback fetch at its live
   * location; {@code fail} retains the hard throw. See RFC-109 "Upsert and Delete Support".
   */
  public static final String STALE_POLICY = "vector.read.stale.policy";
  public static final String STALE_POLICY_FALLBACK = "fallback";
  public static final String STALE_POLICY_FAIL = "fail";

  /**
   * Enables the RFC-109 RLI finalist arbiter (freshness gate) on the read path. When {@code true},
   * finalists are classified against the record-level index so stale (rewritten/moved) and deleted
   * postings are excluded (approx mode) or key-fallback-fetched (exact mode). Default {@code false}:
   * dormant until the commit-time delta writer (upsert/delete work-stream item 2) lands, at which
   * point it becomes load-bearing. Off = the pre-upsert positional-trust behaviour, with no extra
   * RLI lookup per query. MUST NOT be enabled on a table without a record-level index.
   */
  public static final String READ_FINALIST_ARBITER = "vector.read.finalist_arbiter";
  public static final boolean DEFAULT_READ_FINALIST_ARBITER = false;
  // ---- defaults ----------------------------------------------------------

  public static final String DEFAULT_METRIC = "cosine";
  public static final String DEFAULT_ALGORITHM = "ivfflat";
  public static final String DEFAULT_QUANTIZER = "IVF_RABITQ";
  public static final int DEFAULT_NUM_CLUSTERS = 256;
  public static final int DEFAULT_NUM_PROBES = 8;
  public static final int DEFAULT_REFINE_FACTOR = 10;
  public static final int DEFAULT_MAX_ITER = 20;
  public static final long DEFAULT_RABITQ_SEED = 42L;
  public static final int DEFAULT_RABITQ_BITS = 2;
  public static final String DEFAULT_RABITQ_STORAGE = "mdt_lookup";
  public static final int DEFAULT_RABITQ_POSTING_TARGET_ROWS_PER_SHARD = 4096;
  public static final int    DEFAULT_RABITQ_POSTING_MAX_SHARDS_PER_CLUSTER = 64;
  public static final int    DEFAULT_RABITQ_POSTING_TARGET_BLOCK_BYTES = 524288;
  public static final int    DEFAULT_CLUSTERS_PER_FILE_GROUP = 1;

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

  public static int getClustersPerFileGroup(Map<String, String> opts) {
    return Math.max(1, Integer.parseInt(
        opts.getOrDefault(
            CLUSTERS_PER_FILE_GROUP,
            String.valueOf(DEFAULT_CLUSTERS_PER_FILE_GROUP))));
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

  public static int getRaBitQBits(Map<String, String> opts) {
    int bits = Integer.parseInt(
        opts.getOrDefault(RABITQ_BITS, String.valueOf(DEFAULT_RABITQ_BITS)));
    if (bits <= 0 || bits > 8) {
      throw new IllegalArgumentException("RaBitQ bits must be in [1, 8], got: " + bits);
    }
    return bits;
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

  public static int getRaBitQPostingTargetBlockBytes(Map<String, String> opts) {
    return Integer.parseInt(
        opts.getOrDefault(
            RABITQ_POSTING_TARGET_BLOCK_BYTES,
            String.valueOf(DEFAULT_RABITQ_POSTING_TARGET_BLOCK_BYTES)));
  }

  public static String getSearchMode(Map<String, String> opts) {
    return opts.getOrDefault(SEARCH_MODE, DEFAULT_SEARCH_MODE).toLowerCase();
  }

  public static boolean shouldFailOnMissingExactFetchOffsetIndex(Map<String, String> opts) {
    return "fail".equalsIgnoreCase(opts.getOrDefault(
        EXACT_FETCH_OFFSET_INDEX_MISSING_POLICY,
        DEFAULT_EXACT_FETCH_OFFSET_INDEX_MISSING_POLICY));
  }

  public static boolean isApproximateSearchMode(Map<String, String> opts) {
    return "approximate".equals(getSearchMode(opts));
  }

  public static boolean isDataTableClusterByIvf(Map<String, String> opts) {
    return Boolean.parseBoolean(opts.getOrDefault(CLUSTERING_DATA_TABLE_BY_IVF, "false"));
  }

  public static boolean isRaBitQResidualEncoding(Map<String, String> opts) {
    return Boolean.parseBoolean(opts.getOrDefault(RABITQ_RESIDUAL_ENCODING, "false"));
  }

  public static boolean isRaBitQAsymmetricScoring(Map<String, String> opts) {
    return Boolean.parseBoolean(opts.getOrDefault(RABITQ_ASYMMETRIC_SCORING, "false"));
  }

  /**
   * @return {@code true} when the stale-finalist policy is {@code fail} (retain the hard throw);
   *     {@code false} (default) routes stale finalists to key-based fallback via the RLI arbiter.
   */
  public static boolean isStalePolicyFail(Map<String, String> opts) {
    return STALE_POLICY_FAIL.equalsIgnoreCase(opts.getOrDefault(STALE_POLICY, STALE_POLICY_FALLBACK));
  }

  /**
   * @return {@code true} when the RLI finalist arbiter is enabled (default {@code false}). See
   *     {@link #READ_FINALIST_ARBITER}.
   */
  public static boolean isFinalistArbiterEnabled(Map<String, String> opts) {
    return Boolean.parseBoolean(
        opts.getOrDefault(READ_FINALIST_ARBITER, String.valueOf(DEFAULT_READ_FINALIST_ARBITER)));
  }
}