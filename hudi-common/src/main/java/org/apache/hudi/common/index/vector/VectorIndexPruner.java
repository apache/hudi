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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Engine-agnostic IVF cluster pruner for vector queries.
 *
 * <p>Given a set of centroids and a file-group-to-cluster mapping, determines which
 * file groups need to be scanned for an approximate nearest-neighbour query.
 *
 * <p>Used by both the Spark file index ({@code HoodieVectorAwareFileIndex}) and the
 * Trino split manager ({@code HudiVectorSplitManager}), keeping all math in one place.
 *
 * <p>Instances are built from MDT data at query planning time and are short-lived
 * (one per query or per query batch). They are not cached between queries because
 * centroids can change after LIRE compaction.
 */
public final class VectorIndexPruner implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Centroid vectors keyed by cluster id (0-based). */
  private final float[][] centroids;

  /**
   * Mapping from cluster id → set of file group ids containing vectors in that cluster.
   * The inner sets are unmodifiable.
   */
  private final Map<Integer, Set<String>> clusterToFileGroups;

  /** Distance metric for centroid scoring. */
  private final VectorDistanceMetric metric;

  /**
   * @param centroids          centroid vectors, indexed by cluster id
   * @param clusterToFileGroups mapping built from the MDT fg_mapping partition
   * @param metric             distance metric matching the index definition
   */
  public VectorIndexPruner(
      float[][] centroids,
      Map<Integer, Set<String>> clusterToFileGroups,
      VectorDistanceMetric metric) {
    this.centroids           = centroids;
    this.clusterToFileGroups = clusterToFileGroups;
    this.metric              = metric;
  }

  /**
   * Returns the set of file group ids that must be scanned to answer an ANN query.
   *
   * @param queryVector the query embedding
   * @param numProbes   number of clusters to probe (nProbes)
   * @return file group ids; never null, may be empty if the index is not yet initialized
   */
  public Set<String> probe(float[] queryVector, int numProbes) {
    if (centroids == null || centroids.length == 0) {
      return Collections.emptySet();
    }
    int effectiveProbes = Math.min(numProbes, centroids.length);
    int[] topClusters   = findTopClusters(queryVector, effectiveProbes);

    Set<String> fileGroups = new HashSet<>();
    for (int clusterId : topClusters) {
      Set<String> fgs = clusterToFileGroups.get(clusterId);
      if (fgs != null) {
        fileGroups.addAll(fgs);
      }
    }
    return Collections.unmodifiableSet(fileGroups);
  }

  /**
   * Returns the cluster ids closest to the query (sorted best-first).
   * Linear scan; HNSW routing replaces this in Phase 2.
   */
  public int[] findTopClusters(float[] query, int numProbes) {
    int k = centroids.length;
    // scored[i] = (distance, cluster_id)
    List<float[]> scored = new ArrayList<>(k);
    for (int i = 0; i < k; i++) {
      float dist = metric.compute(query, centroids[i]);
      scored.add(new float[]{dist, i});
    }
    scored.sort((a, b) -> Float.compare(a[0], b[0]));

    int[] result = new int[numProbes];
    for (int i = 0; i < numProbes; i++) {
      result[i] = (int) scored.get(i)[1];
    }
    return result;
  }

  /** Returns the number of clusters (K). */
  public int numClusters() {
    return centroids == null ? 0 : centroids.length;
  }

  // ---- factory helpers ---------------------------------------------------

  /**
   * Builds a cluster→file-group map from a flat assignment list.
   * Each entry in {@code assignments} must be a three-element array:
   * {@code [clusterId (int), fileGroupId (String), partitionPath (String)]}.
   *
   * <p>The returned map is partition-aware: pass {@code partitionFilter = null}
   * to include all partitions, or a non-null set to restrict to specific partitions.
   *
   * @param assignments    rows from the MDT assignments/fg_mapping partition
   * @param partitionFilter partition paths to include; null = all
   * @return cluster → file-group mapping
   */
  public static Map<Integer, Set<String>> buildClusterMap(
      Iterable<Object[]> assignments,
      Set<String> partitionFilter) {
    Map<Integer, Set<String>> map = new HashMap<>();
    for (Object[] row : assignments) {
      int    clusterId      = ((Number) row[0]).intValue();
      String fileGroupId    = (String) row[1];
      String partitionPath  = (String) row[2];

      if (partitionFilter != null && !partitionFilter.contains(partitionPath)) {
        continue;
      }
      map.computeIfAbsent(clusterId, k -> new HashSet<>()).add(fileGroupId);
    }
    return map;
  }
}