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
import java.util.List;

/**
 * Selects the closest IVF centroids for a vector query.
 *
 * <p>Partition-aware posting filtering is deliberately outside this class. It ranks clusters only;
 * the query engine decides which postings are eligible for its table snapshot and predicates.
 */
public final class VectorIndexPruner implements Serializable {

  private static final long serialVersionUID = 1L;

  private final float[][] centroids;
  private final VectorDistanceMetric metric;

  public VectorIndexPruner(float[][] centroids, VectorDistanceMetric metric) {
    this.centroids = centroids;
    this.metric = metric;
  }

  /**
   * Returns up to {@code numProbes} cluster ids ordered by ascending distance.
   */
  public int[] findTopClusters(float[] query, int numProbes) {
    if (numProbes <= 0) {
      throw new IllegalArgumentException("Number of probes must be greater than zero: " + numProbes);
    }
    if (centroids == null || centroids.length == 0) {
      return new int[0];
    }

    List<ClusterDistance> scored = new ArrayList<>(centroids.length);
    for (int clusterId = 0; clusterId < centroids.length; clusterId++) {
      scored.add(new ClusterDistance(clusterId, metric.compute(query, centroids[clusterId])));
    }
    scored.sort((left, right) -> {
      int distanceComparison = Float.compare(left.distance, right.distance);
      return distanceComparison != 0
          ? distanceComparison
          : Integer.compare(left.clusterId, right.clusterId);
    });

    int resultSize = Math.min(numProbes, scored.size());
    int[] result = new int[resultSize];
    for (int i = 0; i < resultSize; i++) {
      result[i] = scored.get(i).clusterId;
    }
    return result;
  }

  public int numClusters() {
    return centroids == null ? 0 : centroids.length;
  }

  private static final class ClusterDistance {
    private final int clusterId;
    private final float distance;

    private ClusterDistance(int clusterId, float distance) {
      this.clusterId = clusterId;
      this.distance = distance;
    }
  }
}
