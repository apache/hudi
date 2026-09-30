/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

import org.apache.hudi.common.data.HoodieData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Scores materialized vector posting matches using the configured RaBitQ estimator. */
final class VectorPostingScorer {

  // Preserve the established log category while moving implementation code mechanically.
  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexMdtSearchUtils.class);

  private VectorPostingScorer() {
  }

  static HoodieData<ScoredVectorPostingMatch> score(
      HoodieData<VectorPostingMatch> postingMatches,
      float[] queryVector,
      int dimension,
      long randomSeed,
      int rabitqBits,
      boolean assumeNormalized,
      VectorDistanceMetric metric,
      boolean asymmetric,
      boolean residualEncoding,
      float[][] centroids) {
    long queryPrepStartMs = System.currentTimeMillis();
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, rabitqBits, randomSeed, assumeNormalized);
    final VectorDistanceMetric scoringMetric = metric;
    final boolean useAsymmetric = asymmetric;
    final boolean useResidualEncoding = residualEncoding;
    final float[][] residualCentroids = centroids;
    final int effectiveBits = Math.max(1, rabitqBits);
    final boolean useMultibitEstimator = effectiveBits > 1;

    if (useMultibitEstimator && useAsymmetric) {
      throw new IllegalArgumentException("Multibit RaBitQ does not support asymmetric scoring.");
    }
    if (useResidualEncoding && (residualCentroids == null || residualCentroids.length == 0)) {
      throw new IllegalArgumentException("Residual RaBitQ scoring requires IVF centroids.");
    }

    final RaBitQQueryState sharedQueryState;
    if (useMultibitEstimator || useResidualEncoding) {
      sharedQueryState = null;
    } else {
      sharedQueryState = (RaBitQQueryState) encoder.encodeQuery(queryVector);
    }
    final MetricQueryState multibitMetricQueryState = useMultibitEstimator
        ? MetricQueryState.create(scoringMetric, encoder::rotateVector, queryVector, assumeNormalized)
        : null;
    final MetricQueryState.ClusterQuery originClusterQuery = useMultibitEstimator && !useResidualEncoding
        ? multibitMetricQueryState.forRotatedCentroid(new float[queryVector.length])
        : null;

    long queryPrepMs = System.currentTimeMillis() - queryPrepStartMs;
    LOG.info("[vector_search][stage][score_postings_setup] queryDim={} queryCodeBytes={} metric={} bits={} asymmetric={} residualEncoding={} elapsedMs={}",
        dimension,
        sharedQueryState == null ? Math.max(1, (dimension + 7) / 8) : sharedQueryState.getBinaryCode().length,
        scoringMetric,
        effectiveBits,
        useAsymmetric,
        useResidualEncoding,
        queryPrepMs);
    return postingMatches.mapPartitions(iterator -> {
      long partitionStartMs = System.currentTimeMillis();
      if (!iterator.hasNext()) {
        LOG.info("[vector_search][stage][score_postings] input=0 output=0 queryDim={} residualEncoding={} residualQueryStates=0 computeMs=0 elapsedMs={}",
            dimension,
            useResidualEncoding,
            System.currentTimeMillis() - partitionStartMs);
        return Collections.<ScoredVectorPostingMatch>emptyIterator();
      }

      List<ScoredVectorPostingMatch> scored = new ArrayList<>();
      Map<Integer, RaBitQQueryState> residualQueryStates = (useResidualEncoding && !useMultibitEstimator) ? new HashMap<>() : null;
      Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache =
          useResidualEncoding && useMultibitEstimator ? new HashMap<>() : null;
      float minDistance = Float.POSITIVE_INFINITY;
      float maxDistance = Float.NEGATIVE_INFINITY;
      long computeStartMs = System.currentTimeMillis();
      while (iterator.hasNext()) {
        VectorPostingMatch match = iterator.next();
        if (match.isDeleted()) {
          scored.add(new ScoredVectorPostingMatch(match, Float.POSITIVE_INFINITY, null));
          continue;
        }
        RaBitQQueryState activeQueryState = sharedQueryState;
        if (useResidualEncoding && !useMultibitEstimator) {
          activeQueryState = residualQueryStates.computeIfAbsent(match.getClusterId(), clusterId -> {
            if (clusterId < 0 || clusterId >= residualCentroids.length || residualCentroids[clusterId] == null) {
              throw new IllegalArgumentException("Missing centroid for residual clusterId=" + clusterId);
            }
            return (RaBitQQueryState) encoder.encodeQuery(VectorPostingSerde.subtract(queryVector, residualCentroids[clusterId]));
          });
        }
        final float approxDistance;
        if (useMultibitEstimator) {
          MetricQueryState.ClusterQuery clusterQuery = useResidualEncoding
              ? VectorIndexMdtSearchUtils.clusterQuery(
                  multibitMetricQueryState,
                  residualCentroids,
                  clusterQueryCache,
                  match.getClusterId())
              : originClusterQuery;
          float dotTerm = RaBitQEncoder.multibitDotTerm(
              clusterQuery.rotatedQuery,
              clusterQuery.querySum,
              match.getBinaryCode(),
              match.getExtendedCode(),
              dimension,
              effectiveBits);
          double rip = (match.getRescaleFactor() == null ? 0.0d : match.getRescaleFactor()) * (double) dotTerm;
          float centerRip = match.getAdditiveFactor() != null ? match.getAdditiveFactor() : 0.0f;
          float residualNorm = match.getScalar() != null ? match.getScalar() : 0.0f;
          float vectorNorm = match.getVectorNorm() != null ? match.getVectorNorm() : Float.NaN;
          approxDistance = (float) multibitMetricQueryState.rankingDistance(rip, centerRip, residualNorm, vectorNorm, clusterQuery);
        } else {
          float effectiveScalar = match.getScalar() != null ? match.getScalar() : 1.0f;
          approxDistance = encoder.estimateDistance(
              activeQueryState,
              new QuantizedVector(match.getBinaryCode(), effectiveScalar),
              scoringMetric,
              useAsymmetric);
        }
        scored.add(new ScoredVectorPostingMatch(match, approxDistance, match.toLocation().orElse(null)));
        minDistance = Math.min(minDistance, approxDistance);
        maxDistance = Math.max(maxDistance, approxDistance);
      }
      long computeMs = System.currentTimeMillis() - computeStartMs;
      LOG.info("[vector_search][stage][score_postings] input={} output={} queryDim={} bits={} residualEncoding={} residualQueryStates={} minDist={} maxDist={} computeMs={} elapsedMs={}",
          scored.size(),
          scored.size(),
          dimension,
          effectiveBits,
          useResidualEncoding,
          residualQueryStates == null ? (clusterQueryCache == null ? 0 : clusterQueryCache.size()) : residualQueryStates.size(),
          scored.isEmpty() ? "n/a" : minDistance,
          scored.isEmpty() ? "n/a" : maxDistance,
          computeMs,
          System.currentTimeMillis() - partitionStartMs);
      return scored.iterator();
    }, true);
  }

}
