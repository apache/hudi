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

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.index.vector.VectorDistanceMetric;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Validated reader artifacts for one active vector-index generation. */
public final class LoadedVectorIndexMetadata implements Serializable {

  private static final long serialVersionUID = 1L;

  private final VectorIndexSnapshot snapshot;
  private final float[][] centroids;
  private final Map<Integer, Integer> shardCounts;
  private final int dimension;
  private final int rabitqBits;
  private final long randomSeed;
  private final boolean assumeNormalized;
  private final VectorDistanceMetric metric;
  private final String vectorColumn;

  public LoadedVectorIndexMetadata(VectorIndexSnapshot snapshot,
                                   float[][] centroids,
                                   Map<Integer, Integer> shardCounts,
                                   int dimension,
                                   int rabitqBits,
                                   long randomSeed,
                                   boolean assumeNormalized,
                                   VectorDistanceMetric metric,
                                   String vectorColumn) {
    this.snapshot = snapshot;
    this.centroids = copy(centroids);
    this.shardCounts = Collections.unmodifiableMap(new HashMap<>(shardCounts));
    this.dimension = dimension;
    this.rabitqBits = rabitqBits;
    this.randomSeed = randomSeed;
    this.assumeNormalized = assumeNormalized;
    this.metric = metric;
    this.vectorColumn = vectorColumn;
  }

  public VectorIndexSnapshot getSnapshot() {
    return snapshot;
  }

  public float[][] getCentroids() {
    return copy(centroids);
  }

  public Map<Integer, Integer> getShardCounts() {
    return shardCounts;
  }

  public int getDimension() {
    return dimension;
  }

  public int getRaBitQBits() {
    return rabitqBits;
  }

  public long getRandomSeed() {
    return randomSeed;
  }

  public boolean isAssumeNormalized() {
    return assumeNormalized;
  }

  public VectorDistanceMetric getMetric() {
    return metric;
  }

  public String getVectorColumn() {
    return vectorColumn;
  }

  private static float[][] copy(float[][] source) {
    float[][] result = new float[source.length][];
    for (int i = 0; i < source.length; i++) {
      result[i] = source[i].clone();
    }
    return result;
  }
}
