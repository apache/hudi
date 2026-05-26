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

/**
 * Routing algorithms for Hudi vector indexes (RFC-vector-search §7).
 *
 * <p>The routing algorithm controls how the coarse quantizer maps query vectors
 * to a candidate set of IVF clusters. The fine-grained quantizer (RaBitQ or PQ)
 * is a separate configuration via {@link VectorIndexOptions#QUANTIZER}.
 *
 * <ul>
 *   <li>{@link #IVFFLAT}    – Phase 1: linear O(K) centroid scan, exact or RaBitQ
 *       compressed scan within selected clusters. No external deps.</li>
 *   <li>{@link #IVFPQ_HNSW} – Phase 2: O(log K) HNSW graph for centroid routing
 *       when K &gt; 10,000. Requires JVector dependency.</li>
 * </ul>
 */
public enum VectorIndexType {

  /**
   * Inverted-file with linear centroid probe (Phase 1).
   * Quantizer within clusters is controlled by {@link VectorIndexOptions#QUANTIZER}.
   */
  IVFFLAT,

  /**
   * IVF with HNSW graph over centroids for O(log K) routing (Phase 2).
   * Quantizer within clusters is controlled by {@link VectorIndexOptions#QUANTIZER}.
   */
  IVFPQ_HNSW;

  /**
   * Parse algorithm name (case-insensitive, hyphens or underscores both accepted).
   *
   * @param name e.g. "ivfflat", "ivfpq_hnsw", "ivfpq-hnsw"
   * @return matching constant
   */
  public static VectorIndexType fromString(String name) {
    return valueOf(name.toUpperCase().replace("-", "_").replace(" ", "_"));
  }
}