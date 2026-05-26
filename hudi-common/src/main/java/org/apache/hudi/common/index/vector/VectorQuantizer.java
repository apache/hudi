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

/**
 * Pluggable fine-grained quantizer used within IVF clusters.
 *
 * <p>Implementations encode database vectors into compact binary codes at write time
 * and provide estimated distances at query time, enabling a fast coarse scan before
 * exact distance re-ranking.
 *
 * <p>All implementations must be serializable (for Spark task distribution) and
 * deterministic given the same construction parameters.
 *
 * <p>Implementations in use:
 * <ul>
 *   <li>{@link RaBitQEncoder} — randomized binary quantization (default)</li>
 *   <li>PQEncoder — product quantization (legacy, low-dimensional tables)</li>
 * </ul>
 */
public interface VectorQuantizer extends Serializable {

  /**
   * Encodes a database vector into a compact binary representation.
   *
   * @param vector the raw float vector (any norm)
   * @return encoded result containing the binary code and scalar factor
   */
  QuantizedVector encode(float[] vector);

  /**
   * Encodes a query vector for use with {@link #estimateDistance}.
   * For RaBitQ: returns the binarized rotated query packed as bytes.
   * For PQ: returns ADC lookup tables.
   *
   * @param queryVector the query float vector
   * @return query state for fast distance estimation
   */
  QueryState encodeQuery(float[] queryVector);

  /**
   * Computes an approximate distance from a query state to an encoded database vector.
   * Smaller = more similar (min-heap convention).
   *
   * @param queryState  result of {@link #encodeQuery}
   * @param encoded     result of {@link #encode}
   * @return approximate distance (lower = closer)
   */
  float estimateDistance(QueryState queryState, QuantizedVector encoded);

  /**
   * Returns the byte length of the binary code produced by {@link #encode}.
   * Determines the on-disk size of the hidden column.
   */
  int codeBytes();

  // ---- value types -------------------------------------------------------

  /**
   * Result of encoding a database vector.
   */
  final class QuantizedVector implements Serializable {
    /** Packed binary code (length = {@link #codeBytes()}). */
    public final byte[] code;
    /**
     * Original vector norm. Used by RaBitQ to scale the distance estimate.
     * Set to 1.0f when assume_normalized=true.
     */
    public final float scalar;

    public QuantizedVector(byte[] code, float scalar) {
      this.code   = code;
      this.scalar = scalar;
    }
  }

  /**
   * Opaque per-query state (e.g. binarized query for RaBitQ, ADC tables for PQ).
   */
  interface QueryState extends Serializable {}
}