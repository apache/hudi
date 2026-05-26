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

package org.apache.hudi.sql;

import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorQuantizer;

import org.apache.spark.sql.api.java.UDF6;

/**
 * Spark SQL UDF that computes the RaBitQ approximate distance for a row-local hidden code.
 *
 * <p>This is intentionally Spark-first: notebook/script callers can register it through
 * {@code spark.udf.registerJavaFunction(...)} and then run distributed approximate scoring
 * over IVF-pruned candidate rows before an exact rerank pass.
 */
public class RaBitQApproxDistanceUDF
    implements UDF6<byte[], Float, String, Integer, Long, Boolean, Float> {

  private transient volatile CacheState cacheState;

  @Override
  public Float call(byte[] binaryCode,
                    Float scalar,
                    String queryVectorLiteral,
                    Integer dimension,
                    Long randomSeed,
                    Boolean assumeNormalized) {
    if (binaryCode == null || queryVectorLiteral == null || dimension == null
        || randomSeed == null || assumeNormalized == null) {
      return null;
    }

    CacheState state = getOrCreateCache(queryVectorLiteral, dimension, randomSeed, assumeNormalized);
    if (binaryCode.length != state.encoder.codeBytes()) {
      throw new IllegalArgumentException(String.format(
          "Expected binary code length %d for dimension %d, got %d",
          state.encoder.codeBytes(), dimension, binaryCode.length));
    }

    float effectiveScalar = scalar != null ? scalar : 1.0f;
    return state.encoder.estimateDistance(
        state.queryState,
        new VectorQuantizer.QuantizedVector(binaryCode, effectiveScalar));
  }

  private CacheState getOrCreateCache(String queryVectorLiteral,
                                      int dimension,
                                      long randomSeed,
                                      boolean assumeNormalized) {
    CacheState local = cacheState;
    if (local != null && local.matches(queryVectorLiteral, dimension, randomSeed, assumeNormalized)) {
      return local;
    }

    synchronized (this) {
      local = cacheState;
      if (local == null || !local.matches(queryVectorLiteral, dimension, randomSeed, assumeNormalized)) {
        float[] queryVector = parseQueryVector(queryVectorLiteral, dimension);
        RaBitQEncoder encoder = new RaBitQEncoder(dimension, randomSeed, assumeNormalized);
        cacheState = new CacheState(
            queryVectorLiteral,
            dimension,
            randomSeed,
            assumeNormalized,
            encoder,
            (RaBitQEncoder.RaBitQQueryState) encoder.encodeQuery(queryVector));
      }
      return cacheState;
    }
  }

  private static float[] parseQueryVector(String raw, int dimension) {
    String trimmed = raw.trim();
    if (trimmed.startsWith("[")) {
      trimmed = trimmed.substring(1);
    }
    if (trimmed.endsWith("]")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }

    if (trimmed.isEmpty()) {
      if (dimension == 0) {
        return new float[0];
      }
      throw new IllegalArgumentException("Query vector literal is empty");
    }

    String[] parts = trimmed.split(",");
    if (parts.length != dimension) {
      throw new IllegalArgumentException(String.format(
          "Expected query vector dimension %d but found %d values",
          dimension, parts.length));
    }

    float[] values = new float[dimension];
    for (int i = 0; i < parts.length; i++) {
      values[i] = Float.parseFloat(parts[i].trim());
    }
    return values;
  }

  private static final class CacheState {
    private final String queryVectorLiteral;
    private final int dimension;
    private final long randomSeed;
    private final boolean assumeNormalized;
    private final RaBitQEncoder encoder;
    private final RaBitQEncoder.RaBitQQueryState queryState;

    private CacheState(String queryVectorLiteral,
                       int dimension,
                       long randomSeed,
                       boolean assumeNormalized,
                       RaBitQEncoder encoder,
                       RaBitQEncoder.RaBitQQueryState queryState) {
      this.queryVectorLiteral = queryVectorLiteral;
      this.dimension = dimension;
      this.randomSeed = randomSeed;
      this.assumeNormalized = assumeNormalized;
      this.encoder = encoder;
      this.queryState = queryState;
    }

    private boolean matches(String otherQueryVectorLiteral,
                            int otherDimension,
                            long otherRandomSeed,
                            boolean otherAssumeNormalized) {
      return dimension == otherDimension
          && randomSeed == otherRandomSeed
          && assumeNormalized == otherAssumeNormalized
          && queryVectorLiteral.equals(otherQueryVectorLiteral);
    }
  }
}
