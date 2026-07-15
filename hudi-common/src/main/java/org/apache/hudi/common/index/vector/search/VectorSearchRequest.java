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
import java.util.Objects;

/**
 * Engine-neutral vector search request (RFC-104 v3 §1). Carries only the query intent and budget;
 * no engine, storage, or SQL types. Adapters (Spark/Flink/Java) translate their inputs into this.
 *
 * <p>{@code queryInstant} pins the table snapshot for the entire request; when null the executor
 * resolves the latest completed instant and records it in the result snapshot.
 */
public final class VectorSearchRequest implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String vectorColumn;
  private final float[] queryVector;
  private final VectorDistanceMetric metric;
  private final int topK;
  private final int nprobe;
  private final int refineFactor;
  private final boolean exactRerank;
  private final String queryInstant;
  private final VectorSearchBudget budget;

  public VectorSearchRequest(String vectorColumn,
                             float[] queryVector,
                             VectorDistanceMetric metric,
                             int topK,
                             int nprobe,
                             int refineFactor,
                             boolean exactRerank,
                             String queryInstant,
                             VectorSearchBudget budget) {
    this.vectorColumn = Objects.requireNonNull(vectorColumn, "vectorColumn");
    this.queryVector = Objects.requireNonNull(queryVector, "queryVector");
    this.metric = Objects.requireNonNull(metric, "metric");
    this.topK = topK;
    this.nprobe = nprobe;
    this.refineFactor = refineFactor;
    this.exactRerank = exactRerank;
    this.queryInstant = queryInstant;
    this.budget = Objects.requireNonNull(budget, "budget");
  }

  public String getVectorColumn() {
    return vectorColumn;
  }

  public float[] getQueryVector() {
    return queryVector;
  }

  public VectorDistanceMetric getMetric() {
    return metric;
  }

  public int getTopK() {
    return topK;
  }

  public int getNprobe() {
    return nprobe;
  }

  public int getRefineFactor() {
    return refineFactor;
  }

  public boolean isExactRerank() {
    return exactRerank;
  }

  /** Pinned table instant for the request, or null to resolve the latest completed instant. */
  public String getQueryInstant() {
    return queryInstant;
  }

  public VectorSearchBudget getBudget() {
    return budget;
  }
}
