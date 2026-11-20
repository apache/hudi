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

package org.apache.hudi.utilities.unstructured.embedding.compute;

/**
 * The outcome of embedding one {@link EmbeddingInput}: a vector ({@link Status#OK}) or a failure an
 * {@link EmbeddingClient} classified as worth retrying in a later compute run or not.
 */
public final class EmbeddingResult {

  /** Per-input outcome of one embed call. */
  public enum Status {
    /** Embedded; {@link #getVector()} holds the vector. */
    OK,
    /** Not embedded now; nothing is written for the row, so a later compute run selects and retries it. */
    RETRYABLE_FAILURE,
    /**
     * Retrying cannot help (e.g. the provider rejects this input): the row goes to the compute error table and the writer's
     * incremental rounds do not select it again; a snapshot or rewound read does, since its model column stays NULL.
     */
    PERMANENT_FAILURE
  }

  private final Status status;
  private final float[] vector;
  private final String error;
  private final boolean knownInputFailure;

  private EmbeddingResult(Status status, float[] vector, String error) {
    this(status, vector, error, false);
  }

  private EmbeddingResult(Status status, float[] vector, String error, boolean knownInputFailure) {
    this.status = status;
    this.vector = vector;
    this.error = error;
    this.knownInputFailure = knownInputFailure;
  }

  /**
   * @throws IllegalArgumentException if {@code vector} is null
   */
  public static EmbeddingResult ok(float[] vector) {
    if (vector == null) {
      throw new IllegalArgumentException("An OK embedding result needs a vector");
    }
    return new EmbeddingResult(Status.OK, vector, null);
  }

  public static EmbeddingResult retryable(String error) {
    return new EmbeddingResult(Status.RETRYABLE_FAILURE, null, error);
  }

  public static EmbeddingResult permanent(String error) {
    return new EmbeddingResult(Status.PERMANENT_FAILURE, null, error);
  }

  /** An input rejected locally before any provider request, independent of other inputs in its call. */
  public static EmbeddingResult invalidInput(String error) {
    return new EmbeddingResult(Status.PERMANENT_FAILURE, null, error, true);
  }

  /** Whether local validation established this input's failure without inferring a provider request refusal. */
  public boolean isKnownInputFailure() {
    return knownInputFailure;
  }

  public Status getStatus() {
    return status;
  }

  /** The vector; non-null iff {@link #getStatus()} is {@link Status#OK}. */
  public float[] getVector() {
    return vector;
  }

  /** Why the input was not embedded; null when OK. */
  public String getError() {
    return error;
  }

  @Override
  public String toString() {
    return status == Status.OK ? "OK(" + vector.length + " floats)" : status + "(" + error + ")";
  }
}
