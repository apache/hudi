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

import org.apache.hudi.exception.HoodieException;

/**
 * The compute step fails on a transient condition: a row still RETRYABLE after {@code compute.max.attempts} tries, a client
 * that cannot start, or a provider reported unavailable. It fails the Spark task.
 */
public class EmbeddingComputeTransientException extends HoodieException {

  public EmbeddingComputeTransientException(String message) {
    super(message);
  }

  public EmbeddingComputeTransientException(String message, Throwable cause) {
    super(message, cause);
  }
}
