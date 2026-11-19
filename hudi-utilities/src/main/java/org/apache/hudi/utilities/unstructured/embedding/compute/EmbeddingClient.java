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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;

import java.util.List;

/**
 * Computes one result per input, in order. A task initializes its client, calls embed per micro batch, then closes it.
 */
public interface EmbeddingClient extends AutoCloseable {

  /** Prepares the client for {@code spec}; called once, before any {@link #embed}. */
  void init(EmbeddingModelSpec spec, TypedProperties props);

  /** The model this client was initialized for. */
  EmbeddingModelSpec spec();

  /**
   * @return one result per input, in input order
   * @throws Exception any failure of the call as a whole; every input of the call is then retryable
   */
  List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws Exception;

  @Override
  default void close() {
  }
}
