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

package org.apache.hudi.utilities.transform.embedding;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;

import java.io.Serializable;
import java.util.List;

/**
 * Produces embedding vectors for batches of texts. Implementations run inside Spark
 * executors and are called with record-level batches buffered within a partition; an
 * in-JVM (e.g. ONNX) implementation can slot in beside API-backed ones.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface EmbeddingProvider extends Serializable {

  /**
   * Called once per executor instance before the first {@link #embed}.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  default void init(TypedProperties props) {
  }

  /**
   * Embeds a batch of texts, returning one vector per input, in order.
   * Errors should be retried internally where transient; a thrown exception
   * fails the batch (and the sync) -- the caller never silently drops vectors.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  List<float[]> embed(List<String> texts);
}
