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
 * The embedding endpoint rejected the client's configuration rather than an input (e.g. HTTP 401/403, an unknown model
 * or deployment, a request parameter it does not accept). Retrying the rows cannot help, so the compute run fails
 * without committing: the compute iterator ({@code MicroBatchEmbeddingIterator}) rethrows it, which fails the Spark task before any instant
 * is written. Data prep is a separate flow and is unaffected. The message never carries a credential. A broken prep/compute
 * invariant that no configuration causes (e.g. an image input without a prepared image) is an
 * {@code EmbeddingComputePlatformException}, never this type.
 */
public class EmbeddingClientConfigException extends HoodieException {

  public EmbeddingClientConfigException(String message) {
    super(message);
  }

  public EmbeddingClientConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
