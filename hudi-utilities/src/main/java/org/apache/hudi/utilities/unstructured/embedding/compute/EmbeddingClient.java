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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Computes embeddings by calling a third-party endpoint ("model embedding computation is always 3 party
 * service endpoint call"). One micro batch per {@link #embed} call.
 *
 * <p>Lifecycle per Spark task: built by {@link EmbeddingClientFactory} (no-arg constructor, then {@link #init} once) →
 * {@link #embed} per micro batch → {@link #close}.
 *
 * <p>Outcome contract: {@link #embed} returns exactly one {@link EmbeddingResult} per input, in input order, each OK
 * (with a vector of {@code spec().getDimension()} floats), RETRYABLE_FAILURE or PERMANENT_FAILURE. A thrown exception
 * means every input of that call is retryable; the caller then tries each input of the call alone, so one bad input does
 * not fail its batch-mates, and keeps trying a retryable input alone, with backoff, until it has been tried
 * {@code compute.max.attempts} times. An answer of the wrong size, or an OK result without a vector
 * of the right dimension, makes every input of that call retryable ("malformed answer"). Two exceptions are not retried:
 * {@link EmbeddingClientConfigException} (the endpoint rejects the configuration, e.g. HTTP 401) and
 * {@link EmbeddingProviderUnavailableException} (the client's outage short-circuit is open, or its credential's token endpoint
 * cannot be reached) fail the task, so the compute
 * commit writes nothing and the next sync retries its rows.
 *
 * <p>Modalities: {@link #supportedModalities} names the modalities the client has a request format for. Compute hands the
 * client only rows of the modalities the model declares ({@code hoodie.unstructured.embedding.model.<alias>.modalities}), and
 * the job refuses at start a declaration outside {@link #supportedModalities}.
 */
public interface EmbeddingClient extends AutoCloseable {

  /** The modalities of a client that embeds text only. */
  Set<String> TEXT_ONLY = Collections.singleton(EmbeddingTableSchema.MODALITY_TEXT);

  /** The modalities of a client that embeds text and images. */
  Set<String> TEXT_AND_IMAGE = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(
      EmbeddingTableSchema.MODALITY_TEXT, EmbeddingTableSchema.MODALITY_IMAGE)));

  /** Prepares the client for {@code spec}; called once, before any {@link #embed}. */
  void init(EmbeddingModelSpec spec, TypedProperties props);

  /**
   * The modalities this client has a request format for when it serves {@code modelName} (the model's {@code .name}), a subset
   * of {@code text} and {@code image}. Called on the driver at start, on an instance built with the no-arg constructor that
   * {@link #init} has not been called on, so it reads no configuration and opens no connection. Default: {@code text} only.
   */
  default Set<String> supportedModalities(String modelName) {
    return TEXT_ONLY;
  }

  /** The model this client was initialized for. */
  EmbeddingModelSpec spec();

  /**
   * Any other failure of the call as a whole is a {@link RuntimeException}; every input of the call is then retryable, except
   * for the failures the class comment names.
   *
   * @return one result per input, in input order
   * @throws InterruptedException the calling thread was interrupted while waiting (e.g. on a rate limit or a retry backoff)
   */
  List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws InterruptedException;

  /**
   * What the last {@link #embed} call cost at the provider (requests, retries, bisection requests, reported tokens,
   * wall time, rate-limit wait); {@link EmbeddingCallStats#NONE} for a client that reports nothing. The compute step reads it
   * after every call, also after one that threw, so a client that reports stats resets them when {@code embed} starts and sets
   * them when it ends, by return or by exception.
   */
  default EmbeddingCallStats lastCallStats() {
    return EmbeddingCallStats.NONE;
  }

  /**
   * Whether compute treats alike rejections across files as independent input failures. This is a provider-level promise,
   * not a description of how requests are batched. False by default, including clients that send one request per input.
   */
  default boolean answersEachInputAlone() {
    return false;
  }

  @Override
  default void close() {
  }
}
