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

package org.apache.hudi.utilities.unstructured.embedding.text;

/**
 * Builds the failure a {@link TokenBudget} or a {@link ChunkPacker} throws, so each caller keeps its own failure type.
 */
@FunctionalInterface
public interface PackFailures {

  /** What went wrong while packing. */
  enum Kind {
    /** A limit the packer itself guarantees was breached (a re-count after a cut, a tokenizer input above the window). */
    INTERNAL,
    /** The tokenizer failed, or not even one code point (or the chunk prefix alone) fits the token limit. */
    TOKENIZER_ERROR
  }

  /**
   * The failure to throw.
   *
   * @param cause the tokenizer's exception, or null
   */
  RuntimeException failure(Kind kind, String message, Throwable cause);
}
