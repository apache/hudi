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
 * Counts tokens the way the target embedding model would, so chunks are cut below its input limit
 * (a configurable tokenizer computes the token count of the current chunk).
 * Implementations need a public no-arg constructor.
 */
public interface Tokenizer {

  int countTokens(String text);

  /**
   * Stable identity of this tokenizer; recorded in the preprocessing style text as {@code tokenizer=<id>} and pinned on the
   * table, with the class name, as {@code hoodie.table.embedding.prep.tokenizer.id} and {@code ...tokenizer.class}. A custom tokenizer's settings live outside the prep prefixes (an undeclared key
   * there is refused at job start), and a setting that changes its counts must change this id.
   */
  String id();

  /** What {@link #countTokens} counts. */
  TokenUnit unit();

  /**
   * Whether counts add up at a space that follows a non-whitespace char: {@code countTokens(a + b) == countTokens(a) +
   * countTokens(b)} whenever {@code b} starts with U+0020 and {@code a} ends with a char that is not whitespace in any
   * Unicode sense ({@link TokenBudget#isWordSpace}). It lets {@link TokenBudget} count a growing text by its new part
   * only. False (the default) makes every count a full count of the text, which is always correct.
   */
  default boolean addsUpAtWordSpaces() {
    return false;
  }

  /**
   * An end index {@code e} of {@code text} with {@code countTokens(text.substring(0, e)) <= maxTokens}, never inside a
   * surrogate pair: {@code text.length()} when the whole text fits, 0 when no code point does. Counts need not be
   * monotone in the prefix length (BPE), so the result is a CHECKED large end, not promised to be the largest: a binary
   * search over code-point boundaries that only ever moves its lower bound to an end it has counted within the limit.
   */
  default int prefixEndWithin(String text, int maxTokens) {
    if (text == null || text.isEmpty() || maxTokens <= 0) {
      return 0;
    }
    if (countTokens(text) <= maxTokens) {
      return text.length();
    }
    int fits = 0;
    int exceeds = text.length();
    while (true) {
      int mid = Utf16.floor(text, (fits + exceeds) >>> 1);
      if (mid <= fits) {
        mid = text.offsetByCodePoints(fits, 1);
      }
      if (mid >= exceeds) {
        return fits;
      }
      if (countTokens(text.substring(0, mid)) <= maxTokens) {
        fits = mid;
      } else {
        exceeds = mid;
      }
    }
  }

}
