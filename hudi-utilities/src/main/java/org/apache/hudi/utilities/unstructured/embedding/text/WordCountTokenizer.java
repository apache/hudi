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
 * Counts whitespace-separated words ({@link TokenBudget#words}).
 */
public class WordCountTokenizer implements Tokenizer {

  /** Identity of this tokenizer, also its {@code hoodie.unstructured.chunk.tokenizer.type}. */
  public static final String TOKENIZER_ID = "word-count";

  @Override
  public int countTokens(String text) {
    return text == null ? 0 : TokenBudget.words(text);
  }

  /**
   * True: a word never spans a space, so the words of {@code a + b} are those of {@code a} plus those of {@code b}. A
   * subclass may count differently, so it adds up only when it says so itself.
   */
  @Override
  public boolean addsUpAtWordSpaces() {
    return getClass() == WordCountTokenizer.class;
  }

  @Override
  public String id() {
    return TOKENIZER_ID;
  }

  @Override
  public TokenUnit unit() {
    return TokenUnit.WORDS;
  }
}
