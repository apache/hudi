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
 * One chunk as cut by {@link ChunkPacker}: the text (chunk prefix + overlap + new text, the prefix empty for the PDF chunker,
 * so its text is an exact slice of the stream) and its bookkeeping. A mark is what the caller appended the text under: a
 * 1-based page for the PDF chunker.
 */
public final class PackedChunk {

  final String text;
  final int overlapChars;
  final int prefixChars;
  final int pageStart;
  final int pageEnd;
  final int tokenCount;
  final int wordCount;

  PackedChunk(String text, int overlapChars, int prefixChars, int pageStart, int pageEnd, int tokenCount, int wordCount) {
    this.text = text;
    this.overlapChars = overlapChars;
    this.prefixChars = prefixChars;
    this.pageStart = pageStart;
    this.pageEnd = pageEnd;
    this.tokenCount = tokenCount;
    this.wordCount = wordCount;
  }

  /** The chunk text: {@link #prefixChars()} chars of chunk prefix, then {@link #overlapChars()} chars of overlap, then new text. */
  public String text() {
    return text;
  }

  /** Chars of the chunk prefix at the start of {@link #text()}; 0 for the PDF chunker, which puts no prefix before a chunk. */
  public int prefixChars() {
    return prefixChars;
  }

  /** Chars of overlap carried from the previous chunk, after the prefix. */
  public int overlapChars() {
    return overlapChars;
  }

  /** The mark of the chunk's first stream char (its overlap's first char, else its first new char). */
  public int firstMark() {
    return pageStart;
  }

  /** The mark of the chunk's last char. */
  public int lastMark() {
    return pageEnd;
  }

  /** Tokens of {@link #text()} under the budget's tokenizer, re-counted after the cut. */
  public int tokenCount() {
    return tokenCount;
  }

  /** Words of {@link #text()}. */
  public int wordCount() {
    return wordCount;
  }
}
