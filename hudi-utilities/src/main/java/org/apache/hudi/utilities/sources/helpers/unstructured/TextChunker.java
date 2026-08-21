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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Splits extracted text into overlapping chunks for retrieval-oriented consumers,
 * breaking at natural boundaries where possible: each chunk is at most
 * {@code chunk.size.chars} long and ends at the last paragraph break, line break,
 * sentence end or word boundary inside its window (in that order of preference,
 * the recursive-splitting norm of retrieval pipelines), falling back to a hard cut
 * for unbroken text. Chunks are verbatim substrings so {@code char_start} offsets
 * always index into the original text.
 */
public class TextChunker implements Serializable {

  private static final long serialVersionUID = 1L;

  // in order of preference; a break lands just after the matched separator
  private static final String[] BOUNDARIES = {"\n\n", "\n", ". ", "! ", "? ", " "};

  private final int chunkSizeChars;
  private final int overlapChars;

  public TextChunker(int chunkSizeChars, int overlapChars) {
    ValidationUtils.checkArgument(chunkSizeChars > 0, "chunk size must be positive");
    ValidationUtils.checkArgument(overlapChars >= 0 && overlapChars < chunkSizeChars,
        "chunk overlap must be non-negative and smaller than the chunk size");
    this.chunkSizeChars = chunkSizeChars;
    this.overlapChars = overlapChars;
  }

  /**
   * One chunk of text and its position within the source document.
   */
  public static class Chunk implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int chunkId;
    public final String text;
    public final int charStart;

    public Chunk(int chunkId, String text, int charStart) {
      this.chunkId = chunkId;
      this.text = text;
      this.charStart = charStart;
    }
  }

  public List<Chunk> chunk(String text) {
    if (text == null || text.isEmpty()) {
      return Collections.emptyList();
    }
    List<Chunk> chunks = new ArrayList<>();
    int start = 0;
    for (int id = 0; start < text.length(); id++) {
      int windowEnd = Math.min(start + chunkSizeChars, text.length());
      int end = windowEnd == text.length() ? windowEnd : findBreak(text, start, windowEnd);
      chunks.add(new Chunk(id, text.substring(start, end), start));
      if (end == text.length()) {
        break;
      }
      start = end - overlapChars;
    }
    return chunks;
  }

  /**
   * Best break position in {@code (minBreak, windowEnd]}, preferring the strongest
   * boundary. The floor guarantees forward progress after overlap is subtracted and
   * keeps boundary chunks from degenerating below half the window.
   */
  private int findBreak(String text, int start, int windowEnd) {
    int minBreak = start + Math.max(overlapChars + 1, chunkSizeChars / 2);
    for (String boundary : BOUNDARIES) {
      int length = boundary.length();
      // Only a break landing in (minBreak, windowEnd] is acceptable, so scan just that
      // window. lastIndexOf would instead run back to index 0 whenever the boundary is
      // absent, making chunking quadratic in document length.
      int lowestIdx = Math.max(0, minBreak - length + 1);
      for (int idx = windowEnd - length; idx >= lowestIdx; idx--) {
        if (text.startsWith(boundary, idx)) {
          return idx + length;
        }
      }
    }
    return windowEnd;
  }
}
