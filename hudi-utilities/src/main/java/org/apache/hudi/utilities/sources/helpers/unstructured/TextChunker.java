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
 * Splits extracted text into fixed-size, overlapping character chunks for
 * retrieval-oriented consumers.
 */
public class TextChunker implements Serializable {

  private static final long serialVersionUID = 1L;

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
    int step = chunkSizeChars - overlapChars;
    for (int start = 0, id = 0; start < text.length(); start += step, id++) {
      int end = Math.min(start + chunkSizeChars, text.length());
      chunks.add(new Chunk(id, text.substring(start, end), start));
      if (end == text.length()) {
        break;
      }
    }
    return chunks;
  }
}
