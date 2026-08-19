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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the hardening fixes on the unstructured ingest path: the bounded
 * chunk-boundary search preserves output. The cost half of the fix is a measurement, so
 * it lives in the scale harness rather than here.
 */
public class TestUnstructuredIngestHardening {

  /**
   * The bounded break search must pick the same break points the unbounded one did, so
   * chunk text, ordering, overlap and char_start offsets are unchanged by the fix.
   */
  @Test
  void testBoundedBreakSearchPreservesChunking() {
    int chunkSize = 64;
    int overlap = 8;
    TextChunker chunker = new TextChunker(chunkSize, overlap);
    String text = "First para line one.\nStill first para.\n\nSecond para here! And more? Yes.\n\n"
        + "Third paragraph with a long unbroken runxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx end.";

    List<TextChunker.Chunk> chunks = chunker.chunk(text);

    assertTrue(chunks.size() > 1, "expected the text to split into several chunks");
    for (int i = 0; i < chunks.size(); i++) {
      TextChunker.Chunk chunk = chunks.get(i);
      assertEquals(i, chunk.chunkId);
      assertTrue(chunk.text.length() <= chunkSize, "chunk " + i + " exceeded the window");
      // chunks are verbatim substrings, so char_start must index back into the original
      assertEquals(chunk.text,
          text.substring(chunk.charStart, chunk.charStart + chunk.text.length()));
      if (i > 0) {
        TextChunker.Chunk previous = chunks.get(i - 1);
        assertEquals(previous.charStart + previous.text.length() - overlap, chunk.charStart,
            "chunk " + i + " must start one overlap back from the previous chunk's end");
      }
    }
    TextChunker.Chunk last = chunks.get(chunks.size() - 1);
    assertEquals(text.length(), last.charStart + last.text.length(),
        "the chunks must cover the whole text");
  }

  /**
   * Text whose only usable boundary is the space character exercises the miss path for
   * five of the six boundaries on every chunk, which is where the unbounded search used to
   * walk back to index 0. Behaviour must be identical to the general case.
   */
  @Test
  void testChunkingTextWithoutParagraphOrSentenceBoundaries() {
    TextChunker chunker = new TextChunker(100, 10);
    StringBuilder builder = new StringBuilder();
    while (builder.length() < 5000) {
      builder.append("alpha bravo charlie delta echo foxtrot golf hotel india juliet ");
    }
    String text = builder.toString();

    List<TextChunker.Chunk> chunks = chunker.chunk(text);

    assertTrue(chunks.size() > 40, "expected many chunks, got " + chunks.size());
    for (TextChunker.Chunk chunk : chunks) {
      assertEquals(chunk.text,
          text.substring(chunk.charStart, chunk.charStart + chunk.text.length()));
      assertTrue(chunk.text.length() <= 100);
    }
    TextChunker.Chunk last = chunks.get(chunks.size() - 1);
    assertEquals(text.length(), last.charStart + last.text.length());
  }

}
