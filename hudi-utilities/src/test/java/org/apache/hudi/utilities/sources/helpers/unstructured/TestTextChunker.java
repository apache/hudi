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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTextChunker {

  @Test
  public void testChunkBoundariesOverlapAndEdgeCases() {
    TextChunker chunker = new TextChunker(10, 3);

    // 22 chars, step 7: chunks start at 0, 7, 14; the third reaches the end
    List<TextChunker.Chunk> chunks = chunker.chunk("abcdefghijklmnopqrstuv");
    assertEquals(3, chunks.size());
    assertEquals("abcdefghij", chunks.get(0).text);
    assertEquals(0, chunks.get(0).charStart);
    assertEquals("hijklmnopq", chunks.get(1).text);   // overlaps previous by 3
    assertEquals(7, chunks.get(1).charStart);
    assertEquals("opqrstuv", chunks.get(2).text);     // final partial chunk
    assertEquals(2, chunks.get(2).chunkId);
    assertEquals(14, chunks.get(2).charStart);

    // text shorter than one chunk -> single chunk, exact text
    List<TextChunker.Chunk> single = chunker.chunk("short");
    assertEquals(1, single.size());
    assertEquals("short", single.get(0).text);

    // empty / null -> no chunks
    assertTrue(chunker.chunk("").isEmpty());
    assertTrue(chunker.chunk(null).isEmpty());

    // invalid configs rejected
    assertThrows(IllegalArgumentException.class, () -> new TextChunker(0, 0));
    assertThrows(IllegalArgumentException.class, () -> new TextChunker(10, 10));
  }

  @Test
  public void testBreaksAtNaturalBoundariesInPreferenceOrder() {
    // paragraph break inside the window wins over the later word boundary
    TextChunker chunker = new TextChunker(30, 0);
    List<TextChunker.Chunk> paragraphs = chunker.chunk("First paragraph here.\n\nSecond paragraph runs longer than one window");
    assertEquals("First paragraph here.\n\n", paragraphs.get(0).text);
    assertEquals(23, paragraphs.get(1).charStart);

    // no paragraph/newline -> sentence end preferred over word boundary
    List<TextChunker.Chunk> sentences = chunker.chunk("A short sentence ends. The next sentence continues past the window");
    assertEquals("A short sentence ends. ", sentences.get(0).text);

    // no sentence end -> last word boundary inside the window
    List<TextChunker.Chunk> words = chunker.chunk("words without punctuation keep flowing past the window edge");
    assertTrue(words.get(0).text.endsWith(" "));
    assertTrue(words.get(0).text.length() <= 30);

    // every chunk is a verbatim substring at its recorded offset
    String text = "Mixed content.\n\nWith breaks. And words flowing on and on beyond several windows of text";
    for (TextChunker.Chunk c : chunker.chunk(text)) {
      assertEquals(text.substring(c.charStart, c.charStart + c.text.length()), c.text);
    }
  }

  @Test
  public void testBoundaryFloorGuaranteesProgressWithLargeOverlap() {
    // overlap close to chunk size: boundary breaks must still advance each chunk
    TextChunker chunker = new TextChunker(10, 8);
    String text = "a b c d e f g h i j k l m n o p q r s t";
    List<TextChunker.Chunk> chunks = chunker.chunk(text);
    for (int i = 1; i < chunks.size(); i++) {
      assertTrue(chunks.get(i).charStart > chunks.get(i - 1).charStart);
    }
    assertEquals(text.length(), chunks.get(chunks.size() - 1).charStart
        + chunks.get(chunks.size() - 1).text.length());
  }
}
