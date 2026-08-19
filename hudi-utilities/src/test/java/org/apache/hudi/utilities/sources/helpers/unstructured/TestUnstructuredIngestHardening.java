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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.config.UnstructuredFileSourceConfig;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the hardening fixes on the unstructured ingest path: the bounded
 * chunk-boundary search preserves output, an Error escapes the Tika parser, and the inline
 * blob threshold is validated. The cost half of the chunker fix is a measurement, so it
 * lives in the scale harness rather than here.
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

  /**
   * A corrupt file must not fail the job, but an Error must not be swallowed: catching it
   * would carry on with an undefined JVM state instead of failing the task so Spark can
   * retry it elsewhere. Before the fix the parser caught Throwable and turned an
   * OutOfMemoryError into an ordinary FAILED row.
   */
  @Test
  void testParserPropagatesErrorButRecordsException() {
    TikaDocumentParser parser = new TikaDocumentParser();

    assertThrows(OutOfMemoryError.class,
        () -> parser.parse(streamThrowing(new OutOfMemoryError("synthetic oom")), "doc.txt", 1000));

    ParseResult result =
        parser.parse(streamThrowing(new IllegalStateException("synthetic failure")), "doc.txt", 1000);
    assertEquals(ParseResult.ParseStatus.FAILED, result.getStatus());
    assertTrue(result.getError().contains("synthetic failure"),
        "the parse error must be recorded on the row, got: " + result.getError());
  }

  /**
   * readFully narrows the file size to an int, so a threshold above Integer.MAX_VALUE has
   * to be rejected at construction. Before the fix it surfaced as a
   * NegativeArraySizeException on the first oversized file, deep inside an executor.
   */
  @Test
  void testInlineThresholdAboveIntMaxIsRejected() {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredFileSourceConfig.BLOB_INLINE_MAX_BYTES.key(),
        String.valueOf(Integer.MAX_VALUE + 1L));

    IllegalArgumentException thrown =
        assertThrows(IllegalArgumentException.class, () -> new UnstructuredFileRecordBuilder(props));
    assertTrue(thrown.getMessage().contains(UnstructuredFileSourceConfig.BLOB_INLINE_MAX_BYTES.key()),
        "the error must name the offending config key, got: " + thrown.getMessage());
  }

  /**
   * The probe is what tells a user their table will have no text at all. hudi-utilities has
   * the Tika parser modules on its test classpath, so here it must report that plain text
   * extracts; a deployment running the shipped bundle alone gets false and the warning.
   */
  @Test
  void testPlainTextProbeDetectsAvailableParserModules() {
    assertTrue(TikaDocumentParser.canExtractPlainText(),
        "tika-parsers-standard-package is on the test classpath, so the probe must find text");

    ParseResult parsed = new TikaDocumentParser().parse(
        new ByteArrayInputStream("hudi lakehouse probe text".getBytes(StandardCharsets.UTF_8)),
        "probe.txt", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, parsed.getStatus());
    assertTrue(parsed.getText().contains("lakehouse"));
  }

  private static InputStream streamThrowing(Throwable failure) {
    return new InputStream() {
      @Override
      public int read() {
        if (failure instanceof Error) {
          throw (Error) failure;
        }
        throw (RuntimeException) failure;
      }
    };
  }
}
