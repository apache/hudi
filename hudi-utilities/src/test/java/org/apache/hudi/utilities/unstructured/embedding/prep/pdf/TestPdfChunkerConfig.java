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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The PDF chunker's configuration and its tokenizer selection. */
class TestPdfChunkerConfig {

  private static final Tokenizer BPE = new OpenAiBpeTokenizer();

  @Test
  void defaults() {
    PdfChunkerConfig config = PdfChunkerConfig.from(new TypedProperties(), BPE);
    assertEquals(256, config.maxTokens());
    assertEquals(128, config.overlapTokens());
    assertEquals(256, config.maxWords());
    assertEquals(8192, config.windowChars());
    assertEquals(Locale.forLanguageTag("en"), config.locale());
    assertEquals(20L * 1024 * 1024 * 1024, config.maxInputBytes());
    assertEquals(10_000, config.maxPages());
    assertEquals(262_144, config.maxPageChars());
    assertEquals(262_144, config.maxPageGlyphs());
    assertEquals(33_554_432L, config.maxDecodedStreamBytes());
    assertEquals(1024, config.maxGraphicsDepth());
    assertFalse(config.aboveEnvelope());
    assertEquals("hoodie.unstructured.chunk.max.words", EmbeddingTableConfig.CHUNK_MAX_WORDS.key());
    assertEquals("hoodie.unstructured.chunk.window.max.chars", EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key());
    assertEquals("hoodie.unstructured.pdf.max.input.bytes", EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key());
    TypedProperties small = new TypedProperties();
    small.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20");
    assertEquals(10, PdfChunkerConfig.from(small, BPE).overlapTokens(), "unset overlap = min(128, max.tokens / 2)");
  }

  @Test
  public void explicitInputByteLimitIsApplied() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "21474836480");
    assertEquals(20L * 1024 * 1024 * 1024, PdfChunkerConfig.from(props, BPE).maxInputBytes());
  }

  /** A preprocessor builds its config (and logs an envelope WARN) once, not once per file; other props build a new one. */
  @Test
  void cacheBuildsOnceForTheSamePropsAndTokenizer() {
    PdfChunkerConfig.Cache cache = new PdfChunkerConfig.Cache();
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), String.valueOf(EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGES + 1));
    PdfChunkerConfig first = cache.get(props, BPE);
    assertTrue(first.aboveEnvelope());
    assertSame(first, cache.get(props, BPE), "built once for the same props and tokenizer");
    TypedProperties other = new TypedProperties();
    other.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "5");
    assertEquals(5, cache.get(other, BPE).maxPages(), "other props build their own config");
    assertNotSame(first, cache.get(props, new OpenAiBpeTokenizer()), "another tokenizer builds its own config");
    TypedProperties invalid = new TypedProperties();
    invalid.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "0");
    assertThrows(IllegalArgumentException.class, () -> cache.get(invalid, BPE));
    assertThrows(IllegalArgumentException.class, () -> cache.get(invalid, BPE), "an invalid config is refused every time");
  }

  @Test
  void unknownPdfKeysAreRejected() {
    // a misspelt or misplaced pdf.* key fails fast instead of being ignored
    TypedProperties typo = new TypedProperties();
    typo.setProperty("hoodie.unstructured.pdf.max.page", "50");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> PdfChunkerConfig.from(typo, BPE));
    assertTrue(e.getMessage().contains("hoodie.unstructured.pdf.max.page")
        && e.getMessage().contains(EmbeddingTableConfig.PDF_MAX_PAGES.key()), e.getMessage());
    // neighbours: every known pdf.* key and non-pdf keys are accepted
    TypedProperties known = new TypedProperties();
    known.setProperty(EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "1000");
    known.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "5");
    known.setProperty(EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key(), "262144");
    known.setProperty(EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "524288");
    known.setProperty(EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "1000000");
    known.setProperty(EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH.key(), "64");
    known.setProperty("hoodie.unstructured.pdfx.other", "1");
    assertEquals(5, PdfChunkerConfig.from(known, BPE).maxPages());
  }

  @Test
  void validationRejects() {
    String[][] cases = {
        {EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "15"},
        {EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "8192"},
        {EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "129"},
        {EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "-1"},
        {EmbeddingTableConfig.CHUNK_MAX_WORDS.key(), "15"},
        {EmbeddingTableConfig.CHUNK_MAX_WORDS.key(), "8192"},
        {EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key(), "255"},
        {EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key(), "65537"},
        {EmbeddingTableConfig.CHUNK_SENTENCE_LOCALE.key(), "not a tag!"},
        {EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE.key(), "true"},
        {EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "0"},
        {EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "-1"},
        {EmbeddingTableConfig.PDF_MAX_PAGES.key(), "0"},
        {EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key(), "8191"},
        {EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "262143"},
        {EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "0"},
        {EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH.key(), "15"},
    };
    for (String[] c : cases) {
      TypedProperties props = new TypedProperties();
      props.setProperty(c[0], c[1]);
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> PdfChunkerConfig.from(props, BPE),
          c[0] + "=" + c[1]);
      assertTrue(e.getMessage().contains(c[0]), e.getMessage());
    }
    TypedProperties windowBelowWords = new TypedProperties();
    windowBelowWords.setProperty(EmbeddingTableConfig.CHUNK_MAX_WORDS.key(), "300");
    windowBelowWords.setProperty(EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key(), "299");
    windowBelowWords.setProperty(EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key(), "8192");
    assertThrows(IllegalArgumentException.class, () -> PdfChunkerConfig.from(windowBelowWords, BPE));
    TypedProperties noProps = new TypedProperties();
    assertThrows(IllegalArgumentException.class, () -> PdfChunkerConfig.from(noProps, null));
  }

  @Test
  void maxTokensUpperBound() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "8191");
    assertEquals(8191, PdfChunkerConfig.from(props, BPE).maxTokens());
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16");
    assertEquals(16, PdfChunkerConfig.from(props, BPE).maxTokens());
  }

  @Test
  void aboveEnvelopeWarns() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "10001");
    assertTrue(PdfChunkerConfig.from(props, BPE).aboveEnvelope());
    // the input size is not a heap cap (the file is staged to disk; heap follows the per-page caps): any value stays inside
    TypedProperties bytes = new TypedProperties();
    bytes.setProperty(EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), String.valueOf(536_870_913L));
    PdfChunkerConfig capped = PdfChunkerConfig.from(bytes, BPE);
    assertFalse(capped.aboveEnvelope());
    assertEquals(536_870_913L, capped.maxInputBytes());
    // every per-page and per-stream cap above its benchmarked default leaves the envelope too
    String[][] above = {
        {EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key(), "262145"},
        {EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "524289"},
        {EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "33554433"},
        {EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH.key(), "1025"},
    };
    for (String[] c : above) {
      TypedProperties one = new TypedProperties();
      one.setProperty(c[0], c[1]);
      if (c[0].equals(EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key())) {
        one.setProperty(EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), c[1]);   // glyphs must stay >= chars
      }
      assertTrue(PdfChunkerConfig.from(one, BPE).aboveEnvelope(), c[0] + "=" + c[1]);
    }
    // neighbours: caps at or below their defaults stay inside
    TypedProperties lower = new TypedProperties();
    lower.setProperty(EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "262144");
    lower.setProperty(EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "1000");
    lower.setProperty(EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH.key(), "64");
    assertFalse(PdfChunkerConfig.from(lower, BPE).aboveEnvelope());
  }
}
