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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.IllformedLocaleException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_MAX_TOKENS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_MAX_WORDS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_SENTENCE_LOCALE;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_INPUT_BYTES;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_PAGES;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_PAGE_CHARS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_PAGE_FONT_BYTES;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.PDF_PREFIX;

/**
 * The validated chunker configuration plus the resolved {@link Tokenizer}, built from the
 * job's properties. Invalid values throw {@link IllegalArgumentException}: a job configuration
 * error, never a file error.
 */
public final class PdfChunkerConfig {

  private static final Logger LOG = LoggerFactory.getLogger(PdfChunkerConfig.class);
  /** OpenAI's per-input limit is 8192; JTokkit lists 8191, the stricter value. */
  static final int MAX_TOKENS_UPPER = 8191;
  static final int MAX_TOKENS_LOWER = 16;
  static final int WINDOW_LOWER = 256;
  static final int WINDOW_UPPER = 65_536;
  static final int MAX_GRAPHICS_DEPTH_LOWER = 16;
  /** Every {@code hoodie.unstructured.pdf.*} key; any other key under that prefix is a configuration error. */
  static final List<String> PDF_KEYS = Arrays.asList(PDF_MAX_INPUT_BYTES.key(), PDF_MAX_PAGES.key(), PDF_MAX_PAGE_CHARS.key(),
      PDF_MAX_PAGE_GLYPHS.key(), PDF_MAX_DECODED_STREAM_BYTES.key(), PDF_MAX_PAGE_FONT_BYTES.key(), PDF_MAX_GRAPHICS_DEPTH.key());

  private final Tokenizer tokenizer;
  private final int maxTokens;
  private final int overlapTokens;
  private final int maxWords;
  private final int windowChars;
  private final Locale locale;
  private final long maxInputBytes;
  private final boolean inputBytesConfigured;
  private final int maxPages;
  private final int maxPageChars;
  private final int maxPageGlyphs;
  private final long maxDecodedStreamBytes;
  private final long maxPageFontBytes;
  private final int maxGraphicsDepth;
  private final boolean aboveEnvelope;

  private PdfChunkerConfig(TypedProperties props, Tokenizer tokenizer) {
    this.tokenizer = tokenizer;
    rejectUnknownPdfKeys(props);
    EmbeddingTableConfig table = EmbeddingTableConfig.from(props);
    this.maxTokens = range(CHUNK_MAX_TOKENS.key(), table.chunkMaxTokens(), MAX_TOKENS_LOWER, MAX_TOKENS_UPPER);
    this.overlapTokens = table.chunkOverlapTokens();
    this.maxWords = range(CHUNK_MAX_WORDS.key(), intValue(props, CHUNK_MAX_WORDS, EmbeddingTableConfig.DEFAULT_CHUNK_MAX_WORDS),
        MAX_TOKENS_LOWER, MAX_TOKENS_UPPER);
    this.windowChars = range(CHUNK_WINDOW_MAX_CHARS.key(),
        intValue(props, CHUNK_WINDOW_MAX_CHARS, EmbeddingTableConfig.DEFAULT_CHUNK_WINDOW_MAX_CHARS), WINDOW_LOWER, WINDOW_UPPER);
    if (windowChars < maxWords) {
      throw invalid(CHUNK_WINDOW_MAX_CHARS.key(), windowChars, ">= " + CHUNK_MAX_WORDS.key() + " (" + maxWords + ")");
    }
    this.locale = locale(ConfigUtils.getStringWithAltKeys(props, CHUNK_SENTENCE_LOCALE, EmbeddingTableConfig.DEFAULT_CHUNK_SENTENCE_LOCALE));
    String dehyphenate = ConfigUtils.getStringWithAltKeys(props, CHUNK_NORMALIZE_DEHYPHENATE, "false").trim();
    if (!dehyphenate.equalsIgnoreCase("false")) {
      throw new IllegalArgumentException("Config " + CHUNK_NORMALIZE_DEHYPHENATE.key() + "=" + dehyphenate
          + ": only false is supported");
    }
    String inputCap = ConfigUtils.getStringWithAltKeys(props, PDF_MAX_INPUT_BYTES, null);
    this.inputBytesConfigured = inputCap != null && !inputCap.trim().isEmpty();
    if (!inputBytesConfigured) {
      this.maxInputBytes = EmbeddingTableConfig.DEFAULT_PDF_MAX_INPUT_BYTES;
    } else {
      long cap = longValue(props, PDF_MAX_INPUT_BYTES, 0);
      if (cap <= 0) {
        throw invalid(PDF_MAX_INPUT_BYTES.key(), cap, "> 0 (a byte count)");
      }
      this.maxInputBytes = cap;
    }
    this.maxPages = (int) positive(PDF_MAX_PAGES.key(), intValue(props, PDF_MAX_PAGES, EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGES));
    this.maxPageChars = intValue(props, PDF_MAX_PAGE_CHARS, EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_CHARS);
    if (maxPageChars < windowChars) {
      throw invalid(PDF_MAX_PAGE_CHARS.key(), maxPageChars, ">= " + CHUNK_WINDOW_MAX_CHARS.key() + " (" + windowChars + ")");
    }
    this.maxPageGlyphs = intValue(props, PDF_MAX_PAGE_GLYPHS, EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_GLYPHS);
    if (maxPageGlyphs < maxPageChars) {
      throw invalid(PDF_MAX_PAGE_GLYPHS.key(), maxPageGlyphs, ">= " + PDF_MAX_PAGE_CHARS.key() + " (" + maxPageChars + ")");
    }
    this.maxDecodedStreamBytes = positive(PDF_MAX_DECODED_STREAM_BYTES.key(),
        longValue(props, PDF_MAX_DECODED_STREAM_BYTES, EmbeddingTableConfig.DEFAULT_PDF_MAX_DECODED_STREAM_BYTES));
    this.maxPageFontBytes = positive(PDF_MAX_PAGE_FONT_BYTES.key(),
        longValue(props, PDF_MAX_PAGE_FONT_BYTES, EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_FONT_BYTES));
    this.maxGraphicsDepth = intValue(props, PDF_MAX_GRAPHICS_DEPTH, EmbeddingTableConfig.DEFAULT_PDF_MAX_GRAPHICS_DEPTH);
    if (maxGraphicsDepth < MAX_GRAPHICS_DEPTH_LOWER) {
      throw invalid(PDF_MAX_GRAPHICS_DEPTH.key(), maxGraphicsDepth, ">= " + MAX_GRAPHICS_DEPTH_LOWER);
    }
    // every memory cap above its benchmarked default leaves the envelope; the input size is not one (the file is staged to
    // local disk and PDFBox reads it from there, so it bounds disk, not heap)
    this.aboveEnvelope = maxPages > EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGES
        || maxPageChars > EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_CHARS
        || maxPageGlyphs > EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_GLYPHS
        || maxDecodedStreamBytes > EmbeddingTableConfig.DEFAULT_PDF_MAX_DECODED_STREAM_BYTES
        || maxPageFontBytes > EmbeddingTableConfig.DEFAULT_PDF_MAX_PAGE_FONT_BYTES
        || maxGraphicsDepth > EmbeddingTableConfig.DEFAULT_PDF_MAX_GRAPHICS_DEPTH;
    if (aboveEnvelope) {
      LOG.warn("PDF caps above the benchmarked envelope ({}={}, {}={}, {}={}, {}={}, {}={}, {}={}): memory is not qualified "
          + "for such files", PDF_MAX_PAGES.key(), maxPages,
          PDF_MAX_PAGE_CHARS.key(), maxPageChars, PDF_MAX_PAGE_GLYPHS.key(), maxPageGlyphs, PDF_MAX_DECODED_STREAM_BYTES.key(),
          maxDecodedStreamBytes, PDF_MAX_PAGE_FONT_BYTES.key(), maxPageFontBytes, PDF_MAX_GRAPHICS_DEPTH.key(), maxGraphicsDepth);
    }
  }

  /**
   * @throws IllegalArgumentException for any invalid value
   */
  public static PdfChunkerConfig from(TypedProperties props, Tokenizer tokenizer) {
    if (tokenizer == null) {
      throw new IllegalArgumentException("tokenizer is required");
    }
    return new PdfChunkerConfig(props, tokenizer);
  }

  /** Holds the config, built once per (props, tokenizer) identity. */
  public static final class Cache {
    private final AtomicReference<Entry> entry = new AtomicReference<>();

    public PdfChunkerConfig get(TypedProperties props, Tokenizer tokenizer) {
      Entry current = entry.get();
      if (current == null || current.props != props || current.tokenizer != tokenizer) {
        current = new Entry(props, tokenizer, from(props, tokenizer));
        entry.set(current);
      }
      return current.config;
    }

    private static final class Entry {
      private final TypedProperties props;
      private final Tokenizer tokenizer;
      private final PdfChunkerConfig config;

      private Entry(TypedProperties props, Tokenizer tokenizer, PdfChunkerConfig config) {
        this.props = props;
        this.tokenizer = tokenizer;
        this.config = config;
      }
    }
  }

  public Tokenizer tokenizer() {
    return tokenizer;
  }

  public int maxTokens() {
    return maxTokens;
  }

  public int overlapTokens() {
    return overlapTokens;
  }

  public int maxWords() {
    return maxWords;
  }

  public int windowChars() {
    return windowChars;
  }

  public Locale locale() {
    return locale;
  }

  /**
   * Whether the job sets {@link EmbeddingTableConfig#PDF_MAX_INPUT_BYTES}: only then is PDFBox's scratch storage held to the cap
   * too (its pages are not a byte count of the input, so the default cap bounds only the staged file).
   */
  public boolean inputBytesConfigured() {
    return inputBytesConfigured;
  }

  /** The input size cap in bytes ({@link EmbeddingTableConfig#PDF_MAX_INPUT_BYTES}, its default when unset). */
  public long maxInputBytes() {
    return maxInputBytes;
  }

  public int maxPages() {
    return maxPages;
  }

  public int maxPageChars() {
    return maxPageChars;
  }

  public int maxPageGlyphs() {
    return maxPageGlyphs;
  }

  public long maxDecodedStreamBytes() {
    return maxDecodedStreamBytes;
  }

  /** Aggregate decoded font bytes charged per font dictionary on one page. */
  public long maxPageFontBytes() {
    return maxPageFontBytes;
  }

  public int maxGraphicsDepth() {
    return maxGraphicsDepth;
  }

  /** Whether any {@code pdf.max.*} cap exceeds its benchmarked default (a WARN was logged). */
  public boolean aboveEnvelope() {
    return aboveEnvelope;
  }

  /**
   * Refuses unknown PDF keys that extraction would otherwise ignore. A change to a valid preprocessing setting is not detected;
   * use a new table for a different preprocessing style.
   */
  private static void rejectUnknownPdfKeys(TypedProperties props) {
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith(PDF_PREFIX) && !PDF_KEYS.contains(key)) {
        throw new IllegalArgumentException("Unknown config " + key + " (known " + PDF_PREFIX + "* keys: "
            + String.join(", ", PDF_KEYS) + ")");
      }
    }
  }

  private static int intValue(TypedProperties props, ConfigProperty<?> property, int defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + property.key() + " must be an integer, got '" + raw + "'", e);
    }
  }

  private static long longValue(TypedProperties props, ConfigProperty<?> property, long defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(defaultValue));
    try {
      return Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + property.key() + " must be an integer, got '" + raw + "'", e);
    }
  }

  private static int range(String key, int value, int lower, int upper) {
    if (value < lower || value > upper) {
      throw invalid(key, value, "in " + lower + ".." + upper);
    }
    return value;
  }

  private static long positive(String key, long value) {
    if (value <= 0) {
      throw invalid(key, value, "> 0");
    }
    return value;
  }

  private static Locale locale(String tag) {
    try {
      Locale locale = new Locale.Builder().setLanguageTag(tag.trim()).build();
      if (locale.getLanguage().isEmpty()) {
        throw new IllegalArgumentException("Config " + CHUNK_SENTENCE_LOCALE.key() + "='" + tag + "' names no language");
      }
      return locale;
    } catch (IllformedLocaleException e) {
      throw new IllegalArgumentException("Config " + CHUNK_SENTENCE_LOCALE.key() + "='" + tag + "' is not a language tag", e);
    }
  }

  private static IllegalArgumentException invalid(String key, long value, String rule) {
    return new IllegalArgumentException("Config " + key + " must be " + rule + " but is " + value);
  }
}
