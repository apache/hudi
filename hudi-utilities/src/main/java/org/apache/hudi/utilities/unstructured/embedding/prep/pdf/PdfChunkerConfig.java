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

import java.util.IllformedLocaleException;
import java.util.Locale;

import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_MAX_TOKENS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_MAX_WORDS;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_SENTENCE_LOCALE;
import static org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS;

/**
 * The validated chunker configuration plus the resolved {@link Tokenizer}, built from the
 * job's properties. Invalid values throw {@link IllegalArgumentException}: a job configuration
 * error, never a file error.
 */
public final class PdfChunkerConfig {

  /** OpenAI's per-input limit is 8192; JTokkit lists 8191, the stricter value. */
  static final int MAX_TOKENS_UPPER = 8191;
  static final int MAX_TOKENS_LOWER = 16;
  static final int WINDOW_LOWER = 256;
  static final int WINDOW_UPPER = 65_536;

  private final Tokenizer tokenizer;
  private final int maxTokens;
  private final int overlapTokens;
  private final int maxWords;
  private final int windowChars;
  private final Locale locale;

  private PdfChunkerConfig(TypedProperties props, Tokenizer tokenizer) {
    this.tokenizer = tokenizer;
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
    private volatile Entry entry;

    public PdfChunkerConfig get(TypedProperties props, Tokenizer tokenizer) {
      Entry current = entry;
      if (current == null || current.props != props || current.tokenizer != tokenizer) {
        current = new Entry(props, tokenizer, from(props, tokenizer));
        entry = current;
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

  private static int intValue(TypedProperties props, ConfigProperty<?> property, int defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(raw);
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
