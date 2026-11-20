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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.unstructured.config.NumericConfig;
import org.apache.hudi.utilities.unstructured.embedding.balance.SizeTypeAwareBalancingStrategy;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientNames;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageDecoder;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageDecoderLogicMismatchException;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreprocessingSpec;
import org.apache.hudi.utilities.unstructured.embedding.image.InProcessImageDecoder;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Configuration of embedding table preparation, chunking and computation.
 */
@ConfigClassProperty(name = "Embedding Table Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling embedding table preparation, chunking and embedding computation.")
public class EmbeddingTableConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final String SINCE_1_3_0 = "1.3.0";

  private static final String REQUIRED = "<required>";

  /** Root of every unstructured-data key (embedding and chunking); never passed through to the Hudi writer. */
  public static final String UNSTRUCTURED_PREFIX = "hoodie.unstructured.";

  /** Prefix of every embedding-table key. */
  public static final String PREFIX = UNSTRUCTURED_PREFIX + "embedding.";

  /** Prefix of every chunking key (chunk size limits, tokenizer). */
  public static final String CHUNK_PREFIX = UNSTRUCTURED_PREFIX + "chunk.";

  /**
   * Comma-separated aliases of the declared embedding models, each owning a nullable VECTOR column; default empty. Only for
   * {@value #MODE_PREP_AND_COMPUTE}: a {@value #MODE_PREP_ONLY} table declares no model.
   */
  public static final ConfigProperty<String> MODELS = ConfigProperty
      .key(PREFIX + "models")
      .defaultValue("")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Comma-separated aliases of the declared embedding models, each owning a nullable VECTOR column. Only for "
          + "prep_and_compute: a prep_only table declares no model. Each model's rate.limit.requests.per.second is per compute "
          + "task, so the job's aggregate request rate scales with its active compute tasks.");

  /** Comma-separated declared models the compute flow fills; empty means every declared model. Only for prep_and_compute. */
  public static final ConfigProperty<String> ACTIVE_MODELS = ConfigProperty
      .key(PREFIX + "active.models")
      .defaultValue("")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Comma-separated declared models the compute flow fills; empty means every declared model. Only for "
          + "prep_and_compute. A name not declared in " + PREFIX + "models is refused.");

  /** Prefix of the per-model keys {@code hoodie.unstructured.embedding.model.<alias>.*}. */
  public static final String MODEL_PREFIX = PREFIX + "model.";

  /** Per-model VECTOR column name; default {@code emb_<alias>}. */
  public static final String MODEL_COLUMN_SUFFIX = ".column";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.column family. */
  public static final ConfigProperty<String> MODEL_COLUMN_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.column")
      .noDefaultValue("emb_<alias>")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Name of the model's VECTOR column. Replace <alias> with the declared model alias.");

  /** Per-model vector dimensions (VECTOR(n)), must be positive; required per model. */
  public static final String MODEL_DIMENSIONS_SUFFIX = ".dimensions";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.dimensions family. */
  public static final ConfigProperty<String> MODEL_DIMENSIONS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.dimensions")
      .noDefaultValue(REQUIRED)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Dimensions n of the model's VECTOR(n) column, a positive integer; required for every declared model. "
          + "Replace <alias> with the declared model alias.");

  /** Per-model EmbeddingClient implementation; when unset the client of the model's provider (EmbeddingClientNames). */
  public static final String MODEL_CLIENT_CLASS_SUFFIX = ".client.class";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.client.class family. */
  public static final ConfigProperty<String> MODEL_CLIENT_CLASS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.client.class")
      .noDefaultValue("<unset>")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Embedding client implementation of the model; when unset, the built-in client of the model's provider. "
          + "Replace <alias> with the declared model alias.");

  /** Per-model embedding provider, one of {@link EmbeddingClientNames#PROVIDERS}; required per model, no default. */
  public static final String MODEL_PROVIDER_SUFFIX = ".provider";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.provider family. */
  public static final ConfigProperty<String> MODEL_PROVIDER_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.provider")
      .noDefaultValue(REQUIRED)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Embedding provider of the model; required for every declared model. The built-in provider is dummy, "
          + "which writes deterministic random unit vectors and is meant for tests and pipeline checks only. Replace <alias> "
          + "with the declared model alias.");

  /** Per-model provider-side model name; default the alias. */
  public static final String MODEL_NAME_SUFFIX = ".name";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.name family. */
  public static final ConfigProperty<String> MODEL_NAME_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.name")
      .noDefaultValue("<alias>")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Provider-side name of the model. Replace <alias> with the declared model alias.");

  /**
   * Per-model declared modalities: a non-empty subset of {@code text} and {@code image}, comma-separated, in any letter case and
   * order; required for the active model under {@code prep_and_compute}. Compute sends the model only rows of a declared
   * modality; the job refuses at start a declaration the model's client has no request format for, or one beyond what a known
   * model can embed ({@code ModelModalityChecks}, {@code KnownEmbeddingModels}).
   */
  public static final String MODEL_MODALITIES_SUFFIX = ".modalities";

  /** Metadata for the hoodie.unstructured.embedding.model.{@code <alias>}.modalities family. */
  public static final ConfigProperty<String> MODEL_MODALITIES_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.model.<alias>.modalities")
      .noDefaultValue(REQUIRED)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The modalities the model embeds: text, image, or text,image (comma-separated, any letter case and "
          + "order); required for the active model under prep_and_compute. Only rows of a declared modality are sent to the "
          + "model; a row of another modality gets a NULL vector and an EMBED-COMPUTE-<MODALITY>-NOT-SUPPORTED error record "
          + "when the error table is on, and fails the round when it is off. The job refuses at start a modality the model's "
          + "provider has no request format for, a modality beyond what a known model can embed, and, with the error table "
          + "off, prepared content (hoodie.unstructured.embedding.prep.file.extensions) of a modality the model does not "
          + "declare. A changed value applies from the next batch; committed rows are not embedded again. Replace <alias> "
          + "with the declared model alias.");

  /** Per-chunk token limit, must be positive. */
  public static final ConfigProperty<Integer> CHUNK_MAX_TOKENS = ConfigProperty
      .key(CHUNK_PREFIX + "max.tokens")
      .defaultValue(256)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Per-chunk token limit, a positive integer.");

  /** Default per-chunk token limit. */
  public static final int DEFAULT_CHUNK_MAX_TOKENS = CHUNK_MAX_TOKENS.defaultValue();

  /** Tokens two adjacent chunks may share, {@code 0 <= overlap <= max.tokens / 2}; default {@code min(128, max.tokens / 2)}. */
  public static final ConfigProperty<String> CHUNK_OVERLAP_TOKENS = ConfigProperty
      .key(CHUNK_PREFIX + "overlap.tokens")
      .noDefaultValue("min(128, chunkMaxTokens/2)")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Tokens two adjacent chunks may share, from 0 up to hoodie.unstructured.chunk.max.tokens / 2.");

  /** Default overlap cap: at most 128 tokens of overlap between 2 adjacent chunks. */
  public static final int DEFAULT_CHUNK_OVERLAP_TOKENS = 128;

  /** Tokenizer implementation (public no-arg constructor); when set it wins over {@link #TOKENIZER_TYPE}; default unset. */
  public static final ConfigProperty<String> TOKENIZER_CLASS = ConfigProperty
      .key(CHUNK_PREFIX + "tokenizer.class")
      .noDefaultValue("<unset>")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Tokenizer implementation (public no-arg constructor); when set it wins over "
          + "hoodie.unstructured.chunk.tokenizer.type.");

  /** Built-in tokenizer type ({@code word-count} or {@code openai-bpe}); default {@code openai-bpe} ({@code text.TokenizerFactory}). */
  public static final ConfigProperty<String> TOKENIZER_TYPE = ConfigProperty
      .key(CHUNK_PREFIX + "tokenizer.type")
      .defaultValue("openai-bpe")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Built-in tokenizer type: word-count or openai-bpe.");

  /**
   * jtokkit encoding of the {@code openai-bpe} tokenizer type (e.g. {@code o200k_base}); default {@code cl100k_base}, the
   * encoding of OpenAI's text-embedding-3-small/-large and ada-002 models ({@code text.OpenAiBpeTokenizer}). Part of the
   * tokenizer id ({@code openai-bpe:<encoding>}).
   */
  public static final ConfigProperty<String> TOKENIZER_ENCODING = ConfigProperty
      .key(CHUNK_PREFIX + "tokenizer.encoding")
      .defaultValue("cl100k_base")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("jtokkit encoding of the openai-bpe tokenizer type (e.g. o200k_base). The default, cl100k_base, is the "
          + "encoding of OpenAI's text-embedding-3-small/-large and ada-002 models. Part of the tokenizer id "
          + "(openai-bpe:<encoding>).");

  /** Words per chunk, the hard word cap, {@code 16..8191}. */
  public static final ConfigProperty<Integer> CHUNK_MAX_WORDS = ConfigProperty
      .key(CHUNK_PREFIX + "max.words")
      .defaultValue(256)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Words per chunk, the hard word cap, 16..8191.");

  public static final int DEFAULT_CHUNK_MAX_WORDS = CHUNK_MAX_WORDS.defaultValue();

  /**
   * The most text ever handed to the tokenizer and the per-chunk char cap (UTF-16 units), {@code 256..65536} and at least
   * {@link #CHUNK_MAX_WORDS}.
   */
  public static final ConfigProperty<Integer> CHUNK_WINDOW_MAX_CHARS = ConfigProperty
      .key(CHUNK_PREFIX + "window.max.chars")
      .defaultValue(8192)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The most text ever handed to the tokenizer and the per-chunk char cap (UTF-16 units), 256..65536 and at "
          + "least hoodie.unstructured.chunk.max.words.");

  public static final int DEFAULT_CHUNK_WINDOW_MAX_CHARS = CHUNK_WINDOW_MAX_CHARS.defaultValue();

  /** Language tag of the sentence-boundary rules ({@code java.text.BreakIterator}). */
  public static final ConfigProperty<String> CHUNK_SENTENCE_LOCALE = ConfigProperty
      .key(CHUNK_PREFIX + "sentence.locale")
      .defaultValue("en")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Language tag of the sentence-boundary rules (java.text.BreakIterator).");

  public static final String DEFAULT_CHUNK_SENTENCE_LOCALE = CHUNK_SENTENCE_LOCALE.defaultValue();

  /** De-hyphenation of {@code -\n} line breaks; only {@code false} is supported. */
  public static final ConfigProperty<Boolean> CHUNK_NORMALIZE_DEHYPHENATE = ConfigProperty
      .key(CHUNK_PREFIX + "normalize.dehyphenate")
      .defaultValue(false)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("De-hyphenation of -\\n line breaks; only false is supported.");

  /**
   * The input size caps' note on raising a cap on an existing table: prep reads a file again only when its source row changes,
   * so a file a cap refused keeps its refusal, and the table's content for that file depends on when it arrived.
   */
  private static final String RAISED_CAP = " Raising it does not re-prepare a file it already refused: that file keeps its refusal "
      + "(a FAILED row or a quarantine entry) until its source object changes, while a new file of the same size is prepared.";

  /** Prefix of the PDF extraction keys; every set one is part of the style, except the input size cap. */
  public static final String PDF_PREFIX = UNSTRUCTURED_PREFIX + "pdf.";

  /**
   * Largest PDF chunked (bytes): a larger one is a FAILED {@code INPUT_LIMIT} row, refused before any read when its size is
   * declared. Default {@value #DEFAULT_PDF_MAX_INPUT_BYTES} (20 GiB). A set value must be a positive byte count; 0 or a negative
   * value is refused when the job starts. The file is staged to local disk whole and PDFBox reads it from there, so the cap bounds disk, not heap
   * (heap follows the per-page and per-stream caps). When the key is set explicitly (to any value, the default included), PDFBox's
   * scratch storage is also held to this many bytes; when it is unset, that scratch storage is not bounded. Not pinned: it only
   * refuses a file, so it can be changed on an existing table; raising it does not re-prepare a file it already refused, whose
   * {@code INPUT_LIMIT} row stays until its source object changes.
   */
  public static final long DEFAULT_PDF_MAX_INPUT_BYTES = 20L * 1024 * 1024 * 1024;

  public static final ConfigProperty<String> PDF_MAX_INPUT_BYTES = ConfigProperty
      .key(PDF_PREFIX + "max.input.bytes")
      .defaultValue(Long.toString(DEFAULT_PDF_MAX_INPUT_BYTES))
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Largest PDF chunked (bytes); a larger one is a FAILED INPUT_LIMIT row, refused before any read when its "
          + "size is declared. Default 21474836480 (20 GiB). A set value must be a positive byte count (0 or a negative value is "
          + "refused at job start). The file is staged whole to local disk, so the cap bounds disk use, not heap. When set "
          + "explicitly (to any value, the default included), PDFBox's scratch storage is also held to this many bytes; when "
          + "unset, it is not. Not part of the table's stored prep contract: it only refuses a file, so it can be changed on an "
          + "existing table." + RAISED_CAP);

  /**
   * Pages read per PDF; the rest is dropped with a {@code PAGE_LIMIT} warning, and a file without text in the pages read
   * fails with {@code PAGE_LIMIT}; default 10 000.
   */
  public static final ConfigProperty<Integer> PDF_MAX_PAGES = ConfigProperty
      .key(PDF_PREFIX + "max.pages")
      .defaultValue(10000)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Pages read per PDF; the rest is dropped with a PAGE_LIMIT warning, and a file without text in the pages "
          + "read fails with PAGE_LIMIT; default 10 000.");

  public static final int DEFAULT_PDF_MAX_PAGES = 10_000;

  /** Chars kept per page, raw and after NFKC; at least the window; default 262 144. */
  public static final ConfigProperty<Integer> PDF_MAX_PAGE_CHARS = ConfigProperty
      .key(PDF_PREFIX + "max.page.chars")
      .defaultValue(262144)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Chars kept per page, raw and after NFKC; at least the window; default 262 144.");

  public static final int DEFAULT_PDF_MAX_PAGE_CHARS = 262_144;

  /**
   * Glyphs per PDF page before the page fails {@code PAGE_GLYPH_LIMIT}; at least max.page.chars; default 262 144.
   * Size extraction live heap as floor + k * (256 * glyphs + max.page.font.bytes + max.decoded.stream.bytes), in bytes,
   * then add collector/allocation headroom. Here k is concurrent PDF tasks in one executor, and floor is measured non-page
   * job state. The 256 B/glyph allowance rounds up the measured 201-205 B/glyph graph; it is 64 MiB/task at the default.
   * Glyphs and fonts can coexist on the SAME page: their terms add. Pages beyond the one-page TrueType k=8 combined reference
   * (other font programs, k>8, large files) remain unqualified.
   * This glyph term belongs only to PDF extraction; vanilla structured rows have no PDFBox page graph. Ordinary Spark and
   * writer memory still need their own sizing. Raising this cap leaves the qualified envelope; the cap is not heap admission.
   */
  public static final ConfigProperty<Integer> PDF_MAX_PAGE_GLYPHS = ConfigProperty
      .key(PDF_PREFIX + "max.page.glyphs")
      .defaultValue(262144)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Glyphs per PDF page before the page fails PAGE_GLYPH_LIMIT; at least max.page.chars; default 262 144. Size "
          + "extraction live heap as floor + k * (256 * glyphs + max.page.font.bytes + max.decoded.stream.bytes), in "
          + "bytes, then add collector/allocation headroom. Here k is concurrent PDF tasks in one executor, and floor "
          + "is measured non-page job state. The 256 B/glyph allowance rounds up the measured 201-205 B/glyph graph; it "
          + "is 64 MiB/task at the default. Glyphs and fonts can coexist on the SAME page: their terms add. Pages "
          + "beyond the one-page TrueType k=8 combined reference (other font programs, k>8, large files) remain "
          + "unqualified. This glyph term belongs only to PDF extraction; vanilla structured rows have no PDFBox page "
          + "graph. Ordinary Spark and writer memory still need their own sizing. Raising this cap leaves the qualified "
          + "envelope; the cap is not heap admission.");

  public static final int DEFAULT_PDF_MAX_PAGE_GLYPHS = 262_144;

  /**
   * Decoded bytes per pre-checked stream, and of a page's filtered /Contents streams together (a repeated stream counted
   * per listing), before the page fails {@code DECODE_LIMIT}; default 32 MiB.
   */
  public static final ConfigProperty<Long> PDF_MAX_DECODED_STREAM_BYTES = ConfigProperty
      .key(PDF_PREFIX + "max.decoded.stream.bytes")
      .defaultValue(33554432L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Decoded bytes per pre-checked stream, and of a page's filtered /Contents streams together "
          + "(a repeated stream counted per listing), before the page fails DECODE_LIMIT; default 32 MiB.");

  public static final long DEFAULT_PDF_MAX_DECODED_STREAM_BYTES = 32L * 1024 * 1024;

  /**
   * Static per-page decoded font bytes, charged per dictionary before DECODE_LIMIT; default 48 MiB admits one 31 MiB font.
   * Size this term per concurrent PDF task together with the glyph graph and one decoded stream (see {@link #PDF_MAX_PAGE_GLYPHS});
   * at the defaults the font allowance is 48 MiB + 32 MiB = 80 MiB/task, in addition to glyphs and the executor's job floor.
   * Repeated per-use font materialization is outside this static bound. The default is not corpus-calibrated; raising it leaves the qualified envelope.
   */
  public static final ConfigProperty<Long> PDF_MAX_PAGE_FONT_BYTES = ConfigProperty
      .key(PDF_PREFIX + "max.page.font.bytes")
      .defaultValue(50331648L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Static per-page decoded font bytes, charged per dictionary before DECODE_LIMIT; default 48 MiB admits one "
          + "31 MiB font. Size this term per concurrent PDF task together with the glyph graph and one decoded stream "
          + "(see hoodie.unstructured.pdf.max.page.glyphs); at the defaults the font allowance is 48 MiB + 32 MiB = 80 "
          + "MiB/task, in addition to glyphs and the executor's job floor. Repeated per-use font materialization is "
          + "outside this static bound. The default is not corpus-calibrated; raising it leaves the qualified envelope.");

  public static final long DEFAULT_PDF_MAX_PAGE_FONT_BYTES = 48L * 1024 * 1024;

  /**
   * Graphics-state and marked-content depth of a page, the states of the forms it draws included, before the page fails
   * {@code GRAPHICS_DEPTH_LIMIT}; at least 16; default 1 024.
   */
  public static final ConfigProperty<Integer> PDF_MAX_GRAPHICS_DEPTH = ConfigProperty
      .key(PDF_PREFIX + "max.graphics.depth")
      .defaultValue(1024)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Graphics-state and marked-content depth of a page, the states of the forms it draws included, before the "
          + "page fails GRAPHICS_DEPTH_LIMIT; at least 16; default 1 024.");

  public static final int DEFAULT_PDF_MAX_GRAPHICS_DEPTH = 1024;

  /** Prefix of every data-prep key. */
  public static final String PREP_PREFIX = PREFIX + "prep.";

  /** Prefix of {@code <prefix><ext>.class}, the FilePreprocessor for one extension; defaults in PreprocessorRegistry. */
  public static final String PREPROCESSOR_CLASS_PREFIX = PREP_PREFIX + "preprocessor.";

  /** Metadata for the hoodie.unstructured.embedding.prep.preprocessor.{@code <ext>}.class family. */
  public static final ConfigProperty<String> PREPROCESSOR_CLASS_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.prep.preprocessor.<ext>.class")
      .noDefaultValue("<registry-dependent>")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The FilePreprocessor class for files with extension <ext>; when unset, the built-in preprocessor of "
          + "that extension (pdf has one; png, jpg, jpeg, gif, bmp, tif and tiff map to ImageInputPreprocessor; mp4, mov, mkv, "
          + "webm and avi to the class of hoodie.unstructured.embedding.prep.video.preprocessor.class, VideoFramePreprocessor "
          + "(FFmpeg) by default; other extensions have none). Replace <ext> with a file extension.");

  /** Suffix of the per-extension preprocessor class key. */
  public static final String PREPROCESSOR_CLASS_SUFFIX = ".class";

  /**
   * Image data prep: images whose header declares more pixels fail data prep (a quarantine entry) before any compute; default
   * {@link #DEFAULT_PREP_IMAGE_MAX_PIXELS}. Must be positive. Part of the preprocessing style when set.
   */
  public static final ConfigProperty<Long> PREP_IMAGE_MAX_PIXELS = ConfigProperty
      .key(PREP_PREFIX + "image.max.pixels")
      .defaultValue(100000000L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Image data prep: images whose header declares more pixels fail data prep (a quarantine entry) before any "
          + "compute; default 100000000. Must be positive. Part of the preprocessing style when set.");

  public static final long DEFAULT_PREP_IMAGE_MAX_PIXELS = 100_000_000L;

  /**
   * Image data prep: the most bytes read to find an image's header; an image whose header needs more fails data prep (a
   * quarantine entry). Over a source row (random access) it counts the distinct bytes read wherever they lie in the file (a byte
   * the reader reads again counts once); over a plain stream it is the offset the memory-cached read may reach. Default {@link #DEFAULT_PREP_IMAGE_HEADER_MAX_BYTES} (1 MiB). Must be
   * positive. Part of the preprocessing style when set.
   */
  public static final ConfigProperty<Long> PREP_IMAGE_HEADER_MAX_BYTES = ConfigProperty
      .key(PREP_PREFIX + "image.header.max.bytes")
      .defaultValue(1048576L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Image data prep: the most bytes read to find an image's header; an image whose header needs more fails data "
          + "prep (a quarantine entry). Over a source row (random access) it counts the distinct bytes read wherever they lie in "
          + "the file (a byte the reader reads again counts once); over a plain stream it is the offset the memory-cached read may reach. Default 1048576 (1 MiB). Must be "
          + "positive. Part of the preprocessing style when set.");

  public static final long DEFAULT_PREP_IMAGE_HEADER_MAX_BYTES = 1024L * 1024;

  /**
   * Image data prep: the largest image, in source bytes ({@code file_size}), that prep stores as a model input; a larger one
   * fails data prep (a quarantine entry) before any byte of it is read. An operator's limit, not a memory bound: the row's
   * {@code prepared_image} is the source row's own blob value (its inline bytes, or its reference to the object), never a copy
   * prep reads, so the default {@link #DEFAULT_PREP_IMAGE_MAX_BYTES} sets no limit. Must be positive. Not pinned: it only refuses
   * a file, so it can be changed on an existing table; raising it does not re-prepare a file it already refused, whose
   * quarantine entry stays until its source object changes.
   */
  public static final ConfigProperty<Long> PREP_IMAGE_MAX_BYTES = ConfigProperty
      .key(PREP_PREFIX + "image.max.bytes")
      .defaultValue(9223372036854775807L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Image data prep: the largest image, in source bytes (file_size), that prep stores as a model input; a "
          + "larger one fails data prep (a quarantine entry) before any byte of it is read. An operator's limit, not a "
          + "memory bound: the row's prepared_image is the source row's own blob value (its inline bytes, or its reference "
          + "to the object), never a copy prep reads, so the default 9223372036854775807 sets no limit. Must be positive. "
          + "Not part of the table's stored prep contract: it only refuses a file, so it can be changed on an existing table."
          + RAISED_CAP);

  public static final long DEFAULT_PREP_IMAGE_MAX_BYTES = Long.MAX_VALUE;

  /**
   * The preprocessor of every video extension ({@link org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry#VIDEO_EXTENSIONS}),
   * so a job prepares all its video with one implementation, read once at start. Default
   * {@link #DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS}, the FFmpeg frame preprocessor. Hudi does not ship FFmpeg (org.bytedeco): it is
   * loaded only when the job's first video is opened, so a job without video runs without it, and a job that prepares video adds
   * the FFmpeg jars to its class path (org.bytedeco javacv, javacpp and ffmpeg plus the javacpp and ffmpeg jars of the platform
   * classifier, for example with {@code spark.jars}). The per-extension
   * {@code <PREPROCESSOR_CLASS_PREFIX><ext>.class} key is a low-level override that wins for its extension when set. A job that
   * must not prepare video sets {@link #PREP_FILE_EXTENSIONS} to the non-video extensions it prepares.
   */
  public static final ConfigProperty<String> PREP_VIDEO_PREPROCESSOR_CLASS = ConfigProperty
      .key(PREP_PREFIX + "video.preprocessor.class")
      .defaultValue("org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The preprocessor of every video extension (mp4, mov, mkv, webm and avi), so a job prepares all its video "
          + "with one implementation, read once at start. Default "
          + "org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor, the FFmpeg frame preprocessor. Hudi "
          + "does not ship FFmpeg (org.bytedeco): it is loaded only when the job's first video is opened, so a job without "
          + "video runs without it, and a job that prepares video adds the FFmpeg jars to its class path (org.bytedeco javacv, "
          + "javacpp and ffmpeg plus the javacpp and ffmpeg jars of the platform classifier, for example with spark.jars). The "
          + "per-extension "
          + "hoodie.unstructured.embedding.prep.preprocessor.<ext>.class key is a low-level override that wins for its "
          + "extension when set. A job that must not prepare video sets "
          + "hoodie.unstructured.embedding.prep.file.extensions to the non-video extensions it prepares.");

  /** Default video preprocessor: the FFmpeg frame preprocessor, named by class so this class does not depend on it. */
  public static final String DEFAULT_PREP_VIDEO_PREPROCESSOR_CLASS = PREP_VIDEO_PREPROCESSOR_CLASS.defaultValue();

  /**
   * Video data prep: a video whose source row declares more bytes ({@code source_file_size}) fails data prep (a quarantine entry)
   * before any byte is read. Default {@value #DEFAULT_PREP_VIDEO_MAX_BYTES} (20 GiB). A set value must be a positive byte count;
   * 0 or a negative value is refused on the driver when the job starts. Not pinned: it only refuses a file, so it can be changed
   * on an existing table; raising it does not re-prepare a file it already refused, whose quarantine entry stays until its source
   * object changes. The video is read through seekable
   * range reads with a fixed block buffer, so memory does not grow with its size; decode time and frame rows grow with its
   * duration.
   */
  public static final long DEFAULT_PREP_VIDEO_MAX_BYTES = 20L * 1024 * 1024 * 1024;

  public static final ConfigProperty<String> PREP_VIDEO_MAX_BYTES = ConfigProperty
      .key(PREP_PREFIX + "video.max.bytes")
      .defaultValue(Long.toString(DEFAULT_PREP_VIDEO_MAX_BYTES))
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Video data prep: a video whose source row declares more bytes (source_file_size) fails data prep (a quarantine "
          + "entry) before any byte is read. Default 21474836480 (20 GiB). A set value must be a positive byte count (0 or a negative "
          + "value is refused at job start). Not part of the table's stored prep contract: it only refuses a file, so it can be "
          + "changed on an existing table." + RAISED_CAP);

  /**
   * X of the video frame source's "closest keyframe every X" ({@code video.FfmpegKeyframeVideoFrameSource}), in milliseconds;
   * default {@link #DEFAULT_PREP_VIDEO_KEYFRAME_INTERVAL_MS}. A frame-extraction parameter, so part of the preprocessing style
   * when set. The table pins its effective value as {@code hoodie.table.embedding.prep.video.keyframe.interval.ms}; a change
   * requires a new table.
   */
  public static final ConfigProperty<Long> PREP_VIDEO_KEYFRAME_INTERVAL_MS = ConfigProperty
      .key(PREP_PREFIX + "video.keyframe.interval.ms")
      .defaultValue(1000L)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("X of the video frame source's \"closest keyframe every X\" (the FFmpeg keyframe source), in milliseconds; "
          + "default 1000. A frame-extraction parameter, so part of the preprocessing style when set. The table pins its effective value "
          + "(hoodie.table.embedding.prep.video.keyframe.interval.ms); a change requires a new table.");

  public static final long DEFAULT_PREP_VIDEO_KEYFRAME_INTERVAL_MS = 1000L;

  public static final String DEFAULT_PREP_VIDEO_DEDUP_METHOD = "dhash";

  /**
   * How the video frame source drops a frame that repeats the last kept one: {@code dhash} (64-bit difference hash within
   * {@link #PREP_VIDEO_DEDUP_MAX_DISTANCE}), {@code sha256} (identical encoded frame) or {@code none}; default
   * {@link #DEFAULT_PREP_VIDEO_DEDUP_METHOD}. Preprocessing style when set.
   */
  public static final ConfigProperty<String> PREP_VIDEO_DEDUP_METHOD = ConfigProperty
      .key(PREP_PREFIX + "video.dedup.method")
      .defaultValue(DEFAULT_PREP_VIDEO_DEDUP_METHOD)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("How the video frame source drops a frame that repeats the last kept one: dhash (64-bit difference hash "
          + "within hoodie.unstructured.embedding.prep.video.dedup.max.distance), sha256 (identical encoded frame) or "
          + "none; default dhash. Preprocessing style when set.");

  /** The largest dHash Hamming distance (0..64) that counts as a repeat; default 4. Preprocessing style when set. */
  public static final ConfigProperty<Long> PREP_VIDEO_DEDUP_MAX_DISTANCE = ConfigProperty
      .key(PREP_PREFIX + "video.dedup.max.distance")
      .defaultValue(4L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The largest dHash Hamming distance (0..64) that counts as a repeat; default 4. Preprocessing style when "
          + "set.");

  public static final long DEFAULT_PREP_VIDEO_DEDUP_MAX_DISTANCE = 4L;

  /**
   * The most pixels per frame a video may declare; a larger one is its file's quarantine entry before any frame is decoded, which
   * bounds the decoded frame and its raster per task. Default {@link #DEFAULT_PREP_VIDEO_MAX_PIXELS} (3840x2160). Preprocessing
   * style when set.
   */
  public static final ConfigProperty<Long> PREP_VIDEO_MAX_PIXELS = ConfigProperty
      .key(PREP_PREFIX + "video.max.pixels")
      .defaultValue(8294400L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The most pixels per frame a video may declare; a larger one is its file's quarantine entry before any frame "
          + "is decoded, which bounds the decoded frame and its raster per task. Default 8294400 (3840x2160). "
          + "Preprocessing style when set.");

  public static final long DEFAULT_PREP_VIDEO_MAX_PIXELS = 3840L * 2160;

  /**
   * The per-executor heap a job that preps video leaves outside its task slots (Spark's own structures, per-JVM caches) in the
   * frame-heap start check ({@code prep.VideoFrameHeapCheck}); default {@link #DEFAULT_PREP_VIDEO_HEAP_FIXED_BYTES} (64 MiB, an
   * estimate). Not part of the preprocessing style.
   */
  public static final ConfigProperty<Long> PREP_VIDEO_HEAP_FIXED_BYTES = ConfigProperty
      .key(PREP_PREFIX + "video.heap.fixed.bytes")
      .defaultValue(67108864L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The per-executor heap a job that preps video leaves outside its task slots (Spark's own structures, "
          + "per-JVM caches) in the frame-heap start check (the video frame heap check); default 67108864 (64 MiB, an estimate). "
          + "Not part of the preprocessing style.");

  public static final long DEFAULT_PREP_VIDEO_HEAP_FIXED_BYTES = 64L * 1024 * 1024;

  /**
   * Data prep caches the next whole out-of-line object when its listed size fits this bound and its preprocessor limits.
   * Cached bytes avoid another download; source-version checks still send status requests.
   * A task holds at most two such buffers, the current file's and the next one's: a job that preps video reads ahead only when
   * they fit the executor heap beside its frame decodes (2 x this per task slot), else it reads nothing ahead, with a WARN. An
   * image is read ahead only when it is no larger than {@link #PREP_IMAGE_HEADER_MAX_BYTES}. 0 reads no file ahead. Default
   * {@link #DEFAULT_PREP_PREFETCH_MAX_BYTES} (8 MiB). Changes no output rows: not style.
   */
  public static final ConfigProperty<Long> PREP_PREFETCH_MAX_BYTES = ConfigProperty
      .key(PREP_PREFIX + "prefetch.max.bytes")
      .defaultValue(8388608L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Data prep reads the next whole out-of-line object ahead while preparing the current file, when its "
          + "listed size fits this bound and the preprocessor's input limit. Larger objects and explicit ranges are not cached. "
          + "An image must also fit hoodie.unstructured.embedding.prep.image.header.max.bytes. "
          + "A task holds at most two downloaded buffers; a video task disables prefetch when its frame-heap check cannot "
          + "admit them. Cached bytes avoid another download; source-version validation still sends status requests. "
          + "0 disables read ahead. Default 8388608 (8 MiB). Changes no output rows: not style.");

  public static final long DEFAULT_PREP_PREFETCH_MAX_BYTES = 8L * 1024 * 1024;

  /** The largest {@link #PREP_PREFETCH_MAX_BYTES}: one Java array. */
  public static final long MAX_PREP_PREFETCH_MAX_BYTES = Integer.MAX_VALUE - 8L;

  /**
   * FFmpeg decoder threads per video ({@code thread_count}); default {@link #DEFAULT_VIDEO_FFMPEG_THREADS}, at most
   * {@link #MAX_VIDEO_FFMPEG_THREADS}. Set explicitly because FFmpeg's own default (0) starts one thread per core the executor
   * may use; parallelism comes from Spark tasks, one video per task at a time. Changes no output: not style.
   */
  public static final ConfigProperty<Long> VIDEO_FFMPEG_THREADS = ConfigProperty
      .key(PREFIX + "video.ffmpeg.threads")
      .defaultValue(1L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("FFmpeg decoder threads per video (thread_count); default 1, at most 16. Set explicitly because FFmpeg's own "
          + "default (0) starts one thread per core the executor may use; parallelism comes from Spark tasks, one video "
          + "per task at a time. Changes no output: not style.");

  public static final long DEFAULT_VIDEO_FFMPEG_THREADS = 1L;

  public static final long MAX_VIDEO_FFMPEG_THREADS = 16L;

  /** Prefix of the row-balancing keys; they do not shape the preprocessing style. */
  public static final String BALANCE_PREFIX = PREP_PREFIX + "balance.";

  /**
   * RowBalancingStrategy implementation; the default, {@code balance.SizeTypeAwareBalancingStrategy}, gives each preprocessor
   * group its own tasks, sized by the group's byte budget per task. {@code balance.HashBalancingStrategy} spreads the files over
   * exactly {@code num.partitions} tasks by a hash of their source version, with one shuffle: balanced in expectation, not exactly.
   */
  public static final ConfigProperty<String> BALANCE_STRATEGY_CLASS = ConfigProperty
      .key(BALANCE_PREFIX + "strategy.class")
      .defaultValue("org.apache.hudi.utilities.unstructured.embedding.balance.SizeTypeAwareBalancingStrategy")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("RowBalancingStrategy implementation. The default, SizeTypeAwareBalancingStrategy, gives each "
          + "preprocessor group (pdf, image, video, a custom preprocessor) its own tasks, sized by the group's byte budget per "
          + "task. HashBalancingStrategy spreads the files over exactly hoodie.unstructured.embedding.prep.num.partitions "
          + "tasks by a hash of their source version (path sha, modification time, size), with one shuffle and no plan: a file's "
          + "task depends on its version alone, and task file counts are balanced in expectation, not exactly equal.");

  /** Exact task count of {@code balance.HashBalancingStrategy}; positive, default {@code spark.default.parallelism}. */
  public static final ConfigProperty<String> BALANCE_NUM_PARTITIONS = ConfigProperty
      .key(PREP_PREFIX + "num.partitions")
      .noDefaultValue("<derived>")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Exact task count of HashBalancingStrategy, a positive integer (default spark.default.parallelism). "
          + "Ignored by SizeTypeAwareBalancingStrategy; its per-group floor uses hoodie.unstructured.embedding.prep.balance.min.partitions.");

  /**
   * Parallelism P of {@code balance.SizeTypeAwareBalancingStrategy}'s per-group floor: at least {@code min(files, P)} tasks per
   * group, with more when its source-byte budget requires them. Positive; default derived from the job's executor capacity.
   * Independent of {@link #BALANCE_NUM_PARTITIONS}; neither key is an alias for the other. Not preprocessing style.
   */
  public static final ConfigProperty<String> BALANCE_MIN_PARTITIONS = ConfigProperty
      .key(BALANCE_PREFIX + "min.partitions")
      .noDefaultValue("<derived>")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Parallelism P of SizeTypeAwareBalancingStrategy's per-group floor: at least min(files, P) tasks per "
          + "group, with more when its source-byte budget requires them. Positive; default derived from the job's executor "
          + "capacity. Independent of hoodie.unstructured.embedding.prep.num.partitions, which only the hash strategy reads. "
          + "Not part of the preprocessing style.");

  /**
   * Suffix of the per-group byte budget of one prep task, {@code hoodie.unstructured.embedding.prep.balance.<group>.max.bytes.per.task}
   * ({@code balance.SizeTypeAwareBalancingStrategy}); {@code <group>} is {@code pdf}, {@code image}, {@code video} or a custom
   * preprocessor's fully qualified class name. Must be positive (bytes).
   */
  public static final String BALANCE_MAX_BYTES_PER_TASK_SUFFIX = ".max.bytes.per.task";

  /** Metadata for the hoodie.unstructured.embedding.prep.balance.{@code <group>}.max.bytes.per.task family. */
  public static final ConfigProperty<String> BALANCE_MAX_BYTES_PER_TASK_PROPERTY = ConfigProperty
      .key("hoodie.unstructured.embedding.prep.balance.<group>.max.bytes.per.task")
      .noDefaultValue("<group-dependent>")
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The per-group byte budget of one prep task, "
          + "hoodie.unstructured.embedding.prep.balance.<group>.max.bytes.per.task "
          + "(org.apache.hudi.utilities.unstructured.embedding.balance.SizeTypeAwareBalancingStrategy); <group> is pdf, "
          + "image, video or a custom preprocessor's fully qualified class name. Must be positive (bytes). Replace "
          + "<group> with a balancing group. pdf=" + SizeTypeAwareBalancingStrategy.DEFAULT_BALANCE_PDF_MAX_BYTES_PER_TASK
          + "; image=" + SizeTypeAwareBalancingStrategy.DEFAULT_BALANCE_IMAGE_MAX_BYTES_PER_TASK
          + "; video=" + SizeTypeAwareBalancingStrategy.DEFAULT_BALANCE_VIDEO_MAX_BYTES_PER_TASK
          + "; custom group=hoodie.unstructured.embedding.prep.balance.default.max.bytes.per.task ("
          + SizeTypeAwareBalancingStrategy.DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK + "). "
          + "The video budget bounds source bytes per task; frame memory is per task slot and decode time follows duration "
          + "and resolution. Lower the video budget to spread source bytes over more tasks; tune executor slots/heap for frames. Default: "
          + "<group-dependent>.");

  /**
   * The byte budget per task of a custom preprocessor group with no budget key of its own; default
   * {@value SizeTypeAwareBalancingStrategy#DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK} bytes, the shared PDF budget.
   */
  public static final ConfigProperty<Long> BALANCE_DEFAULT_MAX_BYTES_PER_TASK = ConfigProperty
      .key(BALANCE_PREFIX + "default" + BALANCE_MAX_BYTES_PER_TASK_SUFFIX)
      .defaultValue(SizeTypeAwareBalancingStrategy.DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The byte budget per task of a custom preprocessor group with no budget key of its own; default the pdf "
          + "budget, " + SizeTypeAwareBalancingStrategy.DEFAULT_BALANCE_DEFAULT_MAX_BYTES_PER_TASK
          + " bytes (org.apache.hudi.utilities.unstructured.embedding.balance.SizeTypeAwareBalancingStrategy). "
          + "custom-group fallback; built-in group defaults unaffected by this key.");

  /**
   * Tries in all of opening one blob's stored bytes when the open fails transiently (a throttled, reset or timed-out
   * request), within the same prep round; the last failure fails the round, which the next run retries from the same
   * checkpoint. A file's own fault (gone, short) and an access denial are never retried. Must be positive. Not part of the
   * preprocessing style.
   */
  public static final ConfigProperty<Integer> PREP_SOURCE_RETRY_MAX_ATTEMPTS = ConfigProperty
      .key(PREP_PREFIX + "source.retry.max.attempts")
      .defaultValue(3)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Tries in all of opening one blob's stored bytes when the open fails transiently (a throttled, reset or "
          + "timed-out request), within the same prep round; the last failure fails the round, which the next run retries "
          + "from the same checkpoint. A file's own fault (gone, short) and an access denial are never retried. A positive "
          + "integer. Not part of the preprocessing style.");

  /** Default {@link #PREP_SOURCE_RETRY_MAX_ATTEMPTS}: 3, as the compute stage's per-row tries. */
  public static final int DEFAULT_PREP_SOURCE_RETRY_MAX_ATTEMPTS = PREP_SOURCE_RETRY_MAX_ATTEMPTS.defaultValue();

  /**
   * The wait in ms before the second try of a source open ({@link #PREP_SOURCE_RETRY_MAX_ATTEMPTS}); each later wait doubles,
   * capped at 30 s. Must not be negative. Not part of the preprocessing style.
   */
  public static final ConfigProperty<Long> PREP_SOURCE_RETRY_BACKOFF_MS = ConfigProperty
      .key(PREP_PREFIX + "source.retry.backoff.ms")
      .defaultValue(1000L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The wait in ms before the second try of a source open "
          + "(hoodie.unstructured.embedding.prep.source.retry.max.attempts); each later wait doubles, capped at 30 s. "
          + "Must not be negative. Not part of the preprocessing style.");

  /** Default {@link #PREP_SOURCE_RETRY_BACKOFF_MS}: one second. */
  public static final long DEFAULT_PREP_SOURCE_RETRY_BACKOFF_MS = PREP_SOURCE_RETRY_BACKOFF_MS.defaultValue();

  /**
   * The file extensions data prep consumes: comma-separated, without the dot, in any letter case. A blob row whose
   * {@code source_file_extension} is not listed, including a row with an empty extension or a FAILED ingest, is not
   * prepped: it gets no embedding row and no error-table event. Unset or blank, the default, every blob row is prepped. A
   * set value that lists no extension is refused ({@link #prepFileExtensions}). Each listed extension needs a
   * preprocessor. Not part of the preprocessing style: a changed list applies from the next run's batch on, and blob rows
   * already behind the checkpoint are not read again.
   */
  public static final ConfigProperty<String> PREP_FILE_EXTENSIONS = ConfigProperty
      .key(PREP_PREFIX + "file.extensions")
      .defaultValue("")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The file extensions data prep consumes: comma-separated, without the dot, in any letter case. A blob row "
          + "whose source_file_extension is not listed, including a row with an empty extension or a FAILED ingest, is not "
          + "prepped: it gets no embedding row and no error-table event. Unset or blank, the default, every blob row is "
          + "prepped. A set value that lists no extension is refused. Each listed extension needs a preprocessor. Not part of "
          + "the preprocessing style: a changed list applies from the next run's batch on, and blob rows already behind the "
          + "checkpoint are not read again.");

  /** Rows per EmbeddingClient call (micro batch), must be positive. Only for prep_and_compute. */
  public static final ConfigProperty<Integer> COMPUTE_BATCH_SIZE = ConfigProperty
      .key(PREFIX + "compute.batch.size")
      .defaultValue(64)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Rows per embedding client call (micro batch), a positive integer. Only for prep_and_compute.");

  /** Default micro-batch size. */
  public static final int DEFAULT_COMPUTE_BATCH_SIZE = COMPUTE_BATCH_SIZE.defaultValue();

  /**
   * Tries of one row within one compute step: a row whose call fails transiently (the call throws, the answer is malformed,
   * the client answers RETRYABLE) is tried again, with backoff ({@link #COMPUTE_RETRY_BACKOFF_MS}), until it has been tried
   * this many times; a row still failing then fails the compute commit. An embedding client's own request retries happen
   * inside one try. Must be positive. Only for prep_and_compute.
   */
  public static final ConfigProperty<Integer> COMPUTE_MAX_ATTEMPTS = ConfigProperty
      .key(PREFIX + "compute.max.attempts")
      .defaultValue(3)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Tries of one row within one compute step: a row whose call fails transiently (the call throws, the answer "
          + "is malformed, the client answers RETRYABLE) is tried again, with backoff "
          + "(hoodie.unstructured.embedding.compute.retry.backoff.ms), until it has been tried this many times; a row "
          + "still failing then fails the compute commit. An embedding client's own request retries happen inside one "
          + "try. A positive integer. Only for prep_and_compute.");

  /** Default tries per row: the first try plus two retries. */
  public static final int DEFAULT_COMPUTE_MAX_ATTEMPTS = COMPUTE_MAX_ATTEMPTS.defaultValue();

  /**
   * The wait in ms before the second try of a row ({@link #COMPUTE_MAX_ATTEMPTS}); each later wait doubles, capped at
   * {@link #COMPUTE_RETRY_MAX_BACKOFF_MS}. Must not be negative. Only for prep_and_compute.
   */
  public static final ConfigProperty<Long> COMPUTE_RETRY_BACKOFF_MS = ConfigProperty
      .key(PREFIX + "compute.retry.backoff.ms")
      .defaultValue(1000L)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The wait in ms before the second try of a row (hoodie.unstructured.embedding.compute.max.attempts); each "
          + "later wait doubles, capped at 60000 ms. Must not be negative. Only for prep_and_compute.");

  /** Default {@link #COMPUTE_RETRY_BACKOFF_MS}: one second. */
  public static final long DEFAULT_COMPUTE_RETRY_BACKOFF_MS = COMPUTE_RETRY_BACKOFF_MS.defaultValue();

  /** Cap of one wait between tries of a row: 60 s, the OpenAI-compatible client's default max backoff. */
  public static final long COMPUTE_RETRY_MAX_BACKOFF_MS = 60_000L;

  /**
   * Prefix of the image keys of the compute flow: prep stores an image's source bytes, and compute decodes and resizes them in
   * memory into the model input of a multimodal model's client ({@link ImagePreprocessingSpec}). They do not change what prep
   * stores, and like every compute key they are refused under {@value #MODE_PREP_ONLY}.
   */
  public static final String COMPUTE_IMAGE_PREFIX = PREFIX + "compute.image.";

  /** The long side in pixels compute resizes an image to (never upscaled); default {@value ImagePreprocessingSpec#DEFAULT_LONG_SIDE}. */
  public static final ConfigProperty<Integer> COMPUTE_IMAGE_LONG_SIDE = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "long.side")
      .defaultValue(ImagePreprocessingSpec.DEFAULT_LONG_SIDE)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The long side in pixels compute resizes an image to before the model call (never upscaled); must be "
          + "positive; default 512 (Vertex multimodalembedding resizes to 512x512 itself). Refused under prep_only.");

  /**
   * The most bytes the decoded raster may take: the decode subsampling grows until the raster fits; default 16 MiB. An image no
   * subsampling fits is refused before decode (PERMANENT for its row).
   */
  public static final ConfigProperty<Long> COMPUTE_IMAGE_RASTER_BUDGET_BYTES = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "raster.budget.bytes")
      .defaultValue(ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The most bytes the decoded raster of one image may take in compute: the decode subsampling grows until "
          + "the raster fits, and an image no subsampling fits is refused before decode. Must be positive; default 16777216 "
          + "(16 MiB). Refused under prep_only.");

  /** Images whose header declares more pixels are refused by compute before decode; default 100,000,000. */
  public static final ConfigProperty<Long> COMPUTE_IMAGE_MAX_PIXELS = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "max.pixels")
      .defaultValue(ImagePreprocessingSpec.DEFAULT_MAX_PIXELS)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("Images whose header declares more pixels are refused by compute before decode (PERMANENT for the row). "
          + "Must be positive; default 100000000. Refused under prep_only.");

  /**
   * The most bytes an image reader may need before subsampling applies (a full-resolution TIFF strip or tile, the coefficient
   * buffers of a progressive JPEG), estimated from the image metadata; above it the image is refused before decode; default
   * 64 MiB ({@link ImagePreprocessingSpec#DEFAULT_INTERMEDIATE_BUDGET_BYTES}).
   */
  public static final ConfigProperty<Long> COMPUTE_IMAGE_INTERMEDIATE_BUDGET_BYTES = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "intermediate.budget.bytes")
      .defaultValue(ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES)
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The most bytes an image reader may need before subsampling applies (a full-resolution TIFF strip or tile, "
          + "a BMP RLE bitmap read whole, full-width PNG or uncompressed BMP rows, the coefficient buffers of a progressive JPEG), "
          + "estimated from the image header; above it compute refuses the "
          + "image before decode (PERMANENT for the row). Must be positive; default 67108864 (64 MiB). Refused under prep_only.");

  /** The {@link ImageDecoder} compute decodes images with; it must run this code's decoding logic; default in-process. */
  public static final ConfigProperty<String> COMPUTE_IMAGE_DECODER_CLASS = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "decoder.class")
      .defaultValue(InProcessImageDecoder.class.getName())
      .markAdvanced()
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The ImageDecoder implementation compute decodes and resizes images with, in this JVM or elsewhere; it "
          + "must have a public no-arg constructor and run this code's decoding logic (image-resize-v2), else the job is "
          + "refused at start. Default the in-process decoder. Refused under prep_only.");

  /** {@link #COMPUTE_IMAGE_ENCODING} value: JPEG at {@link #COMPUTE_IMAGE_JPEG_QUALITY}. */
  public static final String IMAGE_ENCODING_JPEG = ImagePreprocessingSpec.ENCODING_JPEG;

  /** {@link #COMPUTE_IMAGE_ENCODING} value: lossless PNG. */
  public static final String IMAGE_ENCODING_PNG = ImagePreprocessingSpec.ENCODING_PNG;

  /** The encoding of the resized model input: {@value #IMAGE_ENCODING_JPEG} (default) or {@value #IMAGE_ENCODING_PNG}. */
  public static final ConfigProperty<String> COMPUTE_IMAGE_ENCODING = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "encoding")
      .defaultValue(ImagePreprocessingSpec.DEFAULT_ENCODING)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The encoding of the resized image compute sends to the model: jpeg (default) or png, in any letter case. "
          + "Refused under prep_only.");

  /** JPEG quality in [1, 100] of the resized model input; default {@value ImagePreprocessingSpec#DEFAULT_JPEG_QUALITY}. */
  public static final ConfigProperty<Integer> COMPUTE_IMAGE_JPEG_QUALITY = ConfigProperty
      .key(COMPUTE_IMAGE_PREFIX + "jpeg.quality")
      .defaultValue(ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("JPEG quality in [1, 100] of the resized image compute sends to the model under the jpeg encoding; "
          + "default 90. Refused under prep_only.");

  /** {@link #MODE}: prepare rows only; the job declares no model, so the table has no model column. */
  public static final String MODE_PREP_ONLY = "prep_only";

  /** {@link #MODE}: prepare rows and compute their embeddings in the same commit. */
  public static final String MODE_PREP_AND_COMPUTE = "prep_and_compute";

  /**
   * What one round of the embedding job does: {@value #MODE_PREP_ONLY} writes the prepared rows and sets no model or compute
   * key, so the table has no model column; {@value #MODE_PREP_AND_COMPUTE} also computes every row's embedding with the
   * active model ({@link #ACTIVE_MODELS}), into its VECTOR column, before the same commit. Required, no default. A table is
   * written in one mode: to change the mode, write a new table.
   */
  public static final ConfigProperty<String> MODE = ConfigProperty
      .key(PREFIX + "mode")
      .noDefaultValue(REQUIRED)
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("What one round of the embedding job does: prep_only writes the prepared rows and sets no model or compute "
          + "key, so the table has no model column; prep_and_compute also computes every row's embedding with the active model "
          + "(hoodie.unstructured.embedding.active.models), into its VECTOR column, before the same commit. A table is "
          + "written in one mode: to change the mode, write a new table.");

  /**
   * The number of Spark tasks the embedding computation runs in, under {@value #MODE_PREP_AND_COMPUTE}: the prepared rows are
   * spread over them by a hash of their key columns, so task row counts are balanced in expectation. Must be positive; default
   * the Spark context's default parallelism.
   */
  public static final ConfigProperty<String> COMPUTE_PARALLELISM = ConfigProperty
      .key(PREFIX + "compute.parallelism")
      .noDefaultValue("the Spark context's default parallelism")
      .sinceVersion(SINCE_1_3_0)
      .withDocumentation("The number of Spark tasks the embedding computation runs in, under prep_and_compute: the prepared rows "
          + "are spread over them by a hash of their key columns, so task row counts are balanced in expectation. A positive "
          + "integer.");

  private final TypedProperties props;

  private EmbeddingTableConfig(TypedProperties props) {
    this.props = props;
  }

  public static EmbeddingTableConfig from(TypedProperties props) {
    return new EmbeddingTableConfig(props);
  }

  public TypedProperties getProps() {
    return props;
  }

  /**
   * The level every cached copy of a data prep run's rows persists at (the balancer's assignment, the chunk rows): Hudi's
   * {@code hoodie.write.tagged.record.storage.level}, default {@code MEMORY_AND_DISK_SER}, read as upstream {@code Source}
   * reads it, so prep has one persist level throughout, the one its write uses for the tagged records.
   */
  public static StorageLevel persistLevel(TypedProperties props) {
    return StorageLevel.fromString(
        ConfigUtils.getStringWithAltKeys(props, HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE, true));
  }

  /**
   * The embedding table: the streamer's target base path ({@code hoodie.base.path}, which the streamer sets from
   * {@code --target-base-path} for the transformer it hands the round's instant, and for the write validators), so every guard and the payload
   * destination check the table the rows are written to.
   */
  public String targetTablePath() {
    return required(HoodieWriteConfig.BASE_PATH.key());
  }

  /**
   * Per-chunk token limit ({@link #CHUNK_MAX_TOKENS}).
   *
   * @throws IllegalArgumentException if the value is not positive
   */
  public int chunkMaxTokens() {
    return positive(CHUNK_MAX_TOKENS.key(), intValue(CHUNK_MAX_TOKENS, DEFAULT_CHUNK_MAX_TOKENS));
  }

  /**
   * Tokens two adjacent chunks may share ({@link #CHUNK_OVERLAP_TOKENS}); when unset,
   * {@code min(}{@link #DEFAULT_CHUNK_OVERLAP_TOKENS}{@code , chunkMaxTokens() / 2)}.
   *
   * @throws IllegalArgumentException if an explicitly set value is negative or above {@code chunkMaxTokens() / 2}
   */
  public int chunkOverlapTokens() {
    int maxOverlap = chunkMaxTokens() / 2;
    if (!props.containsKey(CHUNK_OVERLAP_TOKENS.key())) {
      return Math.min(DEFAULT_CHUNK_OVERLAP_TOKENS, maxOverlap);
    }
    int overlap = intValue(CHUNK_OVERLAP_TOKENS, 0);
    if (overlap < 0 || overlap > maxOverlap) {
      throw new IllegalArgumentException("Config " + CHUNK_OVERLAP_TOKENS.key() + " must be >= 0 and <= " + CHUNK_MAX_TOKENS.key()
          + " / 2 (" + maxOverlap + ") but is " + overlap);
    }
    return overlap;
  }

  /**
   * The round's mode, {@link #MODE}.
   *
   * @throws IllegalArgumentException when it is unset or not one of the two values
   */
  public String mode() {
    String value = ConfigUtils.getStringWithAltKeys(props, MODE, "").trim().toLowerCase(Locale.ROOT);
    if (!MODE_PREP_ONLY.equals(value) && !MODE_PREP_AND_COMPUTE.equals(value)) {
      throw new IllegalArgumentException("Config " + MODE.key() + " must be " + MODE_PREP_ONLY + " or " + MODE_PREP_AND_COMPUTE
          + (value.isEmpty() ? ", and it is unset" : ", got '" + value + "'") + ". Nothing was written");
    }
    return value;
  }

  /** Whether the round computes embeddings ({@link #MODE_PREP_AND_COMPUTE}). */
  public boolean computes() {
    return MODE_PREP_AND_COMPUTE.equals(mode());
  }

  /** The compute task count, {@link #COMPUTE_PARALLELISM}, else {@code defaultParallelism}. */
  public int computeParallelism(int defaultParallelism) {
    if (!props.containsKey(COMPUTE_PARALLELISM.key())) {
      return defaultParallelism;
    }
    return positive(COMPUTE_PARALLELISM.key(), intValue(COMPUTE_PARALLELISM, 0));
  }

  /**
   * Rows per EmbeddingClient call ({@link #COMPUTE_BATCH_SIZE}).
   *
   * @throws IllegalArgumentException if the value is not positive
   */
  public int computeBatchSize() {
    return positive(COMPUTE_BATCH_SIZE.key(), intValue(COMPUTE_BATCH_SIZE, DEFAULT_COMPUTE_BATCH_SIZE));
  }

  /**
   * Tries of one row within one compute step ({@link #COMPUTE_MAX_ATTEMPTS}).
   *
   * @throws IllegalArgumentException if the value is not positive
   */
  public int computeMaxAttempts() {
    return positive(COMPUTE_MAX_ATTEMPTS.key(), intValue(COMPUTE_MAX_ATTEMPTS, DEFAULT_COMPUTE_MAX_ATTEMPTS));
  }

  /**
   * The first wait between tries of a row ({@link #COMPUTE_RETRY_BACKOFF_MS}).
   *
   * @throws IllegalArgumentException if the value is negative
   */
  public long computeRetryBackoffMs() {
    long backoff = longValue(COMPUTE_RETRY_BACKOFF_MS, DEFAULT_COMPUTE_RETRY_BACKOFF_MS);
    if (backoff < 0) {
      throw new IllegalArgumentException("Config " + COMPUTE_RETRY_BACKOFF_MS.key() + " must not be negative but is " + backoff);
    }
    return backoff;
  }

  /**
   * How compute prepares image inputs ({@link #COMPUTE_IMAGE_PREFIX} keys), validated: positive numbers, an encoding of jpeg or
   * png, a JPEG quality in [1, 100], and a decoder class that loads, is an {@link ImageDecoder} and runs this code's decoding
   * logic.
   *
   * @throws IllegalArgumentException naming the offending key
   * @throws ImageDecoderLogicMismatchException when the decoder runs another logic version
   */
  public ImagePreprocessingSpec imagePreprocessing() {
    String decoderClass = ConfigUtils.getStringWithAltKeys(props, COMPUTE_IMAGE_DECODER_CLASS, InProcessImageDecoder.class.getName())
        .trim();
    ImagePreprocessingSpec spec = new ImagePreprocessingSpec(
        (int) positiveLong(COMPUTE_IMAGE_LONG_SIDE.key(), ImagePreprocessingSpec.DEFAULT_LONG_SIDE, Integer.MAX_VALUE),
        positiveLong(COMPUTE_IMAGE_RASTER_BUDGET_BYTES.key(), ImagePreprocessingSpec.DEFAULT_RASTER_BUDGET_BYTES, Long.MAX_VALUE),
        positiveLong(COMPUTE_IMAGE_MAX_PIXELS.key(), ImagePreprocessingSpec.DEFAULT_MAX_PIXELS, Long.MAX_VALUE),
        positiveLong(COMPUTE_IMAGE_INTERMEDIATE_BUDGET_BYTES.key(), ImagePreprocessingSpec.DEFAULT_INTERMEDIATE_BUDGET_BYTES,
            Long.MAX_VALUE),
        decoderClass,
        computeImageEncoding(),
        (int) positiveLong(COMPUTE_IMAGE_JPEG_QUALITY.key(), ImagePreprocessingSpec.DEFAULT_JPEG_QUALITY, 100));
    try {
      spec.newDecoder();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Config " + COMPUTE_IMAGE_DECODER_CLASS.key() + "=" + decoderClass + ": " + e.getMessage(),
          e);
    }
    return spec;
  }

  /**
   * The encoding of {@link #COMPUTE_IMAGE_ENCODING}, lower case.
   *
   * @throws IllegalArgumentException naming the key when it is neither jpeg nor png
   */
  private String computeImageEncoding() {
    String raw = ConfigUtils.getStringWithAltKeys(props, COMPUTE_IMAGE_ENCODING, IMAGE_ENCODING_JPEG);
    String value = raw.trim().toLowerCase(Locale.ROOT);
    if (!IMAGE_ENCODING_JPEG.equals(value) && !IMAGE_ENCODING_PNG.equals(value)) {
      throw new IllegalArgumentException("Config " + COMPUTE_IMAGE_ENCODING.key() + " must be " + IMAGE_ENCODING_JPEG + " or "
          + IMAGE_ENCODING_PNG + ", got '" + raw + "'");
    }
    return value;
  }

  private long positiveLong(String key, long defaultValue, long max) {
    return NumericConfig.positiveLong(props, key, defaultValue, max);
  }

  /**
   * {@link #BALANCE_NUM_PARTITIONS} when set, else {@code defaultValue}.
   *
   * @throws IllegalArgumentException if the configured value is not positive
   */
  public int balanceNumPartitions(int defaultValue) {
    if (!props.containsKey(BALANCE_NUM_PARTITIONS.key())) {
      return defaultValue;
    }
    return positive(BALANCE_NUM_PARTITIONS.key(), intValue(BALANCE_NUM_PARTITIONS, 0));
  }

  /**
   * The in-commit retry of a transient source open ({@link #PREP_SOURCE_RETRY_MAX_ATTEMPTS}, {@link #PREP_SOURCE_RETRY_BACKOFF_MS}).
   *
   * @throws IllegalArgumentException if the attempts are not positive or the backoff is negative
   */
  public TransientRetry prepSourceRetry() {
    int attempts = positive(PREP_SOURCE_RETRY_MAX_ATTEMPTS.key(),
        intValue(PREP_SOURCE_RETRY_MAX_ATTEMPTS, DEFAULT_PREP_SOURCE_RETRY_MAX_ATTEMPTS));
    long backoff = longValue(PREP_SOURCE_RETRY_BACKOFF_MS, DEFAULT_PREP_SOURCE_RETRY_BACKOFF_MS);
    if (backoff < 0) {
      throw new IllegalArgumentException("Config " + PREP_SOURCE_RETRY_BACKOFF_MS.key() + " must not be negative but is " + backoff);
    }
    return new TransientRetry(attempts, backoff);
  }

  /**
   * The extensions of {@link #PREP_FILE_EXTENSIONS}: trimmed, lower case, sorted, without duplicates; empty entries are ignored.
   *
   * @return empty when the key is unset or blank: every blob row is prepped
   * @throws IllegalArgumentException if the key is set but lists no extension
   */
  public Option<List<String>> prepFileExtensions() {
    String value = ConfigUtils.getStringWithAltKeys(props, PREP_FILE_EXTENSIONS, "");
    if (value.trim().isEmpty()) {
      return Option.empty();
    }
    List<String> extensions = Arrays.stream(value.split(",")).map(entry -> entry.trim().toLowerCase(Locale.ROOT))
        .filter(entry -> !entry.isEmpty()).distinct().sorted().collect(Collectors.toList());
    if (extensions.isEmpty()) {
      throw new IllegalArgumentException("Config " + PREP_FILE_EXTENSIONS.key() + "=" + value + " lists no file extension; unset it "
          + "to prep every extension");
    }
    return Option.of(Collections.unmodifiableList(extensions));
  }

  /** The single active model an embedding job computes; refuses zero or multiple active models. */
  public EmbeddingModelSpec activeModel() {
    List<EmbeddingModelSpec> active = activeModels();
    if (active.size() != 1) {
      throw new IllegalArgumentException("The embedding job fills one model column, but "
          + active.stream().map(EmbeddingModelSpec::getName).collect(Collectors.toList()) + " are active; declare one model in "
          + EmbeddingTableConfig.MODELS.key() + " or set " + EmbeddingTableConfig.ACTIVE_MODELS.key() + " to one of them. Nothing was "
          + "written");
    }
    return active.get(0);
  }

  /**
   * Models declared on the table, in declaration order. Each one owns a nullable VECTOR column.
   *
   * @throws IllegalArgumentException if a model's {@link #MODEL_DIMENSIONS_SUFFIX dimensions} is not positive
   * @throws HoodieException if an alias is listed twice, or a model's {@link #MODEL_DIMENSIONS_SUFFIX dimensions} or
   *     {@link #MODEL_PROVIDER_SUFFIX provider} is not set
   */
  public List<EmbeddingModelSpec> models() {
    List<EmbeddingModelSpec> specs = new ArrayList<>();
    List<String> names = splitList(ConfigUtils.getStringWithAltKeys(props, MODELS, ""));
    for (int i = 0; i < names.size(); i++) {
      if (names.subList(0, i).contains(names.get(i))) {
        throw new HoodieException("Embedding model alias " + names.get(i) + " is listed twice in " + MODELS.key());
      }
    }
    for (String name : names) {
      String keyPrefix = MODEL_PREFIX + name;
      String dimension = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(keyPrefix + MODEL_DIMENSIONS_SUFFIX).noDefaultValue(), null);
      if (StringUtils.isNullOrEmpty(dimension)) {
        throw new HoodieException("Missing required config " + keyPrefix + MODEL_DIMENSIONS_SUFFIX);
      }
      // no default provider: a model without one would store the dummy client's vectors under a dummy identity
      String provider = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(keyPrefix + MODEL_PROVIDER_SUFFIX).noDefaultValue(), null);
      if (StringUtils.isNullOrEmpty(provider) || provider.trim().isEmpty()) {
        throw new HoodieException("Missing required config " + keyPrefix + MODEL_PROVIDER_SUFFIX + ": name the embedding provider "
            + "of model " + name + ", one of " + String.join(", ", EmbeddingClientNames.PROVIDERS));
      }
      String clientClass = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(keyPrefix + MODEL_CLIENT_CLASS_SUFFIX).noDefaultValue(), null);
      EmbeddingModelSpec spec = new EmbeddingModelSpec(
          name,
          ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(keyPrefix + MODEL_COLUMN_SUFFIX).noDefaultValue(), "emb_" + name).trim(),
          positive(keyPrefix + MODEL_DIMENSIONS_SUFFIX, dimensions(keyPrefix + MODEL_DIMENSIONS_SUFFIX, dimension)),
          clientClass == null ? null : clientClass.trim(),
          provider.trim(),
          ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(keyPrefix + MODEL_NAME_SUFFIX).noDefaultValue(), name).trim(),
          modalities(keyPrefix + MODEL_MODALITIES_SUFFIX,
              ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(keyPrefix + MODEL_MODALITIES_SUFFIX).noDefaultValue(), null)));
      if (!spec.getColumn().matches("[A-Za-z_][A-Za-z0-9_]*")) {
        throw new IllegalArgumentException("Config " + keyPrefix + MODEL_COLUMN_SUFFIX + " resolves to '" + spec.getColumn()
            + "', which is not a valid Avro field name; set a column starting with a letter or underscore, followed by letters, digits or underscores");
      }
      specs.add(spec);
    }
    checkDistinctColumns(specs, props);
    return specs;
  }

  /**
   * Each declared model owns its {@code .column}; two models sharing one (compared case-insensitively, as Spark resolves
   * columns) would write one column under two identities, and one compute run per model, keyed by alias, would not keep their
   * runs apart. Each user partition column ({@link EmbeddingTableSchema#userPartitionColumns}, which leaves out a field that
   * names a fixed column) follows the model columns in the schema and must not repeat a model column or another partition field.
   *
   * @throws HoodieException naming both aliases and the shared column, or the partition field and what it repeats
   */
  private static void checkDistinctColumns(List<EmbeddingModelSpec> specs, TypedProperties props) {
    Map<String, String> owners = new HashMap<>();
    for (String base : EmbeddingTableSchema.BASE_COLUMNS) {
      owners.put(base, "the embedding table's own column " + base);
    }
    for (EmbeddingModelSpec spec : specs) {
      String column = spec.getColumn();
      String owner = owners.putIfAbsent(column.toLowerCase(Locale.ROOT), spec.getName() + " (" + column + ")");
      if (owner != null) {
        throw new HoodieException("Embedding models " + owner + " and " + spec.getName() + " (" + column + ") in "
            + MODELS.key() + " map to the same column " + column + ": each declared model needs its own column ("
            + MODEL_PREFIX + "<alias>" + MODEL_COLUMN_SUFFIX + ")");
      }
    }
    String partitionKey = CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key();
    Map<String, String> partitionFields = new HashMap<>();
    for (String column : EmbeddingTableSchema.userPartitionColumns(props)) {
      String earlier = partitionFields.putIfAbsent(column.toLowerCase(Locale.ROOT), column);
      if (earlier != null) {
        throw new HoodieException("Partition field " + column + " is listed twice in " + partitionKey + " (as " + earlier
            + " and " + column + "): each partition field is one column of the embedding table");
      }
      String owner = owners.get(column.toLowerCase(Locale.ROOT));
      if (owner != null) {
        throw new HoodieException("Embedding model " + owner + " in " + MODELS.key() + " and partition field " + column + " in "
            + partitionKey + " map to the same column " + column + ": rename the model's column (" + MODEL_PREFIX + "<alias>"
            + MODEL_COLUMN_SUFFIX + ") or the partition field");
      }
    }
  }

  /**
   * Models the compute flow fills: the declared models named in {@link #ACTIVE_MODELS}, in declaration order, or every
   * declared model when it is unset.
   *
   * @throws IllegalArgumentException if {@link #ACTIVE_MODELS} names an alias not declared in {@link #MODELS}: a typo next to a
   *     valid alias would otherwise be dropped silently and the job would run on the valid one alone
   */
  public List<EmbeddingModelSpec> activeModels() {
    String value = ConfigUtils.getStringWithAltKeys(props, ACTIVE_MODELS, "");
    List<String> active = splitList(value);
    List<EmbeddingModelSpec> declared = models();
    if (active.isEmpty()) {
      return declared;
    }
    List<String> names = declared.stream().map(EmbeddingModelSpec::getName).collect(Collectors.toList());
    List<String> undeclared = active.stream().filter(alias -> !names.contains(alias)).collect(Collectors.toList());
    if (!undeclared.isEmpty()) {
      throw new IllegalArgumentException("Config " + ACTIVE_MODELS.key() + "=" + value + " names models not declared in "
          + MODELS.key() + ": " + undeclared + "; declared: " + names);
    }
    return declared.stream().filter(m -> active.contains(m.getName())).collect(Collectors.toList());
  }

  public EmbeddingModelSpec model(String name) {
    return models().stream().filter(m -> m.getName().equals(name)).findFirst()
        .orElseThrow(() -> new HoodieException("Model '" + name + "' is not declared in " + MODELS.key()));
  }

  /**
   * @return whether {@code key} is a {@code hoodie.unstructured.embedding.prep.preprocessor.<ext>.class} key naming the
   *     preprocessor of one extension; any other key under {@link #PREPROCESSOR_CLASS_PREFIX} is a preprocessor tuning key
   */
  public static boolean isPreprocessorClassKey(String key) {
    return key.startsWith(PREPROCESSOR_CLASS_PREFIX) && key.endsWith(PREPROCESSOR_CLASS_SUFFIX)
        && key.length() > PREPROCESSOR_CLASS_PREFIX.length() + PREPROCESSOR_CLASS_SUFFIX.length();
  }

  public static List<String> splitList(String value) {
    if (StringUtils.isNullOrEmpty(value)) {
      return Collections.emptyList();
    }
    return Arrays.stream(value.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
  }

  /** {@link #PREP_PREFETCH_MAX_BYTES}, in [0, {@link #MAX_PREP_PREFETCH_MAX_BYTES}]. */
  public long prepPrefetchMaxBytes() {
    long bytes = longValue(PREP_PREFETCH_MAX_BYTES, DEFAULT_PREP_PREFETCH_MAX_BYTES);
    if (bytes < 0 || bytes > MAX_PREP_PREFETCH_MAX_BYTES) {
      throw new IllegalArgumentException("Config " + PREP_PREFETCH_MAX_BYTES.key() + " must be in [0, " + MAX_PREP_PREFETCH_MAX_BYTES
          + "] but is " + bytes);
    }
    return bytes;
  }

  /** {@link #PREP_VIDEO_HEAP_FIXED_BYTES}, at least 0. */
  public long videoHeapFixedBytes() {
    long fixed = longValue(PREP_VIDEO_HEAP_FIXED_BYTES, DEFAULT_PREP_VIDEO_HEAP_FIXED_BYTES);
    if (fixed < 0) {
      throw new IllegalArgumentException("Config " + PREP_VIDEO_HEAP_FIXED_BYTES.key() + " must be >= 0 but is " + fixed);
    }
    return fixed;
  }

  /**
   * {@link #BALANCE_MIN_PARTITIONS} when set, else {@code defaultValue}; a size-aware per-group floor, independent of hash tasks.
   *
   * @throws IllegalArgumentException if the configured value is not positive
   */
  public int balanceMinPartitions(int defaultValue) {
    if (!props.containsKey(BALANCE_MIN_PARTITIONS.key())) {
      return defaultValue;
    }
    return positive(BALANCE_MIN_PARTITIONS.key(), intValue(BALANCE_MIN_PARTITIONS, 0));
  }

  private int intValue(ConfigProperty<?> property, int defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + property.key() + " must be an integer, got '" + raw + "'", e);
    }
  }

  private long longValue(ConfigProperty<?> property, long defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, property, String.valueOf(defaultValue));
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + property.key() + " must be an integer, got '" + raw + "'", e);
    }
  }

  /**
   * The modalities a {@link #MODEL_MODALITIES_SUFFIX} value declares: trimmed, lower case, sorted; empty when unset or blank
   * (refused at start for the active model under {@code prep_and_compute}).
   *
   * @throws IllegalArgumentException naming the key when the value lists no modality, or one other than {@code text} and
   *     {@code image}
   */
  static Set<String> modalities(String key, String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> declared = new TreeSet<>();
    for (String entry : raw.split(",")) {
      String modality = entry.trim().toLowerCase(Locale.ROOT);
      if (modality.isEmpty()) {
        continue;
      }
      if (!EmbeddingTableSchema.MODALITY_TEXT.equals(modality) && !EmbeddingTableSchema.MODALITY_IMAGE.equals(modality)) {
        throw new IllegalArgumentException("Config " + key + "=" + raw + " names modality '" + entry.trim() + "'; the "
            + "modalities are " + EmbeddingTableSchema.MODALITY_TEXT + " and " + EmbeddingTableSchema.MODALITY_IMAGE
            + ", e.g. " + key + "=" + EmbeddingTableSchema.MODALITY_TEXT + "," + EmbeddingTableSchema.MODALITY_IMAGE);
      }
      declared.add(modality);
    }
    if (declared.isEmpty()) {
      throw new IllegalArgumentException("Config " + key + "=" + raw + " lists no modality; set it to "
          + EmbeddingTableSchema.MODALITY_TEXT + ", " + EmbeddingTableSchema.MODALITY_IMAGE + " or "
          + EmbeddingTableSchema.MODALITY_TEXT + "," + EmbeddingTableSchema.MODALITY_IMAGE);
    }
    return Collections.unmodifiableSet(declared);
  }

  private static int dimensions(String key, String raw) {
    try {
      return Integer.parseInt(raw.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + key + " must be an integer, got '" + raw + "'", e);
    }
  }

  private static int positive(String key, int value) {
    if (value <= 0) {
      throw new IllegalArgumentException("Config " + key + " must be positive but is " + value);
    }
    return value;
  }

  private static long positive(String key, long value) {
    if (value <= 0) {
      throw new IllegalArgumentException("Config " + key + " must be positive but is " + value);
    }
    return value;
  }

  private String required(String key) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), null);
    if (StringUtils.isNullOrEmpty(value)) {
      throw new HoodieException("Missing required config " + key);
    }
    return value;
  }
}
