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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A dummy pdf preprocessor for tests: it chunks a fixed text, never the pdf's own text.
 *
 * <p>Instead of extracting text from the pdf it reads a FIXED text ({@link #FIXED_TEXT}) and cuts it into chunks
 * of at most {@code hoodie.unstructured.chunk.max.tokens} tokens counted by the configured {@link Tokenizer} (the operator's
 * example: 20 words). It still streams the real file bytes once ({@link StreamDigest}) and records
 * {@code src_bytes}/{@code src_crc32} in every chunk's aux, proving which bytes it was handed. Words are read one at a
 * time from a {@link Reader}; only the current chunk is buffered, and never more than {@link #MAX_CHUNK_BYTES} of it: a chunk
 * ends before a word would push it over that bound (text + aux, UTF-8), and a single word longer than it fails the file as soon
 * as it outgrows it. Being a dummy it does NOT apply
 * {@code hoodie.unstructured.chunk.overlap.tokens}: adjacent chunks never share words.
 */
public class DummyPdfPreprocessor implements FilePreprocessor {

  /** The most chunk content (text + aux, UTF-8 bytes) this preprocessor buffers. */
  static final int MAX_CHUNK_BYTES = 64 * 1024;

  public static final String NAME = "dummy-pdf-v0";
  /** The fixed text every dummy pdf "contains" (55 words; with 20-token chunks: 3 chunks). */
  public static final String FIXED_TEXT =
      "Hudi brings transactions record level updates and incremental processing to data lakes. "
      + "This dummy text stands in for the text a real extractor would take from the pdf "
      + "file. Each chunk holds at most the configured number of tokens, counted by the configured"
      + " tokenizer, and chunks keep the order in which their words appear here.";
  public static final String INJECTED_FAILURE = "injected dummy pdf failure after first chunk";

  private final String text;

  public DummyPdfPreprocessor() {
    this(FIXED_TEXT);
  }

  /** Test seam: a dummy whose pdfs "contain" {@code text} instead of {@link #FIXED_TEXT}. */
  DummyPdfPreprocessor(String text) {
    this.text = text;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public SourceKind sourceKind() {
    return SourceKind.DOCUMENT;
  }

  @Override
  public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
      throws IOException {
    Map<String, String> sourceDigest = StreamDigest.digest(bytes);
    EmbeddingTableConfig config = EmbeddingTableConfig.from(props);
    long auxBytes = 0;
    for (Map.Entry<String, String> entry : sourceDigest.entrySet()) {
      auxBytes += utf8Bytes(entry.getKey()) + utf8Bytes(entry.getValue());
    }
    return new WordChunkIterator(new StringReader(text), tokenizer, config.chunkMaxTokens(), MAX_CHUNK_BYTES - auxBytes,
        MAX_CHUNK_BYTES, sourceDigest, DummyFailures.shouldFail(file, props));
  }

  private static int utf8Bytes(String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  /** UTF-8 size of one UTF-16 code unit; a surrogate pair (4 bytes) counts 2 + 2. */
  private static int utf8Bytes(char c) {
    if (c < 0x80) {
      return 1;
    }
    if (c < 0x800 || Character.isSurrogate(c)) {
      return 2;
    }
    return 3;
  }

  /**
   * Packs whole words into chunks of at most {@code maxTokens} tokens whose text fits {@code textBudgetBytes} (the
   * byte bound minus the aux this dummy adds), so {@link #MAX_CHUNK_BYTES} holds while the chunk is built.
   */
  private static final class WordChunkIterator implements ClosableIterator<Chunk> {
    private final Reader reader;
    private final Tokenizer tokenizer;
    private final int maxTokens;
    private final long textBudgetBytes;
    private final int maxBytes;
    private final Map<String, String> sourceDigest;
    private final boolean failAfterFirstChunk;
    private String pendingWord;
    private int emitted;

    WordChunkIterator(Reader reader, Tokenizer tokenizer, int maxTokens, long textBudgetBytes, int maxBytes,
                      Map<String, String> sourceDigest, boolean failAfterFirstChunk) {
      this.reader = reader;
      this.tokenizer = tokenizer;
      this.maxTokens = maxTokens;
      this.textBudgetBytes = textBudgetBytes;
      this.maxBytes = maxBytes;
      this.sourceDigest = sourceDigest;
      this.failAfterFirstChunk = failAfterFirstChunk;
    }

    @Override
    public boolean hasNext() {
      if (failAfterFirstChunk && emitted == 1) {
        throw new PreprocessingException(INJECTED_FAILURE);
      }
      if (pendingWord == null) {
        pendingWord = readWord();
      }
      return pendingWord != null;
    }

    @Override
    public Chunk next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      StringBuilder chunk = new StringBuilder(pendingWord);
      long chunkBytes = utf8Bytes(pendingWord);
      pendingWord = null;
      int tokens = tokenizer.countTokens(chunk.toString());
      while (true) {
        String word = readWord();
        if (word == null) {
          break;
        }
        long candidateBytes = chunkBytes + 1 + utf8Bytes(word);
        if (candidateBytes > textBudgetBytes) {
          pendingWord = word;
          break;
        }
        // the candidate is bounded by the byte budget, so counting its tokens buffers at most the cap
        String candidate = chunk + " " + word;
        int candidateTokens = tokenizer.countTokens(candidate);
        if (candidateTokens > maxTokens) {
          pendingWord = word;
          break;
        }
        chunk.append(' ').append(word);
        chunkBytes = candidateBytes;
        tokens = candidateTokens;
      }
      emitted++;
      Map<String, String> aux = new HashMap<>(sourceDigest);
      return new Chunk(EmbeddingTableSchema.MODALITY_TEXT, chunk.toString(), tokens, aux);
    }

    /**
     * The next word, or null at the end of the text.
     *
     * @throws PreprocessingException as soon as the word outgrows the chunk text budget, before buffering more of it
     */
    private String readWord() {
      try {
        StringBuilder word = new StringBuilder();
        long wordBytes = 0;
        int c;
        while ((c = reader.read()) >= 0) {
          if (Character.isWhitespace(c)) {
            if (word.length() > 0) {
              return word.toString();
            }
          } else {
            wordBytes += utf8Bytes((char) c);
            if (wordBytes > textBudgetBytes) {
              throw new PreprocessingException("word longer than " + textBudgetBytes + " bytes, the chunk text budget of "
                  + maxBytes + " bytes minus the chunk aux");
            }
            word.append((char) c);
          }
        }
        return word.length() > 0 ? word.toString() : null;
      } catch (IOException e) {
        throw new HoodieIOException("Failed reading dummy pdf text", e);
      }
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new HoodieIOException("Failed closing dummy pdf text", e);
      }
    }
  }
}
