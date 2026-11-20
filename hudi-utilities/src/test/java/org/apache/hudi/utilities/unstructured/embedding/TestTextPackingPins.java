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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.SequentialChunkIdScheme;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFilePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins values the embedding table persists from the text packing: the chunking config key spellings, and SHA-256 digests of
 * the chunk streams the PDF preprocessor produces on a fixed fixture. A PDF record is digested with its position in the
 * stream as a sequential chunk id, its modality, text, token count and aux (page bounds). A failure here means persisted
 * values would change.
 */
class TestTextPackingPins {

  private static final SequentialChunkIdScheme IDS = new SequentialChunkIdScheme();

  // Chunk count and SHA-256 of each chunk stream.
  private static final String PDF_BPE_DEFAULT =
      "11:87c33b812d4de51ebeaabf9eddd54d26cfac42be26849af0af4b2bc0927a758a";
  private static final String PDF_BPE_SMALL =
      "42:facaec503b4b1d4a9ccc11e94afcf070174d39699b8f35a7fe80a4a17c2a774b";
  private static final String PDF_WORDS_SMALL =
      "31:107c65cd4634f921b0e541360ff7f8467ee0d4ad7c54b6bb2f8f1ce8667cfae2";

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @Test
  void configKeySpellingsArePinned() {
    assertEquals("hoodie.unstructured.chunk.tokenizer.class", EmbeddingTableConfig.TOKENIZER_CLASS.key());
    assertEquals("hoodie.unstructured.chunk.tokenizer.type", EmbeddingTableConfig.TOKENIZER_TYPE.key());
    assertEquals("hoodie.unstructured.chunk.tokenizer.encoding", EmbeddingTableConfig.TOKENIZER_ENCODING.key());
    assertEquals("hoodie.unstructured.chunk.max.tokens", EmbeddingTableConfig.CHUNK_MAX_TOKENS.key());
    assertEquals("hoodie.unstructured.chunk.overlap.tokens", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key());
  }

  @Test
  void pdfChunksArePinned() throws Exception {
    byte[] pdf = PdfFixtures.prose(11, 4, 30);
    // [BPE default limits, BPE 64/32, words 48/8]: chunk count and SHA-256 of the chunk stream
    assertEquals(Arrays.asList(PDF_BPE_DEFAULT, PDF_BPE_SMALL, PDF_WORDS_SMALL), Arrays.asList(
        pdfDigest(pdf, new OpenAiBpeTokenizer(), -1, -1),
        pdfDigest(pdf, new OpenAiBpeTokenizer(), 64, 32),
        pdfDigest(pdf, new WordCountTokenizer(), 48, 8)));
  }

  private static String pdfDigest(byte[] pdf, Tokenizer tokenizer, int maxTokens, int overlapTokens) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    long ordinal = 0;
    try (ClosableIterator<Chunk> chunks = new PdfFilePreprocessor().open(() -> new ByteArrayInputStream(pdf),
        new FileMeta("fid-pin", "pdf", pdf.length), tokenizer, props(maxTokens, overlapTokens))) {
      while (chunks.hasNext()) {
        Chunk chunk = chunks.next();
        update(digest, IDS.chunkId(ordinal++) + "\0" + chunk.getModality() + "\0" + chunk.getText() + "\0"
            + chunk.getTokenCount() + "\0" + new TreeMap<>(chunk.getAux()));
      }
    }
    return ordinal + ":" + hex(digest);
  }

  private static TypedProperties props(int maxTokens, int overlapTokens) {
    TypedProperties props = new TypedProperties();
    if (maxTokens >= 0) {
      props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), String.valueOf(maxTokens));
    }
    if (overlapTokens >= 0) {
      props.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), String.valueOf(overlapTokens));
    }
    return props;
  }

  private static void update(MessageDigest digest, String entry) {
    digest.update((entry + "\1").getBytes(StandardCharsets.UTF_8));
  }

  private static String hex(MessageDigest digest) {
    StringBuilder hex = new StringBuilder();
    for (byte b : digest.digest()) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
