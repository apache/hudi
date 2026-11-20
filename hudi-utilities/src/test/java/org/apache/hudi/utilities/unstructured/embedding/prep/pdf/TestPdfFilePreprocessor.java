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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceKind;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The FilePreprocessor adapter and the versioned implementation identity. */
public class TestPdfFilePreprocessor {

  private static final Tokenizer BPE = new OpenAiBpeTokenizer();

  @BeforeAll
  public static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  private static List<Chunk> run(PdfFilePreprocessor preprocessor, byte[] pdf, TypedProperties props) throws IOException {
    List<Chunk> chunks = new ArrayList<>();
    try (ClosableIterator<Chunk> iterator = preprocessor.open(() -> new ByteArrayInputStream(pdf),
        new FileMeta("fid-a", "pdf", pdf.length), BPE, props)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    return chunks;
  }

  @Test
  public void noTextMarkerBecomesNoChunks() throws IOException {
    assertTrue(run(new PdfFilePreprocessor(), PdfFixtures.imageOnly(1, 30), new TypedProperties()).isEmpty());
  }

  @Test
  public void nameCarriesImplementationVersion() {
    assertEquals("pdf-chunker/5;pdf-text-extraction-v1", new PdfFilePreprocessor().name());
    assertEquals(new PdfFilePreprocessor().name(), new PreprocessorRegistry(new TypedProperties()).forExtension("pdf").get().name(),
        "the identity the registry's pdf preprocessor carries");
  }

  /**
   * Tripwire: the records for a fixed PDF and config are pinned. If this fails, the emitted records changed: bump
   * {@link PdfFilePreprocessor#IMPLEMENTATION_VERSION} (so {@link PdfFilePreprocessor#name()} in logs and error text tells the
   * implementations apart) and re-pin the digest.
   */
  @Test
  public void goldenRecordsPinned() throws Exception {
    byte[] pdf = PdfFixtures.text(TestPdfBoxChunker.pageText(1, 30, new Random(7)), TestPdfBoxChunker.pageText(2, 30,
        new Random(8)), "");
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    for (Chunk chunk : run(new PdfFilePreprocessor(), pdf, new TypedProperties())) {
      digest.update((chunk.getText() + "\0" + chunk.getTokenCount() + "\0" + new TreeMap<>(chunk.getAux()) + "\1")
          .getBytes(StandardCharsets.UTF_8));
    }
    StringBuilder hex = new StringBuilder();
    for (byte b : digest.digest()) {
      hex.append(String.format("%02x", b));
    }
    assertEquals(GOLDEN, hex.toString(), "emitted records changed: bump PdfFilePreprocessor.IMPLEMENTATION_VERSION and "
        + "re-pin GOLDEN");
  }

  static final String GOLDEN = "c896163ea171edf7c1312d5819d65759f0ab32809e019082044ebbf212d4cd1f";

  @Test
  public void invalidPdfPropsAtOpenAreAConfigurationError() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE.key(), "true");
    PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class,
        () -> run(new PdfFilePreprocessor(), PdfFixtures.imageOnly(1, 30), props));
    assertInstanceOf(IllegalArgumentException.class, refused.getCause());
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE.key()), refused.getMessage());
  }

  @Test
  public void sourceKindIsADocumentWithTextMarkers() {
    assertEquals(SourceKind.DOCUMENT, new PdfFilePreprocessor().sourceKind());
    assertEquals("text", new PdfFilePreprocessor().sourceKind().markerModality());
  }
}
