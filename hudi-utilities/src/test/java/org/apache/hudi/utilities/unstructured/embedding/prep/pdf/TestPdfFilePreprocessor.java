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
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingException;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceKind;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The FilePreprocessor adapter and the versioned implementation identity. */
class TestPdfFilePreprocessor {

  private static final Tokenizer BPE = new OpenAiBpeTokenizer();

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  private static List<Chunk> run(PdfFilePreprocessor preprocessor, byte[] pdf, TypedProperties props) {
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
  void okChunksCarryTextTokensAndPageAux() throws IOException {
    byte[] pdf = PdfFixtures.text(TestPdfBoxChunker.pageText(1, 40, new Random(1)), TestPdfBoxChunker.pageText(2, 40,
        new Random(2)));
    List<Chunk> chunks = run(new PdfFilePreprocessor(), pdf, new TypedProperties());
    assertTrue(chunks.size() > 2);
    for (Chunk chunk : chunks) {
      assertEquals("text", chunk.getModality());
      assertNull(chunk.getPayload(), "a text chunk carries no payload");
      assertEquals(BPE.countTokens(chunk.getText()), (int) chunk.getTokenCount());
      assertTrue(Integer.parseInt(chunk.getAux().get(PdfFilePreprocessor.AUX_PAGE_START)) >= 1);
      assertTrue(chunk.getAux().containsKey(PdfFilePreprocessor.AUX_PAGE_END));
      assertTrue(chunk.getAux().containsKey(PdfFilePreprocessor.AUX_OVERLAP_CHARS));
      assertFalse(chunk.getAux().containsKey(PdfFilePreprocessor.AUX_PREP_LOSS));
    }
    assertTrue(chunks.stream().anyMatch(c -> !"0".equals(c.getAux().get(PdfFilePreprocessor.AUX_OVERLAP_CHARS))),
        "the default overlap applies");
  }

  @Test
  void lossSummaryRidesOnTheLastChunkAux() throws IOException {
    // page 2 fails PAGE_ERROR: its Type3 font has more CharProcs than the cap
    byte[] pdf = PdfFixtures.type3WithCharProcs(StreamDecodeGuard.MAX_CHAR_PROCS_PER_FONT + 1);
    List<Chunk> chunks = run(new PdfFilePreprocessor(), pdf, new TypedProperties());
    Chunk last = chunks.get(chunks.size() - 1);
    assertEquals("PAGES_LOST failed=1 truncated=0 glyph_limited=0 decode_limited=0 graphics_limited=0 page_limit=false "
        + "file_error=none", last.getAux().get(PdfFilePreprocessor.AUX_PREP_LOSS));
  }

  @Test
  void failedMarkerBecomesAnIsolatedFailure() {
    PdfFilePreprocessor preprocessor = new PdfFilePreprocessor();
    byte[] garbage = "garbage, not a pdf".getBytes(StandardCharsets.US_ASCII);
    TypedProperties props = new TypedProperties();
    PreprocessingException e = assertThrows(PreprocessingException.class, () -> run(preprocessor, garbage, props));
    assertTrue(e.getMessage().startsWith("LOAD_ERROR: "), e.getMessage());
  }

  @Test
  void noTextMarkerBecomesNoChunks() throws IOException {
    assertTrue(run(new PdfFilePreprocessor(), PdfFixtures.imageOnly(1, 30), new TypedProperties()).isEmpty());
  }

  @Test
  public void emptyNormalizedTruncationBecomesAnIsolatedFailure() {
    PdfFilePreprocessor preprocessor = new PdfFilePreprocessor(new PdfBoxChunker((supplier, meta, config) ->
        new TestPdfChunkIterator.FakeSource(supplier, 1, page -> () -> new PageText(" \n\u00AD", true))));
    PreprocessingException failure = assertThrows(PreprocessingException.class,
        () -> run(preprocessor, new byte[1], new TypedProperties()));
    assertTrue(failure.getMessage().contains("PAGES_LOST failed=0 truncated=1"), failure.getMessage());
  }

  @Test
  public void invalidConfigFailsAtOpen() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "8");
    PdfFilePreprocessor preprocessor = new PdfFilePreprocessor();
    assertThrows(IllegalArgumentException.class, () -> preprocessor.validateConfig(props, BPE));
    FileMeta file = new FileMeta("f", "pdf", 0);
    BlobBytesSupplier empty = () -> new ByteArrayInputStream(new byte[0]);
    PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class,
        () -> preprocessor.open(empty, file, BPE, props));
    assertInstanceOf(IllegalArgumentException.class, refused.getCause());
  }

  @Test
  void registryDefaultsPdfToTheRealChunker() {
    PreprocessorRegistry registry = new PreprocessorRegistry(new TypedProperties());
    assertTrue(registry.forExtension("pdf").get() instanceof PdfFilePreprocessor);
    assertTrue(registry.forExtension("PDF").get() instanceof PdfFilePreprocessor);
    TypedProperties bad = new TypedProperties();
    bad.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "0");
    PreprocessorRegistry badRegistry = new PreprocessorRegistry(bad);
    assertThrows(IllegalArgumentException.class, () -> badRegistry.validateConfig(bad, BPE));
  }

  @Test
  void nameCarriesImplementationVersion() {
    assertEquals("pdf-chunker/5;pdf-text-extraction-v1", new PdfFilePreprocessor().name(), "the extraction logic, never the PDFBox jar version");
    assertEquals(new PdfFilePreprocessor().name(), new PreprocessorRegistry(new TypedProperties()).forExtension("pdf").get().name(),
        "the identity the registry's pdf preprocessor carries");
  }

  /**
   * Tripwire: the records for a fixed PDF and config are pinned. If this fails, the emitted records changed: bump
   * {@link PdfFilePreprocessor#IMPLEMENTATION_VERSION} (so {@link PdfFilePreprocessor#name()} in logs and error text tells the
   * implementations apart) and re-pin the digest.
   */
  @Test
  void goldenRecordsPinned() throws Exception {
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
  void invalidPdfPropsAtOpenAreAConfigurationError() throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE.key(), "true");
    PdfFilePreprocessor preprocessor = new PdfFilePreprocessor();
    byte[] pdf = PdfFixtures.imageOnly(1, 30);
    PreprocessorConfigurationException refused = assertThrows(PreprocessorConfigurationException.class,
        () -> run(preprocessor, pdf, props));
    assertInstanceOf(IllegalArgumentException.class, refused.getCause());
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE.key()), refused.getMessage());
  }

  @Test
  void sourceKindIsADocumentWithTextMarkers() {
    assertEquals(SourceKind.DOCUMENT, new PdfFilePreprocessor().sourceKind());
    assertEquals("text", new PdfFilePreprocessor().sourceKind().markerModality());
  }
}
