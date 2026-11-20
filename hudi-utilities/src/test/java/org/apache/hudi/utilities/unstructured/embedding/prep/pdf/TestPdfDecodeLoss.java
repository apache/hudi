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
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.Chunk;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepCounters;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepSideOutput;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingException;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessingIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.graphics.form.PDFormXObject;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Decode damage remains visible in emitted records and in the framework's file failure path. */
class TestPdfDecodeLoss {

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @Test
  void partialContentKeepsTextAndRecordsLoss() throws IOException {
    List<Chunk> chunks = prep(contents(true, 1));
    assertEquals(1, chunks.size());
    assertEquals("Kept text.", chunks.get(0).getText());
    String loss = chunks.get(0).getAux().get(PdfFilePreprocessor.AUX_PREP_LOSS);
    assertTrue(loss != null && loss.contains("streams_skipped=1"), String.valueOf(loss));
  }

  @Test
  void cachedUndecodableContentCountsOnEveryPage() throws IOException {
    List<Chunk> chunks = prep(contents(true, 3));
    assertEquals(1, chunks.size());
    assertEquals("Kept text. Kept text. Kept text.", chunks.get(0).getText());
    String loss = chunks.get(0).getAux().get(PdfFilePreprocessor.AUX_PREP_LOSS);
    assertTrue(loss != null && loss.contains("streams_skipped=3"), String.valueOf(loss));
  }

  @Test
  void whollyUndecodableContentIsAClassifiedFileFailure() throws IOException {
    byte[] pdf = contents(false, 1);
    PreprocessingException failure = assertThrows(PreprocessingException.class, () -> prep(pdf));
    assertTrue(failure.getMessage().startsWith("DECODE_ERROR:"), failure.getMessage());
  }

  @Test
  void wholeContentFailureGoesToPrepErrorSideOutput() throws IOException {
    byte[] pdf = contents(false, 1);
    StructType blobType = (StructType) BlobType.dataType();
    StructType sourceType = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
        DataTypes.createStructField(BlobColumns.BLOB, blobType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, true),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, true)});
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, pdf, null}, blobType);
    Row row = new GenericRowWithSchema(new Object[] {"decode-loss", "pdf", (long) pdf.length, 1234L, blob,
        "s3a://bucket/input.pdf", "input.pdf"}, sourceType);
    TypedProperties props = new TypedProperties();
    PrepCounters counters = PrepCounters.local();
    try (PreprocessingIterator iterator = new PreprocessingIterator(List.of(row).iterator(), EmbeddingTableConfig.from(props), "001",
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), new PreprocessorRegistry(props),
        new WordCountTokenizer(), counters, null)) {
      assertFalse(iterator.hasNext());
      assertEquals(1, iterator.getSideOutputs().size());
      PrepSideOutput failure = iterator.getSideOutputs().get(0);
      assertEquals("decode-loss", failure.getSourcePathSha());
      assertEquals(PrepSideOutput.STAGE_PREP, failure.getStage());
      assertTrue(failure.getError().startsWith("DECODE_ERROR:"), failure.getError());
      assertEquals(1L, counters.getFilesFailed().value());
      assertEquals(0L, counters.getFilesNoText().value());
    }
  }

  @Test
  void whollyUndecodablePageAlongsideReadablePageRecordsLoss() throws IOException {
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      PDPage failed = new PDPage();
      failed.setResources(PdfFixtures.helvetica());
      failed.getCOSObject().setItem(COSName.CONTENTS, broken(document));
      document.addPage(failed);
      PDPage good = new PDPage();
      good.setResources(PdfFixtures.helvetica());
      good.getCOSObject().setItem(COSName.CONTENTS,
          PdfFixtures.stream(document, "BT /F1 10 Tf 50 700 Td (Kept text.) Tj ET\n"));
      document.addPage(good);
      pdf = PdfFixtures.save(document);
    }
    List<Chunk> chunks = prep(pdf);
    assertEquals(1, chunks.size());
    assertEquals("Kept text.", chunks.get(0).getText());
    String loss = chunks.get(0).getAux().get(PdfFilePreprocessor.AUX_PREP_LOSS);
    assertTrue(loss != null && loss.contains("decode_failed=1"), String.valueOf(loss));
    assertFalse(new PdfFilePreprocessor().name().startsWith("pdf-chunker/3;"),
        "durable decode-loss metadata requires an identity beyond inherited C3");
  }

  @Test
  void cleanTextHasNoDecodeLoss() throws IOException {
    List<Chunk> chunks = prep(PdfFixtures.text("Kept text."));
    assertEquals(1, chunks.size());
    assertEquals("Kept text.", chunks.get(0).getText());
    assertFalse(chunks.get(0).getAux().containsKey(PdfFilePreprocessor.AUX_PREP_LOSS));
  }

  @Test
  void imageOnlyContentRemainsEmpty() throws IOException {
    assertTrue(prep(PdfFixtures.imageOnly(1, 30)).isEmpty());
  }

  @Test
  void undecodableContentBesideEmptyStreamIsStillAFailure() throws IOException {
    assertNoTextDecodeFailure(withBrokenContent(PdfFixtures.text("")));
  }

  @Test
  void undecodableContentBesideImageOnlyStreamIsStillAFailure() throws IOException {
    assertNoTextDecodeFailure(withBrokenContent(PdfFixtures.imageOnly(1, 30)));
  }

  @Test
  void drawnUndecodableFormRecordsLossOnEveryPage() throws IOException {
    List<Chunk> chunks = prep(forms(true, true, 3, false));
    assertEquals(1, chunks.size());
    assertEquals("Kept text. Kept text. Kept text.", chunks.get(0).getText());
    String loss = chunks.get(0).getAux().get(PdfFilePreprocessor.AUX_PREP_LOSS);
    assertTrue(loss != null && loss.contains("streams_skipped=3"), String.valueOf(loss));
  }

  @Test
  void drawnUndecodableTransparencyGroupRecordsLoss() throws IOException {
    List<Chunk> chunks = prep(forms(true, true, 1, true));
    assertEquals(1, chunks.size());
    assertEquals("Kept text.", chunks.get(0).getText());
    String loss = chunks.get(0).getAux().get(PdfFilePreprocessor.AUX_PREP_LOSS);
    assertTrue(loss != null && loss.contains("streams_skipped=1"), String.valueOf(loss));
  }

  @Test
  void undrawnUndecodableFormDoesNotReportLoss() throws IOException {
    List<Chunk> chunks = prep(forms(false, true, 1, false));
    assertEquals(1, chunks.size());
    assertEquals("Kept text.", chunks.get(0).getText());
    assertFalse(chunks.get(0).getAux().containsKey(PdfFilePreprocessor.AUX_PREP_LOSS));
  }

  @Test
  void undecodableFormWithNoSurvivingTextIsAClassifiedFailure() throws IOException {
    assertNoTextDecodeFailure(forms(true, false, 1, false));
  }

  private static byte[] forms(boolean drawn, boolean pageText, int pages, boolean transparency) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDFormXObject form = new PDFormXObject(broken(document));
      form.getCOSObject().setItem(COSName.TYPE, COSName.XOBJECT);
      form.getCOSObject().setItem(COSName.SUBTYPE, COSName.FORM);
      form.setBBox(new PDRectangle(612, 792));
      form.setResources(PdfFixtures.helvetica());
      if (transparency) {
        COSDictionary group = new COSDictionary();
        group.setItem(COSName.S, COSName.TRANSPARENCY);
        form.getCOSObject().setItem(COSName.GROUP, group);
      }
      for (int i = 0; i < pages; i++) {
        PDPage page = new PDPage();
        page.setResources(PdfFixtures.helvetica());
        page.getResources().put(COSName.getPDFName("Broken"), form);
        page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document,
            (drawn ? "q /Broken Do Q " : "") + (pageText ? "BT /F1 10 Tf 50 700 Td (Kept text.) Tj ET\n" : "")));
        document.addPage(page);
      }
      return PdfFixtures.save(document);
    }
  }

  private static void assertNoTextDecodeFailure(byte[] pdf) {
    PreprocessingException failure = assertThrows(PreprocessingException.class, () -> prep(pdf));
    assertTrue(failure.getMessage().startsWith("DECODE_ERROR:"), failure.getMessage());
    assertTrue(failure.getMessage().contains("streams_skipped=1"), failure.getMessage());
  }

  private static byte[] withBrokenContent(byte[] pdf) throws IOException {
    try (PDDocument document = Loader.loadPDF(pdf)) {
      PDPage page = document.getPage(0);
      COSArray contents = new COSArray();
      contents.add(broken(document));
      contents.add(page.getCOSObject().getItem(COSName.CONTENTS));
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
      return PdfFixtures.save(document);
    }
  }

  private static List<Chunk> prep(byte[] pdf) {
    List<Chunk> chunks = new ArrayList<>();
    try (ClosableIterator<Chunk> iterator = new PdfFilePreprocessor().open(() -> new ByteArrayInputStream(pdf),
        new FileMeta("decode-loss", "pdf", pdf.length), new WordCountTokenizer(), new TypedProperties())) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    return chunks;
  }

  private static byte[] contents(boolean readable, int pages) throws IOException {
    try (PDDocument document = new PDDocument()) {
      COSStream broken = broken(document);
      for (int i = 0; i < pages; i++) {
        PDPage page = new PDPage();
        page.setResources(PdfFixtures.helvetica());
        COSArray contents = new COSArray();
        contents.add(broken);
        if (readable) {
          contents.add(PdfFixtures.stream(document, "BT /F1 10 Tf 50 700 Td (Kept text.) Tj ET\n"));
        }
        page.getCOSObject().setItem(COSName.CONTENTS, contents);
        document.addPage(page);
      }
      return PdfFixtures.save(document);
    }
  }

  private static COSStream broken(PDDocument document) throws IOException {
    COSStream stream = PdfFixtures.stream(document, "BT /F1 10 Tf 50 650 Td (Lost text.) Tj ET\n");
    stream.setItem(COSName.FILTER, COSName.getPDFName("UnknownDecode"));
    return stream;
  }
}
