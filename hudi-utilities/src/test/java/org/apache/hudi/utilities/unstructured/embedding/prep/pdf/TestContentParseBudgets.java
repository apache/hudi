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

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSFloat;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The structure budgets of the decode guard (operands pending at an operator, elements of one array or dictionary, bytes
 * of the page content) hold on the content PDFBox's own parser reads, not only on the guard's own decoding of it: a
 * /Contents stream PDFBox reads differently (a single Flate stream with a /Predictor, which PDPage parses without the
 * predictor; token sequences PDFBox splits differently from the guard's pre-scan) cannot carry an operand list or an
 * array past the budget. Pages within the budgets, however they are encoded, are still accepted.
 */
class TestContentParseBudgets {

  /** The guard's decoded-size budget in these tests. */
  private static final long BUDGET = 1L << 20;
  /** More array elements than StreamStructureScanner.MAX_OPERANDS allows. */
  private static final int LARGE_ARRAY = StreamStructureScanner.MAX_OPERANDS + 10_000;
  private static final String TEXT = "BT /F1 10 Tf 50 700 Td (Page text.) Tj ET\n";

  @TempDir
  Path staging;

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  // ------------------------------------------------------------------ single Flate /Contents with a predictor

  @Test
  void largeArrayInAPredictedFlateStreamIsRefused() throws IOException {
    // the guard's decoder applies /Predictor 12 and sees one long number; PDPage parses the stream unpredicted and builds
    // the array
    byte[] page = content(array(LARGE_ARRAY) + " pop\n", true);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @Test
  void largeArrayInAPlainFlateStreamIsRefused() throws IOException {
    byte[] page = content(array(LARGE_ARRAY) + " pop\n", false);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @Test
  void predictedFlateStreamReadPastTheBudgetIsRefused() throws IOException {
    // the guard's PNG decoder turns each two-byte row (tag byte + one byte) into one byte, so the stream decodes to half
    // of what PDFBox reads unpredicted: 1.5x the budget as PDFBox reads it, 0.75x as the guard decodes it
    StringBuilder ops = new StringBuilder();
    while (ops.length() < BUDGET * 3 / 2) {
      ops.append("0 0 m\n");
    }
    byte[] page = content(ops.toString(), true);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  // ------------------------------------------------------------------ token sequences the pre-scan reads differently

  /** Prefixes after which the pre-scan is inside a string or inline image body while PDFBox reads ordinary tokens. */
  @ParameterizedTest
  @ValueSource(strings = {"(x\\)\n/ ", "IDx ( EI Q ", "ID( EI Q "})
  void largeArrayAfterAPrefixThePreScanReadsDifferentlyIsRefused(String prefix) throws IOException {
    byte[] page = content(prefix + array(LARGE_ARRAY) + " pop\n", false);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @Test
  void manyOperandsPendingAfterAPrefixAreRefused() throws IOException {
    StringBuilder ops = new StringBuilder("(x\\)\n/ ");
    for (int i = 0; i < LARGE_ARRAY; i++) {
      ops.append("1 ");
    }
    byte[] page = content(ops + "n\n", false);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  // ------------------------------------------------------------------ forms and Type3 glyph procedures

  @Test
  void formTheParseRefusesIsRefusedAgainOnTheNextPage() throws IOException {
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      COSStream form = flate(document, "(x\\)\n/ " + array(LARGE_ARRAY) + " pop\n", false);
      form.setItem(COSName.TYPE, COSName.XOBJECT);
      form.setItem(COSName.SUBTYPE, COSName.FORM);
      form.setItem(COSName.BBOX, new PDRectangle(0, 0, 612, 792).getCOSArray());
      for (int p = 0; p < 2; p++) {
        PDPage page = new PDPage();
        page.setResources(PdfFixtures.helvetica());
        page.getResources().getCOSObject().setItem(COSName.XOBJECT, single(COSName.getPDFName("Fm"), form));
        page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document, "/Fm Do\n" + TEXT));
        document.addPage(page);
      }
      pdf = save(document);
    }
    try (PDDocument document = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(BUDGET, staging.toFile());
      PDPage first = document.getPage(0);
      assertEquals(PdfFailureCode.DECODE_LIMIT,
          assertThrows(PageFailure.class, () -> guard.checkPage(first, 1)).code());
      PDPage second = document.getPage(1);
      PageFailure again = assertThrows(PageFailure.class, () -> guard.checkPage(second, 2));
      assertTrue(again.getMessage().contains("already failed"), again.getMessage());
    }
  }

  /**
   * PDFBox parses a Type3 glyph procedure during text extraction when /Widths lacks the code (the glyph width comes from
   * the procedure's d0/d1) and when /FontBBox is empty (each procedure's glyph box); the budgets hold there too.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void type3GlyphProcedureWithALargeArrayIsRefused(boolean emptyFontBox) throws IOException {
    String proc = "(x\\)\n/ " + array(LARGE_ARRAY) + " 500 0 d0\n";
    byte[] page = type3Page(proc, emptyFontBox);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @Test
  void validType3FontIsAccepted() throws IOException {
    byte[] page = type3Page("500 0 d0 0 0 m 400 400 l S\n", true);
    assertDoesNotThrow(() -> check(page));
  }

  // ------------------------------------------------------------------ pages within the budgets

  @Test
  void arrayJustUnderTheBudgetIsAccepted() throws IOException {
    for (boolean predictor : new boolean[] {false, true}) {
      byte[] page = content(array(StreamStructureScanner.MAX_OPERANDS - 16) + " pop\n" + TEXT, predictor);
      assertDoesNotThrow(() -> check(page), "predictor " + predictor);
    }
  }

  @Test
  void manyOperatorsWithFewOperandsEachAreAccepted() throws IOException {
    StringBuilder ops = new StringBuilder();
    for (int i = 0; i < LARGE_ARRAY; i++) {
      ops.append("1 0 0 1 0 0 cm\n");
    }
    // about 1.2 MB: the stream budget is raised to hold it, the per-operator budgets are what this exercises
    byte[] page = content(ops + TEXT, false);
    assertDoesNotThrow(() -> check(page, 4 * BUDGET));
  }

  @Test
  void predictedFlateTextPageWithinTheBudgetIsAccepted() throws IOException {
    for (boolean predictor : new boolean[] {false, true}) {
      byte[] page = content(TEXT + "q 1 0 0 1 10 10 cm [(a) 120 (b)] TJ Q\n", predictor);
      assertDoesNotThrow(() -> check(page), "predictor " + predictor);
    }
  }

  @Test
  void escapedParenthesesAndInlineImagesInOrdinaryTextAreAccepted() throws IOException {
    String ops = "BT /F1 10 Tf 50 700 Td (f\\(x\\) = \\(y\\)) Tj ET\n"
        + "q 10 0 0 10 100 100 cm BI /W 2 /H 1 /BPC 8 /CS /G ID \u0000ÿ EI Q\n"
        + TEXT;
    byte[] page = content(ops, false);
    assertDoesNotThrow(() -> check(page));
  }

  /** Each string fits the budget, but PDFBox keeps the entire operand batch until its operator. */
  @ParameterizedTest
  @ValueSource(strings = {"array", "dictionary", "nested", "pending"})
  public void aggregateOperandBytesAreRefusedBeforeMaterialization(String kind) throws IOException {
    String item = "(" + "x".repeat(3072) + ") ";
    String operands;
    switch (kind) {
      case "array":
        operands = "[" + item.repeat(4) + "] ";
        break;
      case "dictionary":
        operands = "<< /A " + item + "/B " + item + "/C " + item + "/D " + item + ">> ";
        break;
      case "nested":
        operands = "[[" + item + "][" + item + "][" + item + "][" + item + "]] ";
        break;
      default:
        operands = item.repeat(4);
    }
    byte[] pdf = unfilteredContent(operands + "pop\n" + TEXT);
    PageFailure failure = assertThrows(PageFailure.class, () -> check(pdf, 4096));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @Test
  public void unfilteredStreamMayExceedTheBudgetWithSmallIndependentOperandBatches() throws IOException {
    String item = "(" + "x".repeat(1536) + ") ";
    String ops = "%" + "comment".repeat(4096) + "\n" + " ".repeat(16384)
        + ("[" + item.repeat(2) + "] TJ\n").repeat(4) + TEXT;
    assertDoesNotThrow(() -> check(unfilteredContent(ops), 4096));
  }

  private static byte[] unfilteredContent(String ops) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      document.addPage(page);
      page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document, ops));
      return save(document);
    }
  }

  // ------------------------------------------------------------------ helpers

  private static String array(int elements) {
    StringBuilder sb = new StringBuilder(elements * 2 + 4);
    sb.append('[');
    for (int i = 0; i < elements; i++) {
      sb.append("9 ");
    }
    return sb.append(']').toString();
  }

  /** A one-page PDF whose single /Contents stream is {@code ops}, Flate encoded, with a PNG Up predictor when asked. */
  private static byte[] content(String ops, boolean predictor) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      document.addPage(page);
      page.getCOSObject().setItem(COSName.CONTENTS, flate(document, ops, predictor));
      return save(document);
    }
  }

  /** {@code ops} as a Flate stream, with a PNG Up predictor when asked. */
  private static COSStream flate(PDDocument document, String ops, boolean predictor) throws IOException {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream out = new DeflaterOutputStream(stream.createRawOutputStream(), new Deflater(9))) {
      out.write(ops.getBytes(StandardCharsets.ISO_8859_1));
    }
    stream.setItem(COSName.FILTER, COSName.FLATE_DECODE);
    if (predictor) {
      COSDictionary params = new COSDictionary();
      params.setInt(COSName.PREDICTOR, 12);
      params.setInt(COSName.COLUMNS, 1);
      stream.setItem(COSName.DECODE_PARMS, params);
    }
    return stream;
  }

  /**
   * A page showing "a" in a Type3 font whose only glyph procedure is {@code proc}; no /Widths, and an empty /FontBBox when
   * asked, so PDFBox reads the procedure for the width and the glyph box.
   */
  private static byte[] type3Page(String proc, boolean emptyFontBox) throws IOException {
    try (PDDocument document = new PDDocument()) {
      COSDictionary font = new COSDictionary();
      font.setItem(COSName.TYPE, COSName.FONT);
      font.setItem(COSName.SUBTYPE, COSName.TYPE3);
      font.setItem(COSName.FONT_BBOX, emptyFontBox ? new PDRectangle(0, 0, 0, 0).getCOSArray()
          : new PDRectangle(0, 0, 1000, 1000).getCOSArray());
      COSArray matrix = new COSArray();
      for (float v : new float[] {0.001f, 0, 0, 0.001f, 0, 0}) {
        matrix.add(new COSFloat(v));
      }
      font.setItem(COSName.FONT_MATRIX, matrix);
      font.setItem(COSName.CHAR_PROCS, single(COSName.getPDFName("a"), flate(document, proc, false)));
      COSDictionary encoding = new COSDictionary();
      COSArray differences = new COSArray();
      differences.add(COSInteger.get(97));
      differences.add(COSName.getPDFName("a"));
      encoding.setItem(COSName.DIFFERENCES, differences);
      font.setItem(COSName.ENCODING, encoding);
      font.setInt(COSName.FIRST_CHAR, 97);
      font.setInt(COSName.LAST_CHAR, 97);
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      page.getResources().getCOSObject().setItem(COSName.FONT, single(COSName.getPDFName("T3"), font));
      page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document, "BT /T3 12 Tf 50 650 Td (a) Tj ET\n"));
      document.addPage(page);
      return save(document);
    }
  }

  private static COSDictionary single(COSName key, COSBase value) {
    COSDictionary dictionary = new COSDictionary();
    dictionary.setItem(key, value);
    return dictionary;
  }

  private static byte[] save(PDDocument document) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    document.save(bytes);
    return bytes.toByteArray();
  }

  private void check(byte[] pdf) throws IOException {
    check(pdf, BUDGET);
  }

  private void check(byte[] pdf, long budget) throws IOException {
    try (PDDocument document = Loader.loadPDF(pdf)) {
      new StreamDecodeGuard(budget, staging.toFile()).checkPage(document.getPage(0), 1);
    }
  }
}
