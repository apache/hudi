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
import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSNumber;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdfparser.PDFStreamParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Inline-image boundaries agree with the consuming PDFBox parser. */
class TestScannerInlineImages {

  @TempDir
  Path temp;

  @ParameterizedTest
  @ValueSource(strings = {"xEI ", "x EI ", "xEI\r", "xEI\n"})
  void operandsAfterImageRespectTheExistingCap(String ending) throws Exception {
    String content = "q BI /W 1 /H 1 /BPC 8 /CS /G ID " + ending + "Q\n" + "1 ".repeat(65537) + "n\n";
    assertEquals(65537, maximumPendingNumbers(content));
    StreamStructureScanner.Refusal failure = assertThrows(StreamStructureScanner.Refusal.class, () -> scan(content));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
  }

  @ParameterizedTest
  @ValueSource(strings = {"xEI ", "x EI ", "xEI\r", "xEI\n"})
  void operandsAtTheCapPassAfterImage(String ending) throws Exception {
    String content = "q BI /W 1 /H 1 /BPC 8 /CS /G ID " + ending + "Q\n" + "1 ".repeat(65536) + "n\n";
    assertEquals(65536, maximumPendingNumbers(content));
    scan(content);
  }

  @ParameterizedTest
  @ValueSource(strings = {"\001 ", "\377 ", "\f ", "unexpected ", "Q \001 ", "Q \377 ", "longtokenvalue"})
  void embeddedEiWithBinaryOrUnacceptedFollowingBytesStaysImageData(String following) throws Exception {
    String image = "x EI " + following + "1 ".repeat(65537);
    String content = "q BI /W 1 /H 1 /BPC 8 /CS /G ID " + image + "EI Q\n1 n\n";
    assertEquals(1, maximumPendingNumbers(content));
    PDFStreamParser parser = new PDFStreamParser(content.getBytes(StandardCharsets.ISO_8859_1));
    try {
      assertEquals("q", ((Operator) parser.parseNextToken()).getName());
      Operator parsedImage = (Operator) parser.parseNextToken();
      assertEquals("BI", parsedImage.getName());
      assertArrayEquals(image.getBytes(StandardCharsets.ISO_8859_1), parsedImage.getImageData());
    } finally {
      parser.close();
    }
    scan(content);
  }

  @Test
  void inlineImageLookaheadContinuesAcrossContentsStreams() throws Exception {
    String first = "q BI /W 1 /H 1 /BPC 8 /CS /G ID x EI Q ";
    String second = "\001 " + "1 ".repeat(65537) + "EI Q\n1 n\n";
    byte[] pdf = PdfFixtures.contentsStreams(first, second);
    try (PDDocument document = Loader.loadPDF(pdf)) {
      PDFStreamParser parser = new PDFStreamParser(document.getPage(1));
      try {
        assertEquals("q", ((Operator) parser.parseNextToken()).getName());
        Operator image = (Operator) parser.parseNextToken();
        assertEquals("BI", image.getName());
        assertArrayEquals(("x EI Q \n" + "\001 " + "1 ".repeat(65537)).getBytes(StandardCharsets.ISO_8859_1), image.getImageData());
        assertEquals("Q", ((Operator) parser.parseNextToken()).getName());
        assertEquals(1, ((COSNumber) parser.parseNextToken()).intValue());
      } finally {
        parser.close();
      }
      new StreamDecodeGuard(33554432, temp.toFile()).checkPage(document.getPage(1));
    }
  }

  @Test
  void formRefusalDuringFinalTokenIsCachedBeforeAnotherPage() throws Exception {
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      COSStream form = PdfFixtures.stream(document, "1 ".repeat(65536) + "1");
      form.setItem(COSName.SUBTYPE, COSName.FORM);
      form.setItem(COSName.RESOURCES, new PDResources());
      COSDictionary forms = new COSDictionary();
      forms.setItem(COSName.getPDFName("F"), form);
      PDResources resources = new PDResources();
      resources.getCOSObject().setItem(COSName.XOBJECT, forms);
      for (int i = 0; i < 2; i++) {
        PDPage page = new PDPage();
        page.setResources(resources);
        page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document, "/F Do\n"));
        document.addPage(page);
      }
      pdf = PdfFixtures.save(document);
    }
    try (PDDocument document = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(33554432, temp.toFile());
      PDPage first = document.getPage(0);
      PDPage second = document.getPage(1);
      assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(first)).code());
      assertEquals(2, guard.decodedStreams(), "the page's own content stream, then the form");
      assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(second)).code());
      assertEquals(3, guard.decodedStreams(), "only page 2's content stream: the failed final scan was cached, not accepted "
          + "as a passed form");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"Q", "EMC", "S", "1", "123456789012345"})
  void acceptedFollowingTokensResumeOrdinaryOperands(String next) throws Exception {
    String content = "q BI /W 1 /H 1 /BPC 8 /CS /G ID xEI " + next + " n\n" + "1 ".repeat(65537) + "n\n";
    assertEquals(65537, maximumPendingNumbers(content));
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(content));
  }

  @ParameterizedTest
  @ValueSource(strings = {"\t", "\f", "\000"})
  void onlySpaceOrReturnImmediatelyAfterEiCanEndImage(String separator) throws Exception {
    String image = "xEI" + separator + "1 ".repeat(65537);
    String content = "q BI /W 1 /H 1 /BPC 8 /CS /G ID " + image + "EI Q\n1 n\n";
    assertEquals(1, maximumPendingNumbers(content));
    scan(content);
  }

  private static int maximumPendingNumbers(String content) throws Exception {
    PDFStreamParser parser = new PDFStreamParser(content.getBytes(StandardCharsets.ISO_8859_1));
    int pending = 0;
    int maximum = 0;
    try {
      Object token;
      while ((token = parser.parseNextToken()) != null) {
        if (token instanceof Operator) {
          pending = 0;
        } else if (token instanceof COSNumber) {
          maximum = Math.max(maximum, ++pending);
        }
      }
    } finally {
      parser.close();
    }
    System.out.println("inline-image parser maximum pending numbers=" + maximum);
    return maximum;
  }

  private static void scan(String content) {
    try (StreamStructureScanner.Operands scanner = new StreamStructureScanner.Operands("inline-image test content", Long.MAX_VALUE)) {
      byte[] bytes = content.getBytes(StandardCharsets.ISO_8859_1);
      for (byte value : bytes) {
        scanner.write(value);
      }
    }
  }
}
