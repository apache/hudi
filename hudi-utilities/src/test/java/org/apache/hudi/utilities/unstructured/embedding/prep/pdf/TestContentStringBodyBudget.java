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
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An unfiltered content stream is read as a view on the staged file, but PDFBox's content parser still copies each string
 * and inline-image body onto the heap whole, so one oversized string or inline image is held to the decoded-size budget.
 */
class TestContentStringBodyBudget {

  private static final long BUDGET = 10_000;

  @TempDir
  Path temp;

  /** Content-stream shapes whose body PDFBox's content parser copies into one buffer. */
  enum Body {
    LITERAL_STRING, STRING_IN_ARRAY, HEX_STRING, INLINE_IMAGE;

    String ops(int bytes) {
      String fill = "A".repeat(bytes);
      switch (this) {
        case LITERAL_STRING:
          return "BT /F1 10 Tf 50 700 Td (" + fill + ") Tj ET\n";
        case STRING_IN_ARRAY:
          return "BT /F1 10 Tf 50 700 Td [(" + fill + ")] TJ ET\n";
        case HEX_STRING:
          return "BT /F1 10 Tf 50 700 Td <" + "41".repeat(bytes / 2) + "> Tj ET\n";
        case INLINE_IMAGE:
          return "q BI /W 1 /H 1 /BPC 8 /CS /G ID " + fill + "\nEI\nQ\n";
        default:
          throw new UnsupportedOperationException("no content for " + this);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(Body.class)
  void oversizedBodyInAnUnfilteredPageStreamFailsThePage(Body body) throws IOException {
    try (PDDocument document = Loader.loadPDF(PdfFixtures.contentsStreams(body.ops((int) (2 * BUDGET))))) {
      assertDecodeLimit(document.getPage(1));
    }
  }

  @ParameterizedTest
  @EnumSource(Body.class)
  void oversizedBodyInAnUnfilteredFormFailsThePage(Body body) throws IOException {
    try (PDDocument document = Loader.loadPDF(formPage(body.ops((int) (2 * BUDGET))))) {
      assertDecodeLimit(document.getPage(0));
    }
  }

  @ParameterizedTest
  @EnumSource(Body.class)
  void bodyWithinTheBudgetPasses(Body body) throws IOException {
    try (PDDocument page = Loader.loadPDF(PdfFixtures.contentsStreams(body.ops((int) (BUDGET / 2)), body.ops((int) (BUDGET / 2))));
         PDDocument form = Loader.loadPDF(formPage(body.ops((int) (BUDGET / 2))))) {
      new StreamDecodeGuard(BUDGET, temp.toFile()).checkPage(page.getPage(1));
      new StreamDecodeGuard(BUDGET, temp.toFile()).checkPage(form.getPage(0));
    }
  }

  private void assertDecodeLimit(PDPage page) {
    StreamDecodeGuard guard = new StreamDecodeGuard(BUDGET, temp.toFile());
    PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
    assertTrue(failure.getMessage().contains(String.valueOf(BUDGET)), failure.getMessage());
  }

  /** One page that draws an unfiltered form whose content is {@code formOps}. */
  private static byte[] formPage(String formOps) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDResources resources = PdfFixtures.helvetica();
      COSStream form = PdfFixtures.stream(document, formOps);
      form.setItem(COSName.TYPE, COSName.XOBJECT);
      form.setItem(COSName.SUBTYPE, COSName.FORM);
      COSArray box = new COSArray();
      for (int v : new int[] {0, 0, 612, 792}) {
        box.add(COSInteger.get(v));
      }
      form.setItem(COSName.BBOX, box);
      form.setItem(COSName.RESOURCES, PdfFixtures.helvetica().getCOSObject());
      COSDictionary xobjects = new COSDictionary();
      xobjects.setItem(COSName.getPDFName("X1"), form);
      resources.getCOSObject().setItem(COSName.XOBJECT, xobjects);
      PDPage page = new PDPage();
      page.setResources(resources);
      page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document, "q /X1 Do Q\n"));
      document.addPage(page);
      return PdfFixtures.save(document);
    }
  }
}
