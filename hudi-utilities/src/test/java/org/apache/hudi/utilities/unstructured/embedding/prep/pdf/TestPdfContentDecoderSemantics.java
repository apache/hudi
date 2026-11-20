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

import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdfparser.PDFStreamParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The independent oracle is PDFBox's actual page parser, with no resource resolution. */
class TestPdfContentDecoderSemantics {
  @TempDir
  Path staging;

  @ParameterizedTest
  @CsvSource({"false,false,true", "true,false,false", "false,true,false", "true,true,false"})
  void guardMatchesThePageParserForScalarAndArrayFilters(boolean contentArray, boolean filterArray,
                                                        boolean exceedsOperands) throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream stream = predictedContent(document,
          "1 ".repeat(StreamStructureScanner.MAX_OPERANDS + 1) + "n\n", filterArray, 2);
      PDPage page = page(document, stream, contentArray);
      int actualMaximum = maximumPendingOperands(page);
      StreamDecodeGuard guard = new StreamDecodeGuard(1024 * 1024, staging.toFile());
      if (exceedsOperands) {
        assertEquals(StreamStructureScanner.MAX_OPERANDS + 1, actualMaximum);
        assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(page)).code());
      } else {
        assertTrue(actualMaximum < StreamStructureScanner.MAX_OPERANDS, Integer.toString(actualMaximum));
        assertDoesNotThrow(() -> guard.checkPage(page));
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true", "true,true"})
  void ordinaryFlateContentPassesForEveryPageRepresentation(boolean contentArray, boolean filterArray) throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream stream = predictedContent(document, "1 2 3 n\n", filterArray, 2);
      stream.removeItem(COSName.DECODE_PARMS);
      PDPage page = page(document, stream, contentArray);
      assertEquals(3, maximumPendingOperands(page));
      assertDoesNotThrow(() -> new StreamDecodeGuard(1024, staging.toFile()).checkPage(page));
    }
  }

  @Test
  void singleFlateDecoderDoesNotAllocateOrApplyPredictorRows() throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream stream = predictedContent(document, "1 2 3 n\n", false, StreamDecodeGuard.MAX_PREDICTOR_ROW_BYTES + 1);
      PDPage single = page(document, stream, false);
      assertEquals(3, maximumPendingOperands(single));
      StreamDecodeGuard guard = new StreamDecodeGuard(1024, staging.toFile());
      assertDoesNotThrow(() -> guard.checkPage(single));
      PDPage array = page(document, stream, true);
      assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(array)).code());
      assertDoesNotThrow(() -> guard.checkPage(single), "an array-decoder refusal does not poison the streaming decoder");
    }
  }

  @Test
  void streamingStructureRefusalDoesNotPoisonArrayDecoding() throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream stream = predictedContent(document,
          "1 ".repeat(StreamStructureScanner.MAX_OPERANDS + 1) + "n\n", false, 2);
      PDPage single = page(document, stream, false);
      PDPage array = page(document, stream, true);
      StreamDecodeGuard guard = new StreamDecodeGuard(1024 * 1024, staging.toFile());
      assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(single)).code());
      assertTrue(maximumPendingOperands(array) < StreamStructureScanner.MAX_OPERANDS);
      assertDoesNotThrow(() -> guard.checkPage(array));
    }
  }

  private static COSStream predictedContent(PDDocument document, String ops, boolean filterArray, int columns)
      throws IOException {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream out = stream.createOutputStream(COSName.FLATE_DECODE)) {
      out.write(ops.getBytes(StandardCharsets.US_ASCII));
    }
    COSDictionary params = new COSDictionary();
    params.setInt(COSName.PREDICTOR, 2);
    params.setInt(COSName.COLUMNS, columns);
    params.setInt(COSName.COLORS, 1);
    params.setInt(COSName.BITS_PER_COMPONENT, 8);
    if (filterArray) {
      COSArray filters = new COSArray();
      filters.add(COSName.FLATE_DECODE);
      stream.setItem(COSName.FILTER, filters);
      COSArray allParams = new COSArray();
      allParams.add(params);
      stream.setItem(COSName.DECODE_PARMS, allParams);
    } else {
      stream.setItem(COSName.DECODE_PARMS, params);
    }
    return stream;
  }

  private static PDPage page(PDDocument document, COSStream content, boolean array) {
    PDPage page = new PDPage();
    document.addPage(page);
    if (array) {
      COSArray contents = new COSArray();
      contents.add(content);
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
    } else {
      page.getCOSObject().setItem(COSName.CONTENTS, content);
    }
    return page;
  }

  private static int maximumPendingOperands(PDPage page) throws IOException {
    PDFStreamParser parser = new PDFStreamParser(page);
    int pending = 0;
    int maximum = 0;
    try {
      for (Object token = parser.parseNextToken(); token != null; token = parser.parseNextToken()) {
        if (token instanceof Operator) {
          pending = 0;
        } else {
          maximum = Math.max(maximum, ++pending);
        }
      }
    } finally {
      parser.close();
    }
    return maximum;
  }
}
