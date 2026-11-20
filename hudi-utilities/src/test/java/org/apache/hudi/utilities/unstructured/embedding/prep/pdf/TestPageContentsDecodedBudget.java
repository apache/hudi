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
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdfwriter.compress.CompressParameters;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The decoded-size budget bounds the sum of a page's filtered /Contents streams, a repeated stream counted per listing. */
class TestPageContentsDecodedBudget {

  private static final long BUDGET = 1000;

  @TempDir
  Path temp;

  @ParameterizedTest
  @ValueSource(ints = {2, 1000})
  void repeatedStreamAboveTheBudgetTogetherIsRefused(int listings) throws Exception {
    try (PDDocument document = reload(600, COSName.FLATE_DECODE, listings)) {
      StreamDecodeGuard guard = guard();
      PDPage page = document.getPage(0);
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("together"), failure.getMessage());
    }
  }

  @Test
  void repeatedStreamAtTheBudgetTogetherPasses() throws Exception {
    try (PDDocument document = reload(500, COSName.FLATE_DECODE, 2)) {
      guard().checkPage(document.getPage(0));
    }
  }

  @Test
  void oneStreamAtTheBudgetPasses() throws Exception {
    try (PDDocument document = reload(1000, COSName.FLATE_DECODE, 1)) {
      guard().checkPage(document.getPage(0));
    }
  }

  @Test
  void unfilteredStreamsAreNotSummed() throws Exception {
    // PDFBox reads an unfiltered stream as a view on the file, not a heap copy
    try (PDDocument document = reload(600, null, 10)) {
      guard().checkPage(document.getPage(0));
    }
  }

  private StreamDecodeGuard guard() {
    return new StreamDecodeGuard(BUDGET, temp.toFile());
  }

  /** A saved and reloaded one-page file whose /Contents array lists one stream of {@code bytes} spaces {@code listings} times. */
  private PDDocument reload(int bytes, COSName filter, int listings) throws Exception {
    Path file = temp.resolve("contents-" + bytes + "-" + listings + ".pdf");
    try (PDDocument document = new PDDocument()) {
      COSStream stream = document.getDocument().createCOSStream();
      byte[] spaces = new byte[bytes];
      Arrays.fill(spaces, (byte) ' ');
      try (OutputStream out = filter == null ? stream.createOutputStream() : stream.createOutputStream(filter)) {
        out.write(spaces);
      }
      COSArray contents = new COSArray();
      for (int i = 0; i < listings; i++) {
        contents.add(stream);
      }
      PDPage page = new PDPage();
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
      document.addPage(page);
      document.save(file.toFile(), CompressParameters.NO_COMPRESSION);
    }
    return Loader.loadPDF(file.toFile());
  }
}
