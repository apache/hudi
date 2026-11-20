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
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A content or form stream that fails to decode for a reason other than its size is skipped, as PDFBox skips it: the page
 * keeps the text of its other streams instead of failing.
 */
class TestUndecodableContentStreams {

  private static final COSName UNKNOWN_FILTER = COSName.getPDFName("UnknownDecode");

  @TempDir
  Path staging;

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @Test
  void contentsArrayKeepsTheStreamsThatDecode() throws IOException {
    byte[] pdf = build(document -> {
      PDPage page = textPage(document, null);
      COSStream broken = PdfFixtures.stream(document, "BT /F1 10 Tf 50 650 Td (Lost text.) Tj ET\n");
      broken.setItem(COSName.FILTER, UNKNOWN_FILTER);
      COSArray contents = new COSArray();
      contents.add(PdfFixtures.stream(document, "BT /F1 10 Tf 50 700 Td (Kept text.) Tj ET\n"));
      contents.add(broken);
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
    });
    assertEquals(List.of("Kept text."), extract(pdf));
  }

  @Test
  void streamThatFailsMidDecodeLeavesNoStateForTheNextStream() throws IOException {
    // the broken stream decodes 4 KiB of an open array before its invalid byte; the next stream holds more numbers than
    // one array may, each group closed by an operator, so it passes only when scanned without the broken stream's bytes
    StringBuilder openArray = new StringBuilder("[ ");
    while (openArray.length() < 6000) {
      openArray.append("1 ");
    }
    StringBuilder operators = new StringBuilder();
    for (int i = 0; i < 12_000; i++) {
      operators.append("1 0 0 1 0 0 cm\n");
    }
    byte[] pdf = build(document -> {
      PDPage page = textPage(document, null);
      COSStream broken = rawStream(document, ascii85WithInvalidByte(openArray.toString().getBytes(StandardCharsets.US_ASCII)));
      broken.setItem(COSName.FILTER, COSName.ASCII85_DECODE);
      COSArray contents = new COSArray();
      contents.add(broken);
      contents.add(PdfFixtures.stream(document, operators + "BT /F1 10 Tf 50 700 Td (Kept text.) Tj ET\n"));
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
    });
    assertEquals(List.of("Kept text."), extract(pdf));
  }

  @Test
  void undrawnBrokenFormInInheritedResourcesFailsNoPage() throws IOException {
    byte[] pdf = build(document -> {
      PDResources inherited = PdfFixtures.helvetica();
      COSStream form = PdfFixtures.stream(document, "BT /F1 10 Tf 50 600 Td (Form text.) Tj ET\n");
      form.setItem(COSName.FILTER, UNKNOWN_FILTER);
      form.setItem(COSName.TYPE, COSName.XOBJECT);
      form.setItem(COSName.SUBTYPE, COSName.FORM);
      COSArray box = new COSArray();
      for (int v : new int[] {0, 0, 612, 792}) {
        box.add(COSInteger.get(v));
      }
      form.setItem(COSName.BBOX, box);
      COSDictionary xobjects = new COSDictionary();
      xobjects.setItem(COSName.getPDFName("Broken"), form);
      inherited.getCOSObject().setItem(COSName.XOBJECT, xobjects);
      for (int p = 1; p <= 3; p++) {
        PDPage page = textPage(document, "Page " + p + " text.");
        page.getCOSObject().removeItem(COSName.RESOURCES);
      }
      document.getPages().getCOSObject().setItem(COSName.RESOURCES, inherited.getCOSObject());
    });
    assertEquals(List.of("Page 1 text.", "Page 2 text.", "Page 3 text."), extract(pdf));
  }

  @Test
  void drawnBrokenFormIsSkippedAndThePageKeepsItsText() throws IOException {
    byte[] pdf = build(document -> {
      PDPage page = textPage(document, null);
      COSStream form = PdfFixtures.stream(document, "BT /F1 10 Tf 50 600 Td (Form text.) Tj ET\n");
      form.setItem(COSName.FILTER, UNKNOWN_FILTER);
      form.setItem(COSName.TYPE, COSName.XOBJECT);
      form.setItem(COSName.SUBTYPE, COSName.FORM);
      COSArray box = new COSArray();
      for (int v : new int[] {0, 0, 612, 792}) {
        box.add(COSInteger.get(v));
      }
      form.setItem(COSName.BBOX, box);
      COSDictionary xobjects = new COSDictionary();
      xobjects.setItem(COSName.getPDFName("Broken"), form);
      page.getResources().getCOSObject().setItem(COSName.XOBJECT, xobjects);
      page.getCOSObject().setItem(COSName.CONTENTS,
          PdfFixtures.stream(document, "q /Broken Do Q BT /F1 10 Tf 50 700 Td (Kept text.) Tj ET\n"));
    });
    assertEquals(List.of("Kept text."), extract(pdf));
  }

  @Test
  void undecodableStreamIsDecodedOncePerDocument() throws IOException {
    byte[] pdf = build(document -> {
      COSStream broken = PdfFixtures.stream(document, "BT /F1 10 Tf 50 650 Td (Lost text.) Tj ET\n");
      broken.setItem(COSName.FILTER, UNKNOWN_FILTER);
      for (int p = 1; p <= 3; p++) {
        PDPage page = textPage(document, null);
        COSArray contents = new COSArray();
        contents.add(broken);
        contents.add(PdfFixtures.stream(document, "BT /F1 10 Tf 50 700 Td (Page " + p + " text.) Tj ET\n"));
        page.getCOSObject().setItem(COSName.CONTENTS, contents);
      }
    });
    try (PDDocument document = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(1_000_000, staging.toFile());
      guard.checkPage(document.getPage(0));
      long first = guard.decodedStreams();
      guard.checkPage(document.getPage(1));
      guard.checkPage(document.getPage(2));
      assertEquals(first + 2, guard.decodedStreams(), "pages 2-3 decode only their own text stream");
    }
  }

  /** The text of every page, through the guarded page source. */
  private List<String> extract(byte[] pdf) throws IOException {
    PdfChunkerConfig config = PdfChunkerConfig.from(new TypedProperties(), new WordCountTokenizer());
    List<String> pages = new ArrayList<>();
    try (PdfBoxPageSource source = new PdfBoxPageSource(() -> new ByteArrayInputStream(pdf),
        new PdfFileMeta("source_path_sha", "test.pdf", pdf.length), config, staging.toFile(), true)) {
      source.open();
      while (source.hasMorePages()) {
        pages.add(source.next().text().trim());
      }
    }
    return pages;
  }

  private interface Builder {
    void build(PDDocument document) throws IOException;
  }

  private static byte[] build(Builder builder) throws IOException {
    try (PDDocument document = new PDDocument()) {
      builder.build(document);
      return PdfFixtures.save(document);
    }
  }

  /** A page with Helvetica as /F1 and, unless {@code text} is null, one content stream showing {@code text}. */
  private static PDPage textPage(PDDocument document, String text) throws IOException {
    PDPage page = new PDPage();
    page.setResources(PdfFixtures.helvetica());
    if (text != null) {
      page.getCOSObject().setItem(COSName.CONTENTS,
          PdfFixtures.stream(document, "BT /F1 10 Tf 50 700 Td (" + text + ") Tj ET\n"));
    }
    document.addPage(page);
    return page;
  }

  private static COSStream rawStream(PDDocument document, byte[] bytes) throws IOException {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream out = stream.createRawOutputStream()) {
      out.write(bytes);
    }
    return stream;
  }

  /** {@code data} in ASCII85, then a five-char group whose last char is outside the ASCII85 alphabet, then the end marker. */
  private static byte[] ascii85WithInvalidByte(byte[] data) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (int i = 0; i + 4 <= data.length; i += 4) {
      long value = ((data[i] & 0xFFL) << 24) | ((data[i + 1] & 0xFFL) << 16) | ((data[i + 2] & 0xFFL) << 8) | (data[i + 3] & 0xFFL);
      byte[] group = new byte[5];
      for (int k = 4; k >= 0; k--) {
        group[k] = (byte) ('!' + value % 85);
        value /= 85;
      }
      out.write(group, 0, 5);
    }
    for (int k = 0; k < 4; k++) {
      out.write('!');
    }
    out.write(0x7F);
    out.write('~');
    out.write('>');
    return out.toByteArray();
  }
}
