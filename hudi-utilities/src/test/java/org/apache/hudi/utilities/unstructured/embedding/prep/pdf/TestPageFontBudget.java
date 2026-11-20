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
import org.apache.pdfbox.pdmodel.font.PDTrueTypeFont;
import org.apache.pdfbox.pdmodel.font.encoding.WinAnsiEncoding;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Admission tests use real streams and byte-scale limits. */
class TestPageFontBudget {
  @TempDir
  Path temp;

  @Test
  void refusesTwoLargeFontCopiesBelowTheStreamCap() throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("First"), font(stream(document, 31)));
      fonts.setItem(COSName.getPDFName("Second"), font(stream(document, 31)));
      PDPage page = page(fonts);
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 48, temp.toFile());
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("48"), failure.getMessage());
    }
  }

  @Test
  void chargesDistinctFontDictionariesSharingOneLargeStream() throws Exception {
    Path file = temp.resolve("shared-stream.pdf");
    try (PDDocument document = new PDDocument()) {
      COSStream shared = stream(document, 31);
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("First"), font(shared));
      fonts.setItem(COSName.getPDFName("Second"), font(shared));
      document.addPage(page(fonts));
      document.save(file.toFile());
    }
    try (PDDocument loaded = Loader.loadPDF(file.toFile())) {
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 48, temp.toFile());
      PDPage page = loaded.getPage(0);
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("budget=48"), failure.getMessage());
      System.out.println("SHARED_STREAM_REFUSED " + failure.getMessage());
    }
  }

  @Test
  void distinctFontDictionariesKeepSeparatePublicFontObjects() throws Exception {
    byte[] bytes;
    try (InputStream source = PDTrueTypeFont.class.getResourceAsStream(
        "/org/apache/pdfbox/resources/ttf/LiberationSans-Regular.ttf")) {
      assertNotNull(source);
      bytes = source.readAllBytes();
    }
    Path file = temp.resolve("shared-small-font.pdf");
    try (PDDocument document = new PDDocument()) {
      PDTrueTypeFont first = PDTrueTypeFont.load(document, new ByteArrayInputStream(bytes), WinAnsiEncoding.INSTANCE);
      PDTrueTypeFont second = PDTrueTypeFont.load(document, new ByteArrayInputStream(bytes), WinAnsiEncoding.INSTANCE);
      second.getFontDescriptor().setFontFile2(first.getFontDescriptor().getFontFile2());
      PDResources resources = new PDResources();
      resources.put(COSName.getPDFName("First"), first);
      resources.put(COSName.getPDFName("Second"), second);
      PDPage page = new PDPage();
      page.setResources(resources);
      document.addPage(page);
      document.save(file.toFile());
    }
    try (PDDocument loaded = Loader.loadPDF(file.toFile())) {
      PDResources resources = loaded.getPage(0).getResources();
      PDTrueTypeFont first = (PDTrueTypeFont) resources.getFont(COSName.getPDFName("First"));
      PDTrueTypeFont second = (PDTrueTypeFont) resources.getFont(COSName.getPDFName("Second"));
      assertNotSame(first, second);
      assertNotSame(first.getTrueTypeFont(), second.getTrueTypeFont());
      byte[] firstBytes;
      byte[] secondBytes;
      try (InputStream firstStream = first.getFontDescriptor().getFontFile2().createInputStream();
           InputStream secondStream = second.getFontDescriptor().getFontFile2().createInputStream()) {
        firstBytes = firstStream.readAllBytes();
        secondBytes = secondStream.readAllBytes();
      }
      assertArrayEquals(bytes, firstBytes);
      assertArrayEquals(bytes, secondBytes);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"FontFile", "FontFile2", "FontFile3", "ToUnicode", "Encoding", "CIDToGIDMap"})
  void chargesEveryWholeCopiedFontStream(String name) throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream bytes = stream(document, 2);
      COSName key = COSName.getPDFName(name);
      COSDictionary font = new COSDictionary();
      if (name.startsWith("FontFile")) {
        COSDictionary descriptor = new COSDictionary();
        descriptor.setItem(key, bytes);
        font.setItem(COSName.FONT_DESC, descriptor);
      } else if (name.equals("CIDToGIDMap")) {
        COSDictionary descendant = new COSDictionary();
        descendant.setItem(key, bytes);
        COSArray descendants = new COSArray();
        descendants.add(descendant);
        font.setItem(COSName.DESCENDANT_FONTS, descendants);
      } else {
        font.setItem(key, bytes);
      }
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("F"), font);
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 1, temp.toFile());
      PDPage page = page(fonts);
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("budget=1"), failure.getMessage());
      assertTrue(failure.getMessage().contains("sum=2"), failure.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"inherited", "state", "nested-form", "descendant"})
  void followsResourceRoutes(String route) throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSDictionary fonts = new COSDictionary();
      COSDictionary font = font(stream(document, 2));
      fonts.setItem(COSName.getPDFName("F"), font);
      PDPage page = page(fonts);
      if (route.equals("inherited")) {
        document.addPage(page);
        COSDictionary parent = (COSDictionary) page.getCOSObject().getDictionaryObject(COSName.PARENT);
        parent.setItem(COSName.RESOURCES, page.getResources());
        page.getCOSObject().removeItem(COSName.RESOURCES);
      } else if (route.equals("state")) {
        COSArray fontSetting = new COSArray();
        fontSetting.add(font);
        fontSetting.add(COSInteger.ONE);
        COSDictionary state = new COSDictionary();
        state.setItem(COSName.FONT, fontSetting);
        COSDictionary states = new COSDictionary();
        states.setItem(COSName.getPDFName("G"), state);
        PDResources resources = new PDResources();
        resources.getCOSObject().setItem(COSName.EXT_G_STATE, states);
        page.setResources(resources);
        draw(page, "/G gs\n");
      } else if (route.equals("nested-form")) {
        PDResources resources = page.getResources();
        for (int i = 0; i < 2; i++) {
          COSStream form = content(i == 0 ? "BT /F 1 Tf ET\n" : "/X Do\n");   // the inner form selects F, the outer draws it
          form.setItem(COSName.SUBTYPE, COSName.FORM);
          form.setItem(COSName.RESOURCES, resources);
          COSDictionary forms = new COSDictionary();
          forms.setItem(COSName.getPDFName("X"), form);
          resources = new PDResources();
          resources.getCOSObject().setItem(COSName.XOBJECT, forms);
        }
        page.setResources(resources);
        draw(page, "/X Do\n");
      } else {
        COSArray descendants = new COSArray();
        descendants.add(font);
        COSDictionary type0 = new COSDictionary();
        type0.setItem(COSName.DESCENDANT_FONTS, descendants);
        fonts.setItem(COSName.getPDFName("F"), type0);
      }
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 1, temp.toFile());
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("sum=2"));
    }
  }

  @Test
  void deduplicatesIndirectFontDictionaryAliasesAcrossResourceRoutes() throws Exception {
    Path file = temp.resolve("shared-dictionary.pdf");
    try (PDDocument document = new PDDocument()) {
      COSStream shared = stream(document, 2);
      COSDictionary font = font(shared);
      font.setItem(COSName.TO_UNICODE, shared);
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("First"), font);
      fonts.setItem(COSName.getPDFName("Alias"), font);
      PDPage page = page(fonts);
      COSArray setting = new COSArray();
      setting.add(font);
      setting.add(COSInteger.ONE);
      COSDictionary state = new COSDictionary();
      state.setItem(COSName.FONT, setting);
      COSDictionary states = new COSDictionary();
      states.setItem(COSName.getPDFName("G"), state);
      page.getResources().getCOSObject().setItem(COSName.EXT_G_STATE, states);
      COSStream form = content("BT /F 1 Tf ET\n");
      form.setItem(COSName.SUBTYPE, COSName.FORM);
      form.setItem(COSName.RESOURCES, new PDResources(fontResources(font)));
      COSDictionary forms = new COSDictionary();
      forms.setItem(COSName.getPDFName("X"), form);
      page.getResources().getCOSObject().setItem(COSName.XOBJECT, forms);
      draw(page, "BT /First 1 Tf /Alias 1 Tf ET /G gs /X Do\n");     // every route reaches the one font
      document.addPage(page);
      COSDictionary parent = (COSDictionary) page.getCOSObject().getDictionaryObject(COSName.PARENT);
      parent.setItem(COSName.RESOURCES, page.getResources());
      page.getCOSObject().removeItem(COSName.RESOURCES);
      document.save(file.toFile());
    }
    try (PDDocument loaded = Loader.loadPDF(file.toFile())) {
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 2, temp.toFile());
      PDPage page = loaded.getPage(0);
      assertDoesNotThrow(() -> guard.checkPage(page));
    }
  }

  @Test
  void chargesDirectFontDictionaryForEachOccurrence() throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSDictionary font = font(stream(document, 2));
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("First"), font);
      fonts.setItem(COSName.getPDFName("Again"), font);
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 3, temp.toFile());
      PDPage page = page(fonts);
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("sum=4"), failure.getMessage());
    }
  }

  private static COSDictionary fontResources(COSDictionary font) {
    COSDictionary fonts = new COSDictionary();
    fonts.setItem(COSName.getPDFName("F"), font);
    COSDictionary resources = new COSDictionary();
    resources.setItem(COSName.FONT, fonts);
    return resources;
  }

  @Test
  void cachedLengthsChargeEachPageAndPageRefusalDoesNotCacheStreamFailure() throws Exception {
    Path file = temp.resolve("shared.pdf");
    try (PDDocument document = new PDDocument()) {
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("Shared"), font(stream(document, 2)));
      document.addPage(page(fonts));
      COSDictionary middleFonts = new COSDictionary(fonts);
      middleFonts.setItem(COSName.getPDFName("Another"), font(stream(document, 2)));
      document.addPage(page(middleFonts));
      document.addPage(page(fonts));
      COSDictionary finalFonts = new COSDictionary();
      finalFonts.setItem(COSName.getPDFName("RefusedOnPageTwo"), middleFonts.getItem(COSName.getPDFName("Another")));
      document.addPage(page(finalFonts));
      document.save(file.toFile());
    }
    try (PDDocument loaded = Loader.loadPDF(file.toFile())) {
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 3, temp.toFile());
      guard.checkPage(loaded.getPage(0));
      long decoded = guard.decodedStreams();
      PDPage second = loaded.getPage(1);
      assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(second)).code());
      PDPage third = loaded.getPage(2);
      assertDoesNotThrow(() -> guard.checkPage(third));
      // each page also decodes its own content stream (pages 2-3: 2); "Another" once on page 2, Shared stays cached
      assertEquals(decoded + 1 + 2, guard.decodedStreams());
      PDPage fourth = loaded.getPage(3);
      assertDoesNotThrow(() -> guard.checkPage(fourth));
      assertEquals(decoded + 2 + 3, guard.decodedStreams());
    }
  }

  @Test
  void chargesRawFontBytesEvenWithoutAFilter() throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream raw = document.getDocument().createCOSStream();
      try (OutputStream out = raw.createRawOutputStream()) {
        out.write(new byte[3]);
      }
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("F"), font(raw));
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 2, temp.toFile());
      PDPage page = page(fonts);
      assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> guard.checkPage(page)).code());
    }
  }

  @Test
  void chargesOnlyFinalFilterOutput() throws Exception {
    try (PDDocument document = new PDDocument()) {
      COSStream stream = document.getDocument().createCOSStream();
      COSArray filters = new COSArray();
      filters.add(COSName.ASCII_HEX_DECODE);
      filters.add(COSName.FLATE_DECODE);
      try (OutputStream out = stream.createOutputStream(filters)) {
        out.write(new byte[2]);
      }
      COSDictionary fonts = new COSDictionary();
      fonts.setItem(COSName.getPDFName("F"), font(stream));
      StreamDecodeGuard guard = new StreamDecodeGuard(32, 2, temp.toFile());
      PDPage page = page(fonts);
      assertDoesNotThrow(() -> guard.checkPage(page));
    }
  }

  @Test
  void boundsFontDictionaryVisitsEvenWhenAllStreamsAreCached() throws Exception {
    Path file = temp.resolve("many-small-font-streams.pdf");
    try (PDDocument document = new PDDocument()) {
      COSDictionary first = new COSDictionary();
      COSDictionary second = new COSDictionary();
      COSDictionary combined = new COSDictionary();
      for (int i = 0; i <= StreamDecodeGuard.MAX_STREAMS_PER_PAGE; i++) {
        COSName name = COSName.getPDFName("F" + i);
        COSDictionary font = font(stream(document, 0));
        combined.setItem(name, font);
        // page 1 also decodes its content stream, so it holds one font fewer than the per-page stream cap
        (i < StreamDecodeGuard.MAX_STREAMS_PER_PAGE - 1 ? first : second).setItem(name, font);
      }
      document.addPage(page(first));
      document.addPage(page(second));
      document.addPage(page(combined));
      document.save(file.toFile());
    }
    try (PDDocument loaded = Loader.loadPDF(file.toFile())) {
      StreamDecodeGuard guard = new StreamDecodeGuard(32, Long.MAX_VALUE, temp.toFile());
      guard.checkPage(loaded.getPage(0));
      guard.checkPage(loaded.getPage(1));
      PDPage third = loaded.getPage(2);
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(third));
      assertEquals(PdfFailureCode.PAGE_ERROR, failure.code());
      assertTrue(failure.getMessage().contains("4096 font dictionary visits"));
    }
  }

  @Test
  void refusesLongOverflowBeforeAddition() {
    StreamDecodeGuard.FontBytes bytes = new StreamDecodeGuard.FontBytes(Long.MAX_VALUE);
    bytes.add(Long.MAX_VALUE - 1);
    PageFailure failure = assertThrows(PageFailure.class, () -> bytes.add(2));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
    assertTrue(failure.getMessage().contains("budget=9223372036854775807"));
    assertTrue(failure.getMessage().contains("overflow"));
  }

  @Test
  void validatesTheKeyAndRejectsItsMisspellings() {
    TypedProperties props = new TypedProperties();
    WordCountTokenizer tokenizer = new WordCountTokenizer();
    String key = "hoodie.unstructured.pdf.max.page.font.bytes";
    props.setProperty(key, "0");
    assertTrue(assertThrows(IllegalArgumentException.class,
        () -> PdfChunkerConfig.from(props, tokenizer)).getMessage().contains(key));
    props.setProperty(key, "-1");
    assertThrows(IllegalArgumentException.class, () -> PdfChunkerConfig.from(props, tokenizer));
    props.setProperty(key, "2");
    assertEquals(2, PdfChunkerConfig.from(props, tokenizer).maxPageFontBytes());
    props.setProperty(key, "50331649");
    assertTrue(PdfChunkerConfig.from(props, tokenizer).aboveEnvelope());
    props.setProperty(key + ".typo", "2");
    assertThrows(IllegalArgumentException.class, () -> PdfChunkerConfig.from(props, tokenizer));
  }

  /** A page whose resources hold {@code fonts} and whose content selects every one of them with {@code Tf}. */
  private static PDPage page(COSDictionary fonts) throws Exception {
    PDPage page = new PDPage();
    PDResources resources = new PDResources();
    resources.getCOSObject().setItem(COSName.FONT, fonts);
    page.setResources(resources);
    StringBuilder ops = new StringBuilder("BT\n");
    for (COSName name : fonts.keySet()) {
      ops.append('/').append(name.getName()).append(" 1 Tf\n");
    }
    draw(page, ops.append("ET\n").toString());
    return page;
  }

  /** Sets the page's content to {@code ops}; the guard checks only the resources a page's content draws. */
  private static void draw(PDPage page, String ops) throws Exception {
    page.getCOSObject().setItem(COSName.CONTENTS, content(ops));
  }

  private static COSStream content(String ops) throws Exception {
    COSStream stream = new COSStream();
    try (OutputStream out = stream.createRawOutputStream()) {
      out.write(ops.getBytes(StandardCharsets.US_ASCII));
    }
    return stream;
  }

  private static COSDictionary font(COSStream stream) {
    COSDictionary descriptor = new COSDictionary();
    descriptor.setItem(COSName.FONT_FILE2, stream);
    COSDictionary font = new COSDictionary();
    font.setItem(COSName.FONT_DESC, descriptor);
    return font;
  }

  private static COSStream stream(PDDocument document, int bytes) throws Exception {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream out = stream.createOutputStream(COSName.FLATE_DECODE)) {
      byte[] buffer = new byte[64 * 1024];
      for (int remaining = bytes; remaining > 0;) {
        int count = Math.min(remaining, buffer.length);
        out.write(buffer, 0, count);
        remaining -= count;
      }
    }
    return stream;
  }
}
