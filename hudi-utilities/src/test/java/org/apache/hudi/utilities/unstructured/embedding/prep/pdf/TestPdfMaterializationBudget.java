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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDTrueTypeFont;
import org.apache.pdfbox.pdmodel.font.encoding.WinAnsiEncoding;
import org.apache.pdfbox.pdmodel.graphics.PDFontSetting;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Counts retained embedded programs independently of the production admission counter. */
class TestPdfMaterializationBudget {
  private static final COSName STATE = COSName.getPDFName("State");
  private static final COSName FONT = COSName.getPDFName("TextFont");
  private static final COSName FORM = COSName.getPDFName("TextForm");

  @TempDir
  Path staging;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void repeatedStateApplicationsRetainSeparateEmbeddedPrograms(boolean inForm) throws Exception {
    byte[] font = bundledFont();
    try (PDDocument document = Loader.loadPDF(stateText(font, inForm, 3, false))) {
      FontCapture capture = new FontCapture();
      capture.getText(document);
      assertEquals(3, capture.glyphs);
      assertEquals(3, capture.fonts.size());
      assertEquals(3, capture.programs.size());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void repeatedStateApplicationsFailBeforeTheNextFontCopy(boolean inForm) throws Exception {
    byte[] font = bundledFont();
    PageFailure failure = assertThrows(PageFailure.class,
        () -> extract(stateText(font, inForm, 3, false), 2L * font.length));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
    assertTrue(failure.getMessage().contains("budget=" + 2L * font.length), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void exactStateCopyBudgetKeepsReadableText(boolean inForm) throws Exception {
    byte[] font = bundledFont();
    PageText page = extract(stateText(font, inForm, 3, false), 3L * font.length);
    assertFalse(page.text().trim().isEmpty());
    assertFalse(page.truncated());
    assertFalse(extract(stateText(font, inForm, 1, false), font.length).text().trim().isEmpty());
  }

  @Test
  void stateCopiesShareTheBudgetWithCachedTextFonts() throws Exception {
    byte[] font = bundledFont();
    byte[] pdf = stateText(font, false, 3, true);
    assertEquals(PdfFailureCode.DECODE_LIMIT,
        assertThrows(PageFailure.class, () -> extract(pdf, 3L * font.length)).code());
    assertFalse(extract(pdf, 4L * font.length).text().trim().isEmpty());
  }

  @Test
  void repeatedTextFontSelectionKeepsItsCachedProgram() throws Exception {
    byte[] font = bundledFont();
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      PDTrueTypeFont loaded = PDTrueTypeFont.load(document, new ByteArrayInputStream(font), WinAnsiEncoding.INSTANCE);
      PDResources resources = new PDResources();
      resources.put(FONT, loaded);
      PDPage page = new PDPage();
      document.addPage(page);
      page.setResources(resources);
      page.getCOSObject().setItem(COSName.CONTENTS, content(document,
          "BT 40 700 Td /TextFont 12 Tf (A) Tj /TextFont 12 Tf (B) Tj /TextFont 12 Tf (C) Tj ET\n"));
      pdf = save(document);
    }
    assertFalse(extract(pdf, font.length).text().trim().isEmpty());
    try (PDDocument document = Loader.loadPDF(pdf)) {
      FontCapture capture = new FontCapture();
      capture.getText(document);
      assertEquals(3, capture.glyphs);
      assertEquals(1, capture.fonts.size());
      assertEquals(1, capture.programs.size());
    }
  }

  private PageText extract(byte[] pdf, long budget) throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PDF_MAX_PAGE_FONT_BYTES.key(), Long.toString(budget));
    PdfChunkerConfig config = PdfChunkerConfig.from(props, new WordCountTokenizer());
    try (PdfBoxPageSource source = new PdfBoxPageSource(() -> new ByteArrayInputStream(pdf),
        new PdfFileMeta("font-source", "font.pdf", pdf.length), config, staging.toFile(), true)) {
      source.open();
      assertTrue(source.hasMorePages());
      return source.next();
    }
  }

  private static byte[] bundledFont() throws IOException {
    try (InputStream source = PDTrueTypeFont.class.getResourceAsStream(
        "/org/apache/pdfbox/resources/ttf/LiberationSans-Regular.ttf")) {
      assertNotNull(source, "the pinned PDFBox jar supplies this portable embedded font");
      return source.readAllBytes();
    }
  }

  private static byte[] stateText(byte[] font, boolean inForm, int applications, boolean includeTextFont) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDTrueTypeFont loaded = PDTrueTypeFont.load(document, new ByteArrayInputStream(font), WinAnsiEncoding.INSTANCE);
      PDFontSetting setting = new PDFontSetting();
      setting.setFont(loaded);
      setting.setFontSize(12);
      COSDictionary state = new COSDictionary();
      state.setItem(COSName.FONT, setting);
      COSDictionary states = new COSDictionary();
      states.setItem(STATE, state);
      PDResources resources = new PDResources();
      resources.getCOSObject().setItem(COSName.EXT_G_STATE, states);
      if (includeTextFont) {
        resources.put(FONT, loaded);
      }
      PDPage page = new PDPage();
      document.addPage(page);
      StringBuilder ops = new StringBuilder();
      if (includeTextFont) {
        ops.append("BT 40 730 Td /TextFont 12 Tf (D) Tj ET\n");
      }
      if (inForm) {
        COSStream form = content(document, "BT /State gs (A) Tj ET\n");
        form.setItem(COSName.TYPE, COSName.XOBJECT);
        form.setItem(COSName.SUBTYPE, COSName.FORM);
        form.setItem(COSName.BBOX, PDRectangle.LETTER.getCOSArray());
        form.setItem(COSName.RESOURCES, resources);
        COSDictionary forms = new COSDictionary();
        forms.setItem(FORM, form);
        PDResources pageResources = new PDResources();
        pageResources.getCOSObject().setItem(COSName.XOBJECT, forms);
        page.setResources(pageResources);
        for (int i = 0; i < applications; i++) {
          ops.append("q 1 0 0 1 ").append(40 + 20 * i).append(" 700 cm /TextForm Do Q\n");
        }
      } else {
        page.setResources(resources);
        ops.append("BT 40 700 Td ");
        for (int i = 0; i < applications; i++) {
          ops.append("/State gs (").append((char) ('A' + i)).append(") Tj ");
        }
        ops.append("ET\n");
      }
      page.getCOSObject().setItem(COSName.CONTENTS, content(document, ops.toString()));
      return save(document);
    }
  }

  private static COSStream content(PDDocument document, String ops) throws IOException {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream out = stream.createOutputStream()) {
      out.write(ops.getBytes(StandardCharsets.US_ASCII));
    }
    return stream;
  }

  private static byte[] save(PDDocument document) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    document.save(bytes);
    return bytes.toByteArray();
  }

  private static final class FontCapture extends PDFTextStripper {
    private final Set<PDFont> fonts = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<Object> programs = Collections.newSetFromMap(new IdentityHashMap<>());
    private int glyphs;

    private FontCapture() throws IOException {
    }

    @Override
    protected void processTextPosition(TextPosition text) {
      glyphs++;
      fonts.add(text.getFont());
      programs.add(((PDTrueTypeFont) text.getFont()).getTrueTypeFont());
      super.processTextPosition(text);
    }
  }
}
