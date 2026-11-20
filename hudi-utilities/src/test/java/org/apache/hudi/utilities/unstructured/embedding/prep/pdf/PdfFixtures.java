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
import org.apache.pdfbox.cos.COSFloat;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.cos.COSString;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.encryption.AccessPermission;
import org.apache.pdfbox.pdmodel.encryption.StandardProtectionPolicy;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
import org.apache.pdfbox.pdmodel.graphics.image.LosslessFactory;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * Builds the PDF fixtures of the unit tests with PDFBox's writer, deterministically.
 * Text uses the standard Helvetica font (WinAnsi, so fixture text is ASCII). Oversized input fixtures are built at the COS level.
 * Octal escapes keep the fixture payload bytes exact.
 */
public final class PdfFixtures {

  /** Decoded size that exceeds the default 32 MiB guard cap, small enough to build in a unit test. */
  public static final long UNIT_OVERSIZED_INPUT_BYTES = 64L * 1024 * 1024;

  /** The document id every fixture is saved with unless a caller passes its own: a fixed trailer /ID and fixed dates. */
  static final long DOCUMENT_ID = 42L;

  private static final COSName F1 = COSName.getPDFName("F1");
  private static final COSName F2 = COSName.getPDFName("F2");

  private PdfFixtures() {
  }

  /** The oversized input shapes that exceed the decoded-size budget. */
  public enum OversizedInput {
    OVERSIZED_STREAM, OVERSIZED_NESTED_FORM, LARGE_EMBEDDED_FONT, LARGE_INHERITED_EMBEDDED_FONT, LARGE_TO_UNICODE_MAP,
    LARGE_CID_TO_GID_MAP, DEEP_OBJECT_NESTING, LARGE_TYPE3_GLYPH_STREAM,
    LARGE_UNUSED_TYPE3_GLYPH_STREAM, LARGE_TYPE3_WIDTH_STREAM, DEEP_SHARED_FORM,
    /**
     * /Filter [/Fl /AHx /AHx]: abbreviations, a duplicate PDFBox drops, and a chain whose INTERMEDIATE output (hex text)
     * exceeds the cap inside a temp file.
     */
    OVERSIZED_FILTER_CHAIN,
    /** The oversized stream is the /ToUnicode of a font reached only through an ExtGState /Font. */
    LARGE_GRAPHICS_STATE_FONT_MAP,
    /** A tiny Flate stream whose /DecodeParms predictor row is ~256 MiB: PDFBox allocates two rows before decoding. */
    LARGE_PREDICTOR_ROW,
    /** The CIDToGIDMap of {@link #LARGE_CID_TO_GID_MAP} written unfiltered: PDFBox copies it onto the heap whole. */
    LARGE_UNFILTERED_CID_TO_GID_MAP,
    /** An unfiltered /FontFile2: fontbox's TTFParser copies the whole view onto the heap. */
    LARGE_UNFILTERED_EMBEDDED_FONT,
    /** An unfiltered /FontFile3 (Type1C) of a Type1 font: fontbox's CFFParser copies the whole view. */
    LARGE_UNFILTERED_CFF_FONT
  }

  // ------------------------------------------------------------------ plain text documents

  /** One page per entry; lines wrap at {@code wrap} chars; a blank line in the text starts a new paragraph. */
  public static byte[] text(List<String> pages) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      for (String page : pages) {
        addTextPage(doc, page);
      }
      return save(doc);
    }
  }

  public static byte[] text(String... pages) throws IOException {
    return text(Arrays.asList(pages));
  }

  /** {@code pages} pages of deterministic pseudo-English prose (paragraphs of sentences) seeded by {@code seed}. */
  public static byte[] prose(long seed, int pages, int sentencesPerPage) throws IOException {
    String[] words = ("lake table record index stream chunk page file vector model search data commit timeline the of and "
        + "to in is that for it as with by on").split(" ");
    Random random = new Random(seed);
    List<String> texts = new ArrayList<>();
    for (int p = 1; p <= pages; p++) {
      StringBuilder sb = new StringBuilder();
      for (int s = 0; s < sentencesPerPage; s++) {
        int n = 4 + random.nextInt(12);
        for (int w = 0; w < n; w++) {
          String word = words[random.nextInt(words.length)];
          sb.append(w == 0 ? Character.toUpperCase(word.charAt(0)) + word.substring(1) : word).append(w + 1 < n ? " " : ". ");
        }
        if (s % 6 == 5) {
          sb.append("\n\n");
        }
      }
      texts.add(sb.toString().trim());
    }
    return text(texts);
  }

  static void addTextPage(PDDocument doc, String text) throws IOException {
    PDPage page = new PDPage();
    doc.addPage(page);
    page.setResources(helvetica());
    page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, textOps(text, 50, 750)));
  }

  /** Text operators drawing {@code text} wrapped at 90 chars, from (x, y) downwards. */
  static String textOps(String text, int x, int y) {
    StringBuilder ops = new StringBuilder("BT /F1 10 Tf 12 TL ").append(x).append(' ').append(y).append(" Td\n");
    for (String paragraph : text.split("\n\n", -1)) {
      for (String line : wrap(paragraph.replace('\n', ' '), 90)) {
        ops.append('(').append(escape(line)).append(") Tj T*\n");
      }
      ops.append("T*\n");                                // an empty line between paragraphs
    }
    return ops.append("ET\n").toString();
  }

  static List<String> wrap(String text, int width) {
    List<String> lines = new ArrayList<>();
    StringBuilder line = new StringBuilder();
    for (String word : text.split(" ")) {
      if (line.length() > 0 && line.length() + 1 + word.length() > width) {
        lines.add(line.toString());
        line.setLength(0);
      }
      if (line.length() > 0) {
        line.append(' ');
      }
      line.append(word);
    }
    if (line.length() > 0) {
      lines.add(line.toString());
    }
    return lines;
  }

  /** Two columns on one page, the RIGHT column written first in the content stream. */
  public static byte[] multiColumn(String left, String right) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      PDPage page = new PDPage();
      doc.addPage(page);
      page.setResources(helvetica());
      String ops = textOpsNarrow(right, 320, 750) + textOpsNarrow(left, 50, 750);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, ops));
      // an article thread bead over the left column: with beads on, PDFBox would regroup text by article
      COSDictionary bead = new COSDictionary();
      bead.setItem(COSName.TYPE, COSName.getPDFName("Bead"));
      bead.setItem(COSName.R, rect(50, 400, 300, 760));
      bead.setItem(COSName.P, page.getCOSObject());
      COSArray beads = new COSArray();
      beads.add(bead);
      page.getCOSObject().setItem(COSName.B, beads);
      return save(doc);
    }
  }

  private static String textOpsNarrow(String text, int x, int y) {
    StringBuilder ops = new StringBuilder("BT /F1 10 Tf 12 TL ").append(x).append(' ').append(y).append(" Td\n");
    for (String line : wrap(text, 40)) {
      ops.append('(').append(escape(line)).append(") Tj T*\n");
    }
    return ops.append("ET\n").toString();
  }

  /** AES-256 with a non-empty user password. */
  public static byte[] encrypted(String text) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, text);
      StandardProtectionPolicy policy = new StandardProtectionPolicy("owner-secret", "user-secret", new AccessPermission());
      policy.setEncryptionKeyLength(256);
      doc.protect(policy);
      return save(doc);
    }
  }

  /**
   * Encrypted with an owner password only: it opens with the empty user password, and PDFBox writes the decrypted copy of every
   * object stream and page stream it reads to its scratch cache, while loading and while extracting each page.
   */
  public static byte[] ownerPasswordOnly(String... pages) throws IOException {
    return ownerPasswordOnly(text(pages));
  }

  /** {@code pdf} encrypted with an owner password only, its streams kept as they are (a broken one stays broken). */
  public static byte[] ownerPasswordOnly(byte[] pdf) throws IOException {
    try (PDDocument doc = Loader.loadPDF(pdf)) {
      StandardProtectionPolicy policy = new StandardProtectionPolicy("owner-secret", "", new AccessPermission());
      policy.setEncryptionKeyLength(256);
      doc.protect(policy);
      return save(doc);
    }
  }

  /** Pages holding only an incompressible image of about {@code imageBytes} bytes each (no text). */
  public static byte[] imageOnly(int pages, int side) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      Random random = new Random(42);
      for (int p = 0; p < pages; p++) {
        PDPage page = new PDPage();
        doc.addPage(page);
        BufferedImage image = new BufferedImage(side, side, BufferedImage.TYPE_INT_RGB);
        for (int x = 0; x < side; x++) {
          for (int y = 0; y < side; y++) {
            image.setRGB(x, y, random.nextInt(0xFFFFFF));
          }
        }
        PDImageXObject xobject = LosslessFactory.createFromImage(doc, image);
        try (PDPageContentStream content = new PDPageContentStream(doc, page)) {
          content.drawImage(xobject, 50, 300, 300, 300);
        }
      }
      return save(doc);
    }
  }

  /** Three text pages; page 2's content stream names a filter that does not exist, so extracting it throws. */
  public static byte[] brokenPage2(String p1, String p2, String p3) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, p1);
      addTextPage(doc, p2);
      addTextPage(doc, p3);
      COSStream broken = (COSStream) doc.getPage(1).getCOSObject().getDictionaryObject(COSName.CONTENTS);
      broken.setItem(COSName.FILTER, COSName.getPDFName("BogusDecode"));
      return save(doc);
    }
  }

  // ------------------------------------------------------------------ glyph/text and depth caps

  /** Page 2 of 3 shows {@code glyphs} glyphs in one Tj. */
  public static byte[] manyGlyphs(int glyphs) throws IOException {
    StringBuilder run = new StringBuilder(glyphs);
    for (int i = 0; i < glyphs; i++) {
      run.append('a');
    }
    return threePages("BT /F1 1 Tf 0 700 Td (" + run + ") Tj ET\n");
  }

  /** Page 2 of 3 draws a form holding {@code glyphsPerForm} glyphs {@code draws} times. */
  public static byte[] repeatedForm(int glyphsPerForm, int draws) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      StringBuilder run = new StringBuilder();
      for (int i = 0; i < glyphsPerForm; i++) {
        run.append('b');
      }
      COSStream form = form(doc, "BT /F1 1 Tf 0 0 Td (" + run + ") Tj ET\n", resources.getCOSObject());
      COSDictionary xobjects = new COSDictionary();
      xobjects.setItem(COSName.getPDFName("Fm1"), form);
      resources.getCOSObject().setItem(COSName.XOBJECT, xobjects);
      page.setResources(resources);
      StringBuilder ops = new StringBuilder();
      for (int i = 0; i < draws; i++) {
        ops.append("/Fm1 Do\n");
      }
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, ops.toString()));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3: one /ActualText of {@code chars} chars, used by {@code uses} single-glyph spans on one line. */
  public static byte[] repeatedActualText(int chars, int uses) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      StringBuilder actual = new StringBuilder();
      for (int i = 0; i < chars; i++) {
        actual.append('x');
      }
      COSDictionary properties = new COSDictionary();
      COSDictionary span = new COSDictionary();
      span.setItem(COSName.ACTUAL_TEXT, new COSString(actual.toString()));
      properties.setItem(COSName.getPDFName("P0"), span);
      resources.getCOSObject().setItem(COSName.PROPERTIES, properties);
      page.setResources(resources);
      StringBuilder ops = new StringBuilder("BT /F1 10 Tf 50 700 Td\n");
      for (int i = 0; i < uses; i++) {
        ops.append("/Span /P0 BDC (a) Tj EMC\n");
      }
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, ops.append("ET\n").toString()));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3 is {@code q} repeated {@code depth} times. */
  public static byte[] graphicsStateStack(int depth) throws IOException {
    return threePages(repeat("q ", depth) + "BT /F1 10 Tf 50 700 Td (deep) Tj ET\n");
  }

  /** Page 2 of 3 is {@code /A BMC} repeated {@code depth} times. */
  public static byte[] bmcStack(int depth) throws IOException {
    return threePages(repeat("/A BMC ", depth) + "BT /F1 10 Tf 50 700 Td (deep) Tj ET\n");
  }

  /** {@code pages} pages, each opening {@code perPage} BDC spans it never closes, plus text. */
  public static byte[] unbalancedBdc(int pages, int perPage) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      for (int p = 1; p <= pages; p++) {
        PDPage page = new PDPage();
        doc.addPage(page);
        page.setResources(helvetica());
        page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, repeat("/Span <<>> BDC ", perPage)
            + textOps("Page " + p + " text survives unbalanced spans.", 50, 700)));
      }
      return save(doc);
    }
  }

  /** {@code pages} pages of balanced BDC...EMC spans, {@code spans} per page (a legitimate tagged PDF). */
  public static byte[] tagged(int pages, int spans) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      for (int p = 1; p <= pages; p++) {
        PDPage page = new PDPage();
        doc.addPage(page);
        page.setResources(helvetica());
        StringBuilder ops = new StringBuilder("BT /F1 10 Tf 12 TL 50 750 Td\n");
        for (int s = 0; s < spans; s++) {
          ops.append("/P <</MCID ").append(s).append(">> BDC (w").append(s).append(" ) Tj EMC\n");
          if (s % 10 == 9) {
            ops.append("T*\n");
          }
        }
        page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, ops.append("ET\n").toString()));
      }
      return save(doc);
    }
  }

  /** Three pages; page 2's content stream is {@code page2Ops} (Helvetica as /F1), Flate-compressed. */
  public static byte[] threePagesWithPage2(String page2Ops) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      page.setResources(helvetica());
      COSStream stream = doc.getDocument().createCOSStream();
      try (OutputStream out = stream.createOutputStream(COSName.FLATE_DECODE)) {
        out.write(page2Ops.getBytes(StandardCharsets.ISO_8859_1));
      }
      page.getCOSObject().setItem(COSName.CONTENTS, stream);
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3 uses a Helvetica font whose /ToUnicode holds one bfrange line {@code range} (e.g. {@code <0000> <FFFF> <0000>}). */
  public static byte[] bfrangeOf(String range) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      page.setResources(resources);
      String cmap = "/CIDInit /ProcSet findresource begin 12 dict begin begincmap\n"
          + "1 begincodespacerange <0000> <FFFF> endcodespacerange\n1 beginbfrange " + range + " endbfrange\n"
          + "endcmap CMapName currentdict /CMap defineresource pop end end\n";
      COSDictionary font = type1Helvetica();
      font.setItem(COSName.TO_UNICODE, stream(doc, cmap));
      fonts(resources.getCOSObject()).setItem(F2, font);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (x) Tj ET\n"));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  private static byte[] threePages(String page2Ops) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      page.setResources(helvetica());
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, page2Ops));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  // ------------------------------------------------------------------ oversized streams

  /**
   * Pages: "First page text." / the oversized input page / "Third page text." Page 2 references the oversized stream,
   * so with the guard off PDFBox decodes it.
   */
  public static byte[] oversizedInput(OversizedInput kind, long decodedBytes) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      page.setResources(resources);
      COSDictionary res = resources.getCOSObject();
      String ops = "BT /F1 10 Tf 50 700 Td (fill page) Tj ET\n";
      switch (kind) {
        case OVERSIZED_STREAM: {
          COSArray contents = new COSArray();
          contents.add(stream(doc, ops));
          contents.add(oversizedFlateStream(doc, decodedBytes));
          page.getCOSObject().setItem(COSName.CONTENTS, contents);
          ops = null;
          break;
        }
        case OVERSIZED_NESTED_FORM: {
          COSStream inner = oversizedFlateStream(doc, decodedBytes);
          markForm(inner, res);
          COSStream outer = form(doc, "/In Do\n", xobjectResources(res, "In", inner));
          xobjects(res).setItem(COSName.getPDFName("Out"), outer);
          ops += "/Out Do\n";
          break;
        }
        case DEEP_OBJECT_NESTING: {
          COSStream current = oversizedFlateStream(doc, decodedBytes);
          markForm(current, res);
          for (int level = 29; level >= 1; level--) {
            current = form(doc, "/Next Do\n", xobjectResources(res, "Next", current));
          }
          xobjects(res).setItem(COSName.getPDFName("Lvl1"), current);
          ops += "/Lvl1 Do\n";
          break;
        }
        case DEEP_SHARED_FORM: {
          // form F is reached at level 50 through a chain AND directly from the page; F's child G is the oversized stream
          COSStream g = oversizedFlateStream(doc, decodedBytes);
          markForm(g, res);
          COSStream f = form(doc, "/G Do\n", xobjectResources(res, "G", g));
          COSStream current = f;
          for (int level = 49; level >= 1; level--) {
            current = form(doc, "/Next Do\n", xobjectResources(res, "Next", current));
          }
          xobjects(res).setItem(COSName.getPDFName("AChain"), current);
          xobjects(res).setItem(COSName.getPDFName("ZDirect"), f);
          ops += "/AChain Do /ZDirect Do\n";
          break;
        }
        case OVERSIZED_FILTER_CHAIN: {
          // Flate of decodedBytes '0' chars: the Flate stage yields hex text, the ASCIIHex stage zeros
          COSStream stream = oversizedFlateStream(doc, decodedBytes, (byte) '0');
          COSArray filters = new COSArray();
          filters.add(COSName.getPDFName("Fl"));
          filters.add(COSName.getPDFName("AHx"));
          filters.add(COSName.getPDFName("AHx"));
          stream.setItem(COSName.FILTER, filters);
          COSArray contents = new COSArray();
          contents.add(stream(doc, ops));
          contents.add(stream);
          page.getCOSObject().setItem(COSName.CONTENTS, contents);
          ops = null;
          break;
        }
        case LARGE_GRAPHICS_STATE_FONT_MAP: {
          COSDictionary font = type1Helvetica();
          font.setItem(COSName.TO_UNICODE, oversizedFlateStream(doc, decodedBytes));
          COSArray fontRef = new COSArray();
          fontRef.add(font);
          fontRef.add(COSInteger.get(10));
          COSDictionary state = new COSDictionary();
          state.setItem(COSName.FONT, fontRef);
          COSDictionary states = new COSDictionary();
          states.setItem(COSName.getPDFName("GS1"), state);
          res.setItem(COSName.EXT_G_STATE, states);
          ops += "BT /GS1 gs 50 650 Td (x) Tj ET\n";
          break;
        }
        case LARGE_PREDICTOR_ROW: {
          COSStream stream = oversizedFlateStream(doc, 1024);
          COSDictionary params = new COSDictionary();
          params.setInt(COSName.PREDICTOR, 12);
          params.setInt(COSName.COLUMNS, 1);
          params.setInt(COSName.COLORS, 1);
          params.setInt(COSName.BITS_PER_COMPONENT, 2_147_000_000);
          stream.setItem(COSName.DECODE_PARMS, params);
          COSArray contents = new COSArray();
          contents.add(stream(doc, ops));
          contents.add(stream);
          page.getCOSObject().setItem(COSName.CONTENTS, contents);
          ops = null;
          break;
        }
        case LARGE_UNFILTERED_EMBEDDED_FONT: {
          fonts(res).setItem(F2, trueTypeFont(unfilteredZeros(doc, decodedBytes)));
          ops += "BT /F2 10 Tf 50 650 Td (x) Tj ET\n";
          break;
        }
        case LARGE_UNFILTERED_CFF_FONT: {
          COSStream fontFile3 = unfilteredZeros(doc, decodedBytes);
          fontFile3.setItem(COSName.SUBTYPE, COSName.getPDFName("Type1C"));
          COSDictionary font = trueTypeFont(fontFile3);
          COSDictionary descriptor = (COSDictionary) font.getDictionaryObject(COSName.FONT_DESC);
          descriptor.removeItem(COSName.FONT_FILE2);
          descriptor.setItem(COSName.FONT_FILE3, fontFile3);
          font.setItem(COSName.SUBTYPE, COSName.TYPE1);
          fonts(res).setItem(F2, font);
          ops += "BT /F2 10 Tf 50 650 Td (x) Tj ET\n";
          break;
        }
        case LARGE_EMBEDDED_FONT:
        case LARGE_INHERITED_EMBEDDED_FONT: {
          COSDictionary font = trueTypeFont(oversizedFlateStream(doc, decodedBytes));
          if (kind == OversizedInput.LARGE_EMBEDDED_FONT) {
            fonts(res).setItem(F2, font);
          } else {
            // the page's own /Resources lacks /F2; it is inherited from the page tree node (PDPage.getResources)
            COSDictionary parentResources = new COSDictionary();
            COSDictionary parentFonts = new COSDictionary();
            parentFonts.setItem(F2, font);
            parentFonts.setItem(F1, fonts(res).getItem(F1));
            parentResources.setItem(COSName.FONT, parentFonts);
            doc.getPages().getCOSObject().setItem(COSName.RESOURCES, parentResources);
            page.getCOSObject().removeItem(COSName.RESOURCES);
          }
          ops += "BT /F2 10 Tf 50 650 Td (x) Tj ET\n";
          break;
        }
        case LARGE_TO_UNICODE_MAP: {
          COSDictionary font = type1Helvetica();
          font.setItem(COSName.TO_UNICODE, oversizedFlateStream(doc, decodedBytes));
          fonts(res).setItem(F2, font);
          ops += "BT /F2 10 Tf 50 650 Td (x) Tj ET\n";
          break;
        }
        case LARGE_CID_TO_GID_MAP:
        case LARGE_UNFILTERED_CID_TO_GID_MAP: {
          COSDictionary descendant = new COSDictionary();
          descendant.setItem(COSName.TYPE, COSName.FONT);
          descendant.setItem(COSName.SUBTYPE, COSName.CID_FONT_TYPE2);
          descendant.setItem(COSName.BASE_FONT, COSName.getPDFName("Fill"));
          COSDictionary info = new COSDictionary();
          info.setString(COSName.REGISTRY, "Adobe");
          info.setString(COSName.ORDERING, "Identity");
          info.setInt(COSName.SUPPLEMENT, 0);
          descendant.setItem(COSName.CIDSYSTEMINFO, info);
          descendant.setItem(COSName.CID_TO_GID_MAP, kind == OversizedInput.LARGE_CID_TO_GID_MAP ? oversizedFlateStream(doc, decodedBytes)
              : unfilteredZeros(doc, decodedBytes));
          COSDictionary font = new COSDictionary();
          font.setItem(COSName.TYPE, COSName.FONT);
          font.setItem(COSName.SUBTYPE, COSName.TYPE0);
          font.setItem(COSName.BASE_FONT, COSName.getPDFName("Fill"));
          font.setItem(COSName.ENCODING, COSName.IDENTITY_H);
          COSArray descendants = new COSArray();
          descendants.add(descendant);
          font.setItem(COSName.DESCENDANT_FONTS, descendants);
          fonts(res).setItem(F2, font);
          ops += "BT /F2 10 Tf 50 650 Td <0001> Tj ET\n";
          break;
        }
        case LARGE_TYPE3_GLYPH_STREAM:
        case LARGE_UNUSED_TYPE3_GLYPH_STREAM:
        case LARGE_TYPE3_WIDTH_STREAM: {
          fonts(res).setItem(F2, type3(doc, kind, decodedBytes, 1));
          ops += "BT /F2 10 Tf 50 650 Td (a) Tj ET\n";
          break;
        }
        default:
          throw new IllegalArgumentException(kind.name());
      }
      if (ops != null) {
        page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, ops));
      }
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /**
   * Three pages sharing one stream that fails to decode (a /Crypt filter naming an unsupported crypt filter): as every
   * page's /Contents, or as the /FontFile2 of a font every page uses.
   */
  public static byte[] undecodableSharedStream(boolean asContent) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      COSStream broken = rawStream(doc, "BT /F1 10 Tf 50 700 Td (x) Tj ET\n".getBytes(StandardCharsets.US_ASCII));
      broken.setItem(COSName.FILTER, COSName.getPDFName("Crypt"));
      // PDFBox's CryptFilter reads /Name from the dictionary it is handed, which is the stream's own (not /DecodeParms)
      broken.setItem(COSName.NAME, COSName.getPDFName("Unsupported"));
      COSDictionary font = trueTypeFont(broken);
      for (int p = 1; p <= 3; p++) {
        PDPage page = new PDPage();
        doc.addPage(page);
        PDResources resources = helvetica();
        page.setResources(resources);
        if (asContent) {
          page.getCOSObject().setItem(COSName.CONTENTS, broken);
        } else {
          fonts(resources.getCOSObject()).setItem(F2, font);
          page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (x) Tj ET\n"));
        }
      }
      return save(doc);
    }
  }

  public static byte[] sharedOversizedStream(long decodedBytes) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      COSDictionary font = type1Helvetica();
      font.setItem(COSName.TO_UNICODE, oversizedFlateStream(doc, decodedBytes));
      for (int p = 2; p <= 3; p++) {
        PDPage page = new PDPage();
        doc.addPage(page);
        PDResources resources = helvetica();
        fonts(resources.getCOSObject()).setItem(F2, font);
        page.setResources(resources);
        page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (x) Tj ET\n"));
      }
      addTextPage(doc, "Fourth page text. It is fine too.");
      return save(doc);
    }
  }

  /**
   * Page 2 of 3: a content stream of {@code "1.5 "} repeated until {@code bytes}, never closed by an operator, so PDFBox's
   * stream parser keeps every operand in one list. Flate-compressed, or raw when {@code flate} is false.
   */
  public static byte[] operandPile(long bytes, boolean flate) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      page.setResources(helvetica());
      COSStream stream = doc.getDocument().createCOSStream();
      byte[] unit = "1.5 ".getBytes(StandardCharsets.US_ASCII);
      byte[] block = new byte[unit.length * 16384];
      for (int i = 0; i < block.length; i++) {
        block[i] = unit[i % unit.length];
      }
      try (OutputStream raw = stream.createRawOutputStream();
           OutputStream out = flate ? new DeflaterOutputStream(raw, new Deflater(Deflater.BEST_COMPRESSION)) : raw) {
        for (long written = 0; written < bytes; written += block.length) {
          out.write(block, 0, (int) Math.min(block.length, bytes - written));
        }
      }
      if (flate) {
        stream.setItem(COSName.FILTER, COSName.FLATE_DECODE);
      }
      COSArray contents = new COSArray();
      contents.add(stream(doc, "BT /F1 10 Tf 50 700 Td (fill page) Tj ET\n"));
      contents.add(stream);
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3: a /Contents array of one unfiltered stream per body, in order. */
  public static byte[] contentsStreams(String... bodies) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      page.setResources(helvetica());
      COSArray contents = new COSArray();
      for (String body : bodies) {
        contents.add(stream(doc, body));
      }
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /**
   * Page 1 draws form XObject S; page 2's /Contents array is [S, a text stream], so the guard meets S first as a form, then as page content.
   */
  public static byte[] formReusedAsContents() throws IOException {
    try (PDDocument doc = new PDDocument()) {
      PDResources base = helvetica();
      COSStream shared = form(doc, "BT /F1 10 Tf 50 700 Td (Shared form text.) Tj ET\n", base.getCOSObject());
      PDPage page1 = new PDPage();
      doc.addPage(page1);
      page1.setResources(new PDResources(xobjectResources(base.getCOSObject(), "S", shared)));
      page1.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "q /S Do Q\n"));
      PDPage page2 = new PDPage();
      doc.addPage(page2);
      page2.setResources(helvetica());
      COSArray contents = new COSArray();
      contents.add(shared);
      contents.add(stream(doc, "BT /F1 10 Tf 50 650 Td (Page two text.) Tj ET\n"));
      page2.getCOSObject().setItem(COSName.CONTENTS, contents);
      return save(doc);
    }
  }

  /**
   * Page 2 of 3: a /Contents array whose streams each hold {@code operands} numbers ({@code tail} appended, e.g. an operator
   * or nothing). With {@code reuse}, one stream object fills all {@code streams} slots of the array.
   */
  public static byte[] contentsArray(int streams, int operands, String tail, boolean reuse) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      page.setResources(helvetica());
      StringBuilder ops = new StringBuilder();
      for (int i = 0; i < operands; i++) {
        ops.append("1 ");
      }
      String body = ops.append(tail).append('\n').toString();
      COSArray contents = new COSArray();
      contents.add(stream(doc, "BT /F1 10 Tf 50 700 Td (array page) Tj ET\n"));
      COSStream shared = stream(doc, body);
      for (int i = 0; i < streams; i++) {
        contents.add(reuse ? shared : stream(doc, body));
      }
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3 uses a font whose /ToUnicode CMap maps one bfrange over 2^31 four-byte codes. */
  public static byte[] largeBfrange() throws IOException {
    return largeBfrange(false);
  }

  /**
   * As {@link #largeBfrange()}; with {@code alsoFontFile2}, an earlier font on the page names the same CMap stream as its
   * /FontFile2, so the guard meets the stream first as an opaque font file.
   */
  public static byte[] largeBfrange(boolean alsoFontFile2) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      page.setResources(resources);
      String cmap = "/CIDInit /ProcSet findresource begin 12 dict begin begincmap\n"
          + "1 begincodespacerange <00000000> <FFFFFFFF> endcodespacerange\n"
          + "1 beginbfrange <00000000> <7FFFFFFE> <0000> endbfrange\n"
          + "endcmap CMapName currentdict /CMap defineresource pop end end\n";
      COSStream toUnicode = stream(doc, cmap);
      if (alsoFontFile2) {
        fonts(resources.getCOSObject()).setItem(COSName.getPDFName("F0"), trueTypeFont(toUnicode));
      }
      COSDictionary font = type1Helvetica();
      font.setItem(COSName.TO_UNICODE, toUnicode);
      fonts(resources.getCOSObject()).setItem(F2, font);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (x) Tj ET\n"));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3 uses an embedded Type1 font whose private dict declares /Subrs with {@code subrs} entries. */
  public static byte[] largeType1Subrs(long subrs) throws IOException {
    return largeType1Subrs(subrs, Type1Shape.PLAIN);
  }

  /** How the embedded Type1 font file of {@link #largeType1Subrs(long, Type1Shape)} is laid out. */
  public enum Type1Shape {
    /** clear text, then binary eexec section; /Length1 and /Length2 exact. */
    PLAIN,
    /** a comment "%currentfile eexec" early in the clear text (PDFBox splits at /Length1, after the real eexec). */
    DECOY_EEXEC,
    /** the first ciphertext byte is 0x00 (PDFBox skips only CR, LF, space and tab after eexec). */
    LEADING_ZERO,
    /** the whole PFB (0x80 records) embedded as the font file, as some bad files do (PDFBOX-2607). */
    PFB,
    /** the eexec section hex-encoded, 64 digits per line. */
    HEX,
    /** the /Subrs count written with 40 leading zeros (fontbox reads every digit). */
    PADDED_COUNT,
    /** an entry before /Subrs whose string holds a /Subrs literal and a comment marker. */
    MASKED_LITERAL
  }

  public static byte[] largeType1Subrs(long subrs, Type1Shape shape) throws IOException {
    return largeType1Subrs((shape == Type1Shape.PADDED_COUNT ? "0000000000000000000000000000000000000000" : "") + subrs, shape);
  }

  /** {@link #largeType1Subrs(long)} with the /Subrs count token written verbatim. */
  public static byte[] largeType1Subrs(String count) throws IOException {
    return largeType1Subrs(count, Type1Shape.PLAIN);
  }

  private static byte[] largeType1Subrs(String count, Type1Shape shape) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      page.setResources(resources);
      byte[] clear = type1Clear(shape == Type1Shape.DECOY_EEXEC);
      // the four leading random bytes are free: 0xD9 makes the first ciphertext byte 0x00 (0xD9 ^ (55665 >> 8))
      byte[] plain = ((shape == Type1Shape.LEADING_ZERO ? (char) 0xD9 + "bcd" : "abcd") + type1Private(shape == Type1Shape.MASKED_LITERAL ? MASK_ENTRY : "", " " + count))
          .getBytes(StandardCharsets.ISO_8859_1);
      byte[] encrypted = eexecEncrypt(plain);
      if (shape == Type1Shape.HEX) {
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < encrypted.length; i++) {
          hex.append(String.format("%02x", encrypted[i] & 0xFF)).append(i % 32 == 31 ? "\n" : "");
        }
        encrypted = hex.toString().getBytes(StandardCharsets.US_ASCII);
      }
      COSStream fontFile = doc.getDocument().createCOSStream();
      try (OutputStream out = fontFile.createRawOutputStream()) {
        if (shape == Type1Shape.PFB) {
          pfbRecord(out, 1, clear);
          pfbRecord(out, 2, encrypted);
          out.write(new byte[] {(byte) 0x80, 3});
        } else {
          out.write(clear);
          out.write(encrypted);
        }
      }
      int header = shape == Type1Shape.PFB ? 6 : 0;
      fontFile.setInt(COSName.LENGTH1, clear.length + header);
      fontFile.setInt(COSName.LENGTH2, encrypted.length + header);
      fontFile.setInt(COSName.LENGTH3, 0);
      COSDictionary descriptor = new COSDictionary();
      descriptor.setItem(COSName.TYPE, COSName.FONT_DESC);
      descriptor.setItem(COSName.FONT_NAME, COSName.getPDFName("Fill"));
      descriptor.setInt(COSName.FLAGS, 32);
      descriptor.setItem(COSName.FONT_BBOX, rect(0, 0, 1000, 1000));
      descriptor.setItem(COSName.FONT_FILE, fontFile);
      COSDictionary font = new COSDictionary();
      font.setItem(COSName.TYPE, COSName.FONT);
      font.setItem(COSName.SUBTYPE, COSName.TYPE1);
      font.setItem(COSName.BASE_FONT, COSName.getPDFName("Fill"));
      font.setItem(COSName.FONT_DESC, descriptor);
      fonts(resources.getCOSObject()).setItem(F2, font);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (x) Tj ET\n"));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** The clear text of the fixtures' Type1 font file; {@code decoyEexec} adds a "%currentfile eexec" comment. */
  static byte[] type1Clear(boolean decoyEexec) {
    return ("%!PS-AdobeFont-1.0: Fill 001.000\n11 dict begin\n/FontName /Fill def\n/PaintType 0 def\n/FontType 1 def\n"
        + "/FontMatrix [0.001 0 0 0.001 0 0] readonly def\n/Encoding StandardEncoding def\n"
        + "/FontBBox {0 0 1000 1000} readonly def\n" + (decoyEexec ? "%currentfile eexec\n" : "")
        + "currentdict end\ncurrentfile eexec\n").getBytes(StandardCharsets.US_ASCII);
  }

  /**
   * The decrypted private part after the four random bytes: a /Private dict whose {@code /Subrs} literal is followed by
   * {@code afterLiteral} and {@code " array"}; no CharStrings.
   */
  static String type1Private(String afterLiteral) {
    return type1Private("", afterLiteral);
  }

  /**
   * A private dict entry fontbox reads as a string, holding a /Subrs literal then a comment marker; written on the line of
   * the real /Subrs key, which a comment would otherwise hide up to the line end.
   */
  static final String MASK_ENTRY = "/Mask (/Subrs %) def ";

  /** {@link #type1Private(String)} with {@code entriesBefore} written on the /Subrs line, just before the key. */
  static String type1Private(String entriesBefore, String afterLiteral) {
    return " dup /Private 8 dict dup begin\n/RD {string currentfile exch readstring pop} executeonly def\n"
        + "/ND {noaccess def} executeonly def\n/NP {noaccess put} executeonly def\n/lenIV 4 def\n" + entriesBefore + "/Subrs"
        + afterLiteral + " array\nND\nend\n";
  }

  /** An unfiltered stream of {@code bytes} zero bytes, written in 1 MiB pieces. */
  private static COSStream unfilteredZeros(PDDocument doc, long bytes) throws IOException {
    COSStream stream = doc.getDocument().createCOSStream();
    byte[] zeros = new byte[1 << 20];
    try (OutputStream out = stream.createRawOutputStream()) {
      for (long left = bytes; left > 0; left -= zeros.length) {
        out.write(zeros, 0, (int) Math.min(zeros.length, left));
      }
    }
    return stream;
  }

  /** Adobe Type 1 eexec encryption (r = 55665), the inverse of the decryption PDFBox's Type1Parser applies. */
  private static void pfbRecord(OutputStream out, int type, byte[] data) throws IOException {
    out.write(new byte[] {(byte) 0x80, (byte) type, (byte) data.length, (byte) (data.length >> 8), (byte) (data.length >> 16),
        (byte) (data.length >> 24)});
    out.write(data);
  }

  static byte[] eexecEncrypt(byte[] plain) {
    byte[] cipher = new byte[plain.length];
    int r = 55665;
    for (int i = 0; i < plain.length; i++) {
      int c = (plain[i] & 0xFF) ^ (r >> 8);
      cipher[i] = (byte) c;
      r = ((c + r) * 52845 + 22719) & 0xFFFF;
    }
    return cipher;
  }

  /** Page 2 of 3 holds a form XObject whose stream is DCT-encoded (a fake 30000x30000 JPEG header), refused undecoded. */
  public static byte[] dctForm() throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      page.setResources(resources);
      COSStream dct = rawStream(doc, new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, (byte) 0xC0, 0, 17, 8, 0x75, 0x30,
          0x75, 0x30, 3, 1, 0x22, 0, 2, 0x11, 1, 3, 0x11, 1});
      dct.setItem(COSName.FILTER, COSName.DCT_DECODE);
      markForm(dct, resources.getCOSObject());
      xobjects(resources.getCOSObject()).setItem(COSName.getPDFName("Img"), dct);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "/Img Do\n"));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3 uses a font whose /ToUnicode is CCITT-encoded, claiming 100 000 rows; refused undecoded. */
  public static byte[] ccittToUnicode() throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      page.setResources(resources);
      COSStream ccitt = rawStream(doc, new byte[64]);
      ccitt.setItem(COSName.FILTER, COSName.CCITTFAX_DECODE);
      COSDictionary params = new COSDictionary();
      params.setInt(COSName.COLUMNS, 100_000);
      params.setInt(COSName.ROWS, 100_000);
      ccitt.setItem(COSName.DECODE_PARMS, params);
      COSDictionary font = type1Helvetica();
      font.setItem(COSName.TO_UNICODE, ccitt);
      fonts(resources.getCOSObject()).setItem(F2, font);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (x) Tj ET\n"));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** Page 2 of 3 uses a Type3 font with {@code charProcs} small CharProcs (the per-font cap). */
  public static byte[] type3WithCharProcs(int charProcs) throws IOException {
    try (PDDocument doc = new PDDocument()) {
      addTextPage(doc, "First page text. It is fine.");
      PDPage page = new PDPage();
      doc.addPage(page);
      PDResources resources = helvetica();
      fonts(resources.getCOSObject()).setItem(F2, type3(doc, OversizedInput.LARGE_TYPE3_GLYPH_STREAM, 0, charProcs));
      page.setResources(resources);
      page.getCOSObject().setItem(COSName.CONTENTS, stream(doc, "BT /F2 10 Tf 50 650 Td (a) Tj ET\n"));
      addTextPage(doc, "Third page text. It is fine too.");
      return save(doc);
    }
  }

  /** A Type3 font; with {@code decodedBytes} > 0 one CharProc is the oversized stream (under /a, or under an unused name). */
  private static COSDictionary type3(PDDocument doc, OversizedInput kind, long decodedBytes, int charProcCount) throws IOException {
    COSDictionary font = new COSDictionary();
    font.setItem(COSName.TYPE, COSName.FONT);
    font.setItem(COSName.SUBTYPE, COSName.TYPE3);
    font.setItem(COSName.FONT_BBOX, rect(0, 0, 0, 0));
    COSArray matrix = new COSArray();
    for (float v : new float[] {0.001f, 0, 0, 0.001f, 0, 0}) {
      matrix.add(new COSFloat(v));
    }
    font.setItem(COSName.FONT_MATRIX, matrix);
    COSDictionary charProcs = new COSDictionary();
    for (int i = 0; i < charProcCount; i++) {
      charProcs.setItem(COSName.getPDFName(i == 0 ? "a" : "g" + i), stream(doc, "1000 0 0 0 750 750 d1 0 0 750 750 re f\n"));
    }
    if (decodedBytes > 0) {
      charProcs.setItem(COSName.getPDFName(kind == OversizedInput.LARGE_UNUSED_TYPE3_GLYPH_STREAM ? "unused" : "a"), oversizedFlateStream(doc, decodedBytes));
    }
    font.setItem(COSName.CHAR_PROCS, charProcs);
    COSDictionary encoding = new COSDictionary();
    COSArray differences = new COSArray();
    differences.add(COSInteger.get(97));
    differences.add(COSName.getPDFName("a"));
    encoding.setItem(COSName.DIFFERENCES, differences);
    font.setItem(COSName.ENCODING, encoding);
    if (kind != OversizedInput.LARGE_TYPE3_WIDTH_STREAM) {
      font.setInt(COSName.FIRST_CHAR, 97);
      font.setInt(COSName.LAST_CHAR, 97);
      COSArray widths = new COSArray();
      widths.add(COSInteger.get(1000));
      font.setItem(COSName.WIDTHS, widths);
    }
    return font;
  }

  /** A Flate stream decoding to {@code decodedBytes} zero bytes (deflate's ratio is about 1032:1). */
  public static COSStream oversizedFlateStream(PDDocument doc, long decodedBytes) throws IOException {
    return oversizedFlateStream(doc, decodedBytes, (byte) 0);
  }

  /** A Flate stream decoding to {@code decodedBytes} copies of {@code fill}. */
  public static COSStream oversizedFlateStream(PDDocument doc, long decodedBytes, byte fill) throws IOException {
    COSStream stream = doc.getDocument().createCOSStream();
    Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
    byte[] zeros = new byte[1 << 20];
    Arrays.fill(zeros, fill);
    byte[] out = new byte[1 << 16];
    try (OutputStream raw = stream.createRawOutputStream()) {
      long remaining = decodedBytes;
      while (remaining > 0) {
        int n = (int) Math.min(zeros.length, remaining);
        deflater.setInput(zeros, 0, n);
        remaining -= n;
        while (!deflater.needsInput()) {
          int c = deflater.deflate(out);
          raw.write(out, 0, c);
        }
      }
      deflater.finish();
      while (!deflater.finished()) {
        int c = deflater.deflate(out);
        raw.write(out, 0, c);
      }
    } finally {
      deflater.end();
    }
    stream.setItem(COSName.FILTER, COSName.FLATE_DECODE);
    return stream;
  }

  // ------------------------------------------------------------------ helpers

  static PDResources helvetica() {
    PDResources resources = new PDResources();
    resources.put(F1, new PDType1Font(Standard14Fonts.FontName.HELVETICA));
    return resources;
  }

  private static COSDictionary type1Helvetica() {
    COSDictionary font = new COSDictionary();
    font.setItem(COSName.TYPE, COSName.FONT);
    font.setItem(COSName.SUBTYPE, COSName.TYPE1);
    font.setItem(COSName.BASE_FONT, COSName.getPDFName("Helvetica"));
    return font;
  }

  private static COSDictionary trueTypeFont(COSStream fontFile2) {
    COSDictionary descriptor = new COSDictionary();
    descriptor.setItem(COSName.TYPE, COSName.FONT_DESC);
    descriptor.setItem(COSName.FONT_NAME, COSName.getPDFName("Fill"));
    descriptor.setInt(COSName.FLAGS, 32);
    descriptor.setItem(COSName.FONT_BBOX, rect(0, 0, 1000, 1000));
    descriptor.setItem(COSName.FONT_FILE2, fontFile2);
    COSDictionary font = new COSDictionary();
    font.setItem(COSName.TYPE, COSName.FONT);
    font.setItem(COSName.SUBTYPE, COSName.TRUE_TYPE);
    font.setItem(COSName.BASE_FONT, COSName.getPDFName("Fill"));
    font.setItem(COSName.FONT_DESC, descriptor);
    return font;
  }

  private static COSStream form(PDDocument doc, String ops, COSDictionary resources) throws IOException {
    COSStream form = stream(doc, ops);
    markForm(form, resources);
    return form;
  }

  private static void markForm(COSStream stream, COSDictionary resources) {
    stream.setItem(COSName.TYPE, COSName.XOBJECT);
    stream.setItem(COSName.SUBTYPE, COSName.FORM);
    stream.setItem(COSName.BBOX, rect(0, 0, 612, 792));
    stream.setItem(COSName.RESOURCES, resources);
  }

  private static COSDictionary xobjectResources(COSDictionary base, String name, COSStream xobject) {
    COSDictionary resources = new COSDictionary();
    resources.setItem(COSName.FONT, base.getItem(COSName.FONT));
    COSDictionary xobjects = new COSDictionary();
    xobjects.setItem(COSName.getPDFName(name), xobject);
    resources.setItem(COSName.XOBJECT, xobjects);
    return resources;
  }

  private static COSDictionary xobjects(COSDictionary resources) {
    COSDictionary xobjects = resources.getCOSDictionary(COSName.XOBJECT);
    if (xobjects == null) {
      xobjects = new COSDictionary();
      resources.setItem(COSName.XOBJECT, xobjects);
    }
    return xobjects;
  }

  private static COSDictionary fonts(COSDictionary resources) {
    COSDictionary fonts = resources.getCOSDictionary(COSName.FONT);
    if (fonts == null) {
      fonts = new COSDictionary();
      resources.setItem(COSName.FONT, fonts);
    }
    return fonts;
  }

  static COSStream stream(PDDocument doc, String ops) throws IOException {
    return rawStream(doc, ops.getBytes(StandardCharsets.ISO_8859_1));
  }

  private static byte[] rawBytes(COSStream stream) throws IOException {
    try (java.io.InputStream in = stream.createRawInputStream()) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int n;
      while ((n = in.read(buffer)) >= 0) {
        out.write(buffer, 0, n);
      }
      return out.toByteArray();
    }
  }

  /**
   * A hand-written PDF whose large embedded font and its /ToUnicode carry object numbers 2 000 000 001 / 2 000 000 002;
   * written without an xref table, so PDFBox reconstructs it by scanning for objects.
   */
  public static byte[] highObjectNumberOversizedStream(long decodedBytes) throws IOException {
    byte[] oversizedInput;
    try (PDDocument scratch = new PDDocument()) {
      oversizedInput = rawBytes(oversizedFlateStream(scratch, decodedBytes));
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    String head = "%PDF-1.7\n"
        + "1 0 obj <</Type /Catalog /Pages 2 0 R>> endobj\n"
        + "2 0 obj <</Type /Pages /Kids [3 0 R 5 0 R 7 0 R] /Count 3>> endobj\n"
        + "3 0 obj <</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources <</Font <</F1 9 0 R>>>> /Contents 4 0 R>> endobj\n"
        + stream("4 0", "BT /F1 10 Tf 50 700 Td (First page text. It is fine.) Tj ET")
        + "5 0 obj <</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources <</Font <</F1 9 0 R /F2 2000000001 0 R>>>> "
        + "/Contents 6 0 R>> endobj\n"
        + stream("6 0", "BT /F2 10 Tf 50 700 Td (x) Tj ET")
        + "7 0 obj <</Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources <</Font <</F1 9 0 R>>>> /Contents 8 0 R>> endobj\n"
        + stream("8 0", "BT /F1 10 Tf 50 700 Td (Third page text. It is fine too.) Tj ET")
        + "9 0 obj <</Type /Font /Subtype /Type1 /BaseFont /Helvetica>> endobj\n"
        + "2000000001 0 obj <</Type /Font /Subtype /Type1 /BaseFont /Helvetica /ToUnicode 2000000002 0 R>> endobj\n"
        + "2000000002 0 obj <</Length " + oversizedInput.length + " /Filter /FlateDecode>>\nstream\n";
    out.write(head.getBytes(StandardCharsets.ISO_8859_1));
    out.write(oversizedInput);
    out.write("\nendstream\nendobj\ntrailer <</Root 1 0 R>>\n%%EOF\n".getBytes(StandardCharsets.ISO_8859_1));
    return out.toByteArray();
  }

  private static String stream(String id, String ops) {
    return id + " obj <</Length " + ops.length() + ">>\nstream\n" + ops + "\nendstream\nendobj\n";
  }

  private static COSStream rawStream(PDDocument doc, byte[] bytes) throws IOException {
    COSStream stream = doc.getDocument().createCOSStream();
    try (OutputStream out = stream.createRawOutputStream()) {
      out.write(bytes);
    }
    return stream;
  }

  private static COSArray rect(float llx, float lly, float urx, float ury) {
    COSArray array = new COSArray();
    for (float v : new float[] {llx, lly, urx, ury}) {
      array.add(new COSFloat(v));
    }
    return array;
  }

  static String escape(String text) {
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)");
  }

  private static String repeat(String s, int n) {
    StringBuilder sb = new StringBuilder(s.length() * n);
    for (int i = 0; i < n; i++) {
      sb.append(s);
    }
    return sb.toString();
  }

  static byte[] save(PDDocument doc) throws IOException {
    return save(doc, DOCUMENT_ID);
  }

  /** {@link #save(PDDocument, long, OutputStream)} to a byte array. */
  static byte[] save(PDDocument doc, long documentId) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    save(doc, documentId, out);
    return out.toByteArray();
  }

  /**
   * Saves {@code doc} with {@code documentId} as PDFBox's document id and as the document's creation and modification time
   * (seconds since the epoch, UTC), so the trailer /ID, a hash of the id and of the Info dictionary that PDFBox otherwise
   * takes from the clock, and the Info dates are the same on every save: the bytes depend on the content only.
   */
  public static void save(PDDocument doc, long documentId, OutputStream out) throws IOException {
    doc.setDocumentId(documentId);
    Calendar at = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    at.setTimeInMillis(TimeUnit.SECONDS.toMillis(documentId));
    PDDocumentInformation info = doc.getDocumentInformation();
    info.setCreationDate(at);
    info.setModificationDate(at);
    doc.save(out);
  }
}
