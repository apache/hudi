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
import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdfparser.PDFStreamParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The decode guard checks the resources a page's content draws, not every resource the page lists: an oversized form
 * or font that is listed (directly or through inherited resources) but never drawn does not fail the page, while the same
 * entry drawn by {@code Do}, {@code Tf} or {@code gs} still does.
 */
class TestDecodeGuardDrawnResources {

  /** The guard's per-stream decoded-size cap in these tests; the oversized stream decodes to four times it. */
  private static final long CAP = 64 * 1024;
  private static final long OVERSIZED = 4 * CAP;
  private static final COSName BIG = COSName.getPDFName("Big");
  private static final COSName F2 = COSName.getPDFName("F2");
  private static final String TEXT = "BT /F1 10 Tf 50 700 Td (Page text.) Tj ET\n";

  @TempDir
  Path staging;

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @Test
  void fallbackTraversalStopsAtTheFontVisitRefusal() throws IOException {
    CountingDictionary fonts = fallbackFonts(StreamDecodeGuard.MAX_STREAMS_PER_PAGE + 1000, true);
    assertTrue(fonts.visited <= StreamDecodeGuard.MAX_STREAMS_PER_PAGE + 1,
        "enumerated " + fonts.visited + " entries before refusing the page");
  }

  @Test
  void smallFallbackDictionaryIsAccepted() throws IOException {
    assertEquals(16, fallbackFonts(16, false).visited);
  }

  private CountingDictionary fallbackFonts(int entries, boolean refused) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      document.addPage(page);
      PDResources resources = new PDResources();
      page.setResources(resources);
      CountingDictionary fonts = new CountingDictionary();
      COSBase font = fonts(PdfFixtures.helvetica()).getItem(COSName.getPDFName("F1"));
      for (int i = 0; i < entries; i++) {
        fonts.setItem(COSName.getPDFName("Fn" + i), font);
      }
      resources.getCOSObject().setItem(COSName.FONT, fonts);
      StringBuilder ops = new StringBuilder();
      for (int i = 0; i <= DrawnResources.MAX_NAMES; i++) {
        ops.append("/Fn").append(i).append(" 12 Tf\n");
      }
      setContents(document, page, ops.toString());
      StreamDecodeGuard guard = new StreamDecodeGuard(CAP, staging.toFile());
      if (refused) {
        assertEquals(PdfFailureCode.PAGE_ERROR, assertThrows(PageFailure.class, () -> guard.checkPage(page)).code());
      } else {
        assertDoesNotThrow(() -> guard.checkPage(page));
      }
      return fonts;
    }
  }

  /** Counts enumeration on the real dictionary, including copies made before resource visits. */
  private static final class CountingDictionary extends COSDictionary {
    private int visited;

    @Override
    public Set<COSName> keySet() {
      Set<COSName> keys = super.keySet();
      return new AbstractSet<COSName>() {
        @Override
        public Iterator<COSName> iterator() {
          Iterator<COSName> iterator = keys.iterator();
          return new Iterator<COSName>() {
            @Override
            public boolean hasNext() {
              return iterator.hasNext();
            }

            @Override
            public COSName next() {
              visited++;
              return iterator.next();
            }
          };
        }

        @Override
        public int size() {
          return keys.size();
        }
      };
    }
  }

  @Test
  void undrawnOversizedFormDoesNotFailThePage() throws IOException {
    byte[] pdf = build((document, page) -> {
      xobjects(page.getResources()).setItem(BIG, oversizedForm(document));
      setContents(document, page, TEXT);
    });
    assertDoesNotThrow(() -> check(pdf, 0));
  }

  @Test
  void drawnOversizedFormStillFailsThePage() throws IOException {
    byte[] pdf = build((document, page) -> {
      xobjects(page.getResources()).setItem(BIG, oversizedForm(document));
      setContents(document, page, "q /Big Do Q " + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 0)).code());
  }

  @Test
  void undrawnOversizedFormInInheritedResourcesFailsNoPage() throws IOException {
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      PDResources inherited = PdfFixtures.helvetica();
      xobjects(inherited).setItem(BIG, oversizedForm(document));
      for (int p = 0; p < 3; p++) {
        PDPage page = new PDPage();
        document.addPage(page);
        setContents(document, page, p == 2 ? "/Big Do " + TEXT : TEXT);
      }
      document.getPages().getCOSObject().setItem(COSName.RESOURCES, inherited.getCOSObject());
      pdf = PdfFixtures.save(document);
    }
    assertDoesNotThrow(() -> check(pdf, 0));
    assertDoesNotThrow(() -> check(pdf, 1));
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 2)).code(),
        "the one page that draws the inherited form is still refused");
  }

  @Test
  void undrawnOversizedFontDoesNotFailThePageButADrawnOneDoes() throws IOException {
    byte[] undrawn = build((document, page) -> {
      fonts(page.getResources()).setItem(F2, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
      setContents(document, page, TEXT);
    });
    assertDoesNotThrow(() -> check(undrawn, 0));
    byte[] drawn = build((document, page) -> {
      fonts(page.getResources()).setItem(F2, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
      setContents(document, page, TEXT + "BT /F2 10 Tf 50 650 Td (x) Tj ET\n");
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(drawn, 0)).code());
  }

  @Test
  void graphicsStateFontIsCheckedOnlyWhenTheStateIsSet() throws IOException {
    byte[] undrawn = build((document, page) -> {
      page.getResources().getCOSObject().setItem(COSName.EXT_G_STATE, stateWithFont(document));
      setContents(document, page, TEXT);
    });
    assertDoesNotThrow(() -> check(undrawn, 0));
    byte[] drawn = build((document, page) -> {
      page.getResources().getCOSObject().setItem(COSName.EXT_G_STATE, stateWithFont(document));
      setContents(document, page, "/GS1 gs " + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(drawn, 0)).code());
  }

  @Test
  void formWithoutResourcesDrawsFromItsCallersResources() throws IOException {
    // the page draws /Fm, which has no /Resources, so its "/F2 Tf" resolves in the page's resources (PDFStreamEngine)
    byte[] pdf = build((document, page) -> {
      COSStream form = PdfFixtures.stream(document, "BT /F2 10 Tf 0 0 Td (x) Tj ET\n");
      markForm(form);
      xobjects(page.getResources()).setItem(COSName.getPDFName("Fm"), form);
      fonts(page.getResources()).setItem(F2, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
      setContents(document, page, "/Fm Do " + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 0)).code());
  }

  @Test
  void formReachedThroughTwoResourceDictionariesIsCheckedInBoth() throws IOException {
    // /Fm has no /Resources. Drawn first from the page (whose /F2 is small), then from form /Outer (whose /F2 is the
    // oversized font): the second reach resolves /F2 elsewhere and must be checked again.
    byte[] pdf = build((document, page) -> {
      COSStream inner = PdfFixtures.stream(document, "BT /F2 10 Tf 0 0 Td (x) Tj ET\n");
      markForm(inner);
      COSDictionary outerResources = new COSDictionary();
      COSDictionary outerXObjects = new COSDictionary();
      outerXObjects.setItem(COSName.getPDFName("Fm"), inner);
      outerResources.setItem(COSName.XOBJECT, outerXObjects);
      COSDictionary outerFonts = new COSDictionary();
      outerFonts.setItem(F2, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
      outerResources.setItem(COSName.FONT, outerFonts);
      COSStream outer = PdfFixtures.stream(document, "/Fm Do\n");
      markForm(outer);
      outer.setItem(COSName.RESOURCES, outerResources);
      xobjects(page.getResources()).setItem(COSName.getPDFName("Fm"), inner);
      xobjects(page.getResources()).setItem(COSName.getPDFName("Outer"), outer);
      fonts(page.getResources()).setItem(F2, fonts(page.getResources()).getItem(COSName.getPDFName("F1")));
      setContents(document, page, "/Fm Do /Outer Do " + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 0)).code());
  }

  @Test
  void checkingAFormLeavesItsDictionaryUnchanged() throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      document.addPage(page);
      COSStream form = PdfFixtures.stream(document, TEXT);
      form.setItem(COSName.SUBTYPE, COSName.FORM);                  // no /Type: optional for a form XObject
      xobjects(page.getResources()).setItem(COSName.getPDFName("Fm"), form);
      setContents(document, page, "/Fm Do\n");
      new StreamDecodeGuard(CAP, staging.toFile()).checkPage(page, 1);
      assertEquals(null, form.getItem(COSName.TYPE), "the guard reads the form, it does not edit it");
    }
  }

  @Test
  void escapedNameResolvesToTheEntryItNames() throws IOException {
    byte[] pdf = build((document, page) -> {
      xobjects(page.getResources()).setItem(BIG, oversizedForm(document));
      setContents(document, page, "/B#69g Do " + TEXT);              // #69 is 'i'
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 0)).code());
  }

  @Test
  void longNameResolvesExactly() throws IOException {
    StringBuilder longName = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      longName.append('n');
    }
    byte[] drawn = build((document, page) -> {
      xobjects(page.getResources()).setItem(COSName.getPDFName(longName.toString()), oversizedForm(document));
      setContents(document, page, "/" + longName + " Do " + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(drawn, 0)).code());
    byte[] other = build((document, page) -> {
      xobjects(page.getResources()).setItem(BIG, oversizedForm(document));
      setContents(document, page, "/" + longName + " Do " + TEXT);       // draws a name the page does not list
    });
    assertDoesNotThrow(() -> check(other, 0));
  }

  @Test
  void collectorRecordsTheFirstOperandOfDoTfAndGs() throws IOException {
    byte[] content = ("q /Fm0 Do Q BT /X /F1 12 Tf /GS1 gs [/F9 1] TJ (/F8) Tj /F7 12 TL ET\n"
        + "/Fm1 Do /Im#31 Do").getBytes(StandardCharsets.US_ASCII);
    DrawnResources drawn = DrawnResources.collect(new PDFStreamParser(content));
    assertFalse(drawn.drawsEverything());
    assertTrue(drawn.draws(DrawnResources.Category.XOBJECT, COSName.getPDFName("Fm0")));
    assertTrue(drawn.draws(DrawnResources.Category.XOBJECT, COSName.getPDFName("Fm1")));
    assertTrue(drawn.draws(DrawnResources.Category.XOBJECT, COSName.getPDFName("Im1")));
    assertTrue(drawn.draws(DrawnResources.Category.FONT, COSName.getPDFName("X")), "PDFBox reads Tf's first operand");
    assertFalse(drawn.draws(DrawnResources.Category.FONT, COSName.getPDFName("F1")));
    assertTrue(drawn.draws(DrawnResources.Category.EXT_G_STATE, COSName.getPDFName("GS1")));
    for (String name : new String[] {"F7", "F8", "F9"}) {
      assertFalse(drawn.draws(DrawnResources.Category.FONT, COSName.getPDFName(name)), name);
    }
    assertEquals(5, drawn.size());
  }

  @Test
  void skippedUndecodableContentStreamIsLoggedOncePerPage() throws IOException {
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      for (int p = 0; p < 2; p++) {
        PDPage page = new PDPage();
        page.setResources(PdfFixtures.helvetica());
        document.addPage(page);
        COSArray contents = new COSArray();
        contents.add(PdfFixtures.stream(document, TEXT));
        if (p == 1) {
          for (int i = 0; i < 2; i++) {
            COSStream broken = PdfFixtures.stream(document, "BT /F1 10 Tf 50 650 Td (Lost text.) Tj ET\n");
            broken.setItem(COSName.FILTER, COSName.getPDFName("UnknownDecode"));
            contents.add(broken);
          }
        }
        page.getCOSObject().setItem(COSName.CONTENTS, contents);
      }
      pdf = PdfFixtures.save(document);
    }
    List<String> warnings = new ArrayList<>();
    try (PDDocument document = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(CAP, CAP, staging.toFile(), warnings::add);
      guard.checkPage(document.getPage(0), 1);
      assertEquals(List.of(), warnings, "a page that loses nothing logs nothing");
      guard.checkPage(document.getPage(1), 2);
    }
    assertEquals(1, warnings.size(), warnings.toString());
    String warning = warnings.get(0);
    assertTrue(warning.startsWith("PDF page 2: skipped 2 content stream(s) that cannot be decoded"), warning);
    assertTrue(warning.contains("stream "), warning);
    assertTrue(warning.contains("missing from this page's extraction"), warning);
  }

  /**
   * Content PDFStreamParser tokenizes in unusual ways: a stray {@code )}, {@code >} or {@code }}, braces, a name containing
   * a brace, a nested literal string PDFBox ends early at {@code )} + line break, dictionaries and arrays PDFBox
   * ends early, inline-image dictionaries and {@code ID} data. PDFBox draws the oversized form named in the second
   * column; the guard reads names with PDFBox's parser, so it must refuse the page.
   */
  @ParameterizedTest
  @CsvSource(delimiter = '|', value = {
      "/A ) /Big Do | Big",
      "/A > /Big Do | Big",
      "/A } /Big Do | Big",
      "{ /Big Do | Big",
      "/A n{ /Big Do | Big",
      "/Big} Do | Big}",
      "/Big{x Do | Big{x",
      "((x)\\n/A Q /Big Do | Big",
      "/A >> /Big Do | Big",
      "<< /K x >> /Big Do | Big",
      "<< ( >> ) /Big Do | Big",
      "[ x [ ] Q /Big Do ] | Big",
      "[ endobj /Big Do | Big",
      "BI 5 /Big Do | Big",
      "BI /K 1 2 /Big Do | Big",
      "IDx ( EI Q /Big Do | Big"})
  void contentPdfBoxTokenizesUnusuallyStillRefusesTheDrawnForm(String ops, String drawnName) throws IOException {
    String content = ops.replace("\\n", "\n");
    assertEquals(drawnName, pdfBoxDoOperand(content), "PDFBox resolves this XObject for Do");
    byte[] pdf = build((document, page) -> {
      xobjects(page.getResources()).setItem(COSName.getPDFName(drawnName), oversizedForm(document));
      setContents(document, page, content + "\n" + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 0)).code());
  }

  @Test
  void formWhoseResourcesAreNotADictionaryDrawsNothingFromItsCaller() throws IOException {
    // PDFormXObject#getResources: a /Resources key that is not a dictionary gives empty resources (PDFBOX-4372), so the
    // form's "/F2 Tf" never reaches the page's oversized /F2
    byte[] pdf = build((document, page) -> {
      COSStream form = PdfFixtures.stream(document, "BT /F2 10 Tf 0 0 Td (x) Tj ET\n");
      markForm(form);
      form.setItem(COSName.RESOURCES, COSInteger.get(7));
      xobjects(page.getResources()).setItem(COSName.getPDFName("Fm"), form);
      fonts(page.getResources()).setItem(F2, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
      setContents(document, page, "/Fm Do " + TEXT);
    });
    assertDoesNotThrow(() -> check(pdf, 0));
  }

  @Test
  void directFontSelectedFromTwoFormsIsChargedForEachSelection() throws IOException {
    // PDFBox caches only indirect fonts: each form's Tf on the one direct font dictionary builds another copy
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      document.addPage(page);
      COSDictionary shared = new COSDictionary();
      COSDictionary sharedFonts = new COSDictionary();
      sharedFonts.setItem(F2, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, 3000)));
      shared.setItem(COSName.FONT, sharedFonts);
      for (String name : new String[] {"Fa", "Fb"}) {
        COSStream form = PdfFixtures.stream(document, "BT /F2 10 Tf 0 0 Td (x) Tj ET\n");
        markForm(form);
        form.setItem(COSName.RESOURCES, shared);
        xobjects(page.getResources()).setItem(COSName.getPDFName(name), form);
      }
      setContents(document, page, "/Fa Do /Fb Do " + TEXT);
      StreamDecodeGuard guard = new StreamDecodeGuard(CAP, 4000, staging.toFile());
      PageFailure failure = assertThrows(PageFailure.class, () -> guard.checkPage(page, 1));
      assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
      assertTrue(failure.getMessage().contains("sum=6000"), failure.getMessage());
    }
  }

  @Test
  void glyphProceduresAreNotKeptInTheDrawnNamesCache() throws IOException {
    byte[] pdf = PdfFixtures.type3WithCharProcs(3);
    try (PDDocument document = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(CAP, staging.toFile());
      guard.checkPage(document.getPage(1), 2);
      assertEquals(0, guard.cachedDrawnStreams(), "a Type3 font's CharProcs are never looked up by name");
    }
  }

  @Test
  void skippedStreamListedTwiceIsNamedOnceWithItsDecodeError() throws IOException {
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      document.addPage(page);
      COSStream broken = PdfFixtures.stream(document, "BT /F1 10 Tf 50 650 Td (Lost text.) Tj ET\n");
      StringBuilder longName = new StringBuilder("Unknown");
      for (int i = 0; i < 100; i++) {
        longName.append("Decode");
      }
      broken.setItem(COSName.FILTER, COSName.getPDFName(longName.toString()));
      COSArray contents = new COSArray();
      contents.add(PdfFixtures.stream(document, TEXT));
      contents.add(broken);
      contents.add(broken);
      page.getCOSObject().setItem(COSName.CONTENTS, contents);
      pdf = PdfFixtures.save(document);
    }
    List<String> warnings = new ArrayList<>();
    try (PDDocument document = Loader.loadPDF(pdf)) {
      new StreamDecodeGuard(CAP, CAP, staging.toFile(), warnings::add).checkPage(document.getPage(0), 1);
    }
    assertEquals(1, warnings.size(), warnings.toString());
    String warning = warnings.get(0);
    assertTrue(warning.startsWith("PDF page 1: skipped 1 content stream(s) that cannot be decoded"), warning);
    assertFalse(warning.contains("earlier page"), warning);
    assertTrue(warning.length() < 600, "decode errors are truncated: " + warning.length());
  }

  /**
   * A large resource dictionary shared through inheritance by many pages, of which each page draws one font, one form and
   * one graphics state: the walk looks up those entries, it does not visit the whole dictionary on every page.
   */
  @Test
  void largeSharedDictionaryCostsTheDrawnEntriesNotItsSize() throws IOException {
    int entries = 2000;
    int pages = 50;
    byte[] pdf;
    try (PDDocument document = new PDDocument()) {
      PDResources shared = PdfFixtures.helvetica();
      COSDictionary fontsInShared = fonts(shared);
      COSDictionary xobjectsInShared = xobjects(shared);
      COSDictionary states = new COSDictionary();
      COSBase helvetica = fontsInShared.getItem(COSName.getPDFName("F1"));
      for (int i = 0; i < entries; i++) {
        fontsInShared.setItem(COSName.getPDFName("Fn" + i), helvetica);
        COSStream form = PdfFixtures.stream(document, "0 0 m\n");
        markForm(form);
        xobjectsInShared.setItem(COSName.getPDFName("Fm" + i), form);
        states.setItem(COSName.getPDFName("GS" + i), new COSDictionary());
      }
      shared.getCOSObject().setItem(COSName.EXT_G_STATE, states);
      for (int p = 0; p < pages; p++) {
        PDPage page = new PDPage();
        document.addPage(page);
        setContents(document, page, "/GS7 gs /Fm7 Do BT /Fn7 10 Tf 50 700 Td (Page text.) Tj ET\n");
      }
      document.getPages().getCOSObject().setItem(COSName.RESOURCES, shared.getCOSObject());
      pdf = PdfFixtures.save(document);
    }
    try (PDDocument document = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(CAP, staging.toFile());
      for (int p = 0; p < pages; p++) {
        guard.checkPage(document.getPage(p), p + 1);
      }
      assertTrue(guard.resourceEntriesVisited() <= 3L * pages, "visited " + guard.resourceEntriesVisited()
          + " resource entries for " + pages + " pages drawing 3 entries each of a " + 3 * entries + "-entry dictionary");
    }
  }

  /**
   * A resource name whose bytes are not UTF-8 (/X#E9 is X and the single byte E9): PDFBox resolves it by its bytes, so the
   * guard must too, or the entry it draws goes unchecked.
   */
  @Test
  void drawnNameThatIsNotUtf8IsCheckedLikeAnyOther() throws IOException {
    COSName xobject = COSName.getPDFName(new byte[] {'X', (byte) 0xE9});
    byte[] form = build((document, page) -> {
      xobjects(page.getResources()).setItem(xobject, oversizedForm(document));
      setContents(document, page, "/X#E9 Do " + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(form, 0)).code());
    COSName font = COSName.getPDFName(new byte[] {'F', (byte) 0xE9});
    byte[] fontPage = build((document, page) -> {
      fonts(page.getResources()).setItem(font, toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
      setContents(document, page, TEXT + "BT /F#E9 10 Tf 50 650 Td (x) Tj ET\n");
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(fontPage, 0)).code());
  }

  @Test
  void pageNamingMoreThanTheNameBudgetChecksEveryEntry() throws IOException {
    // past DrawnResources.MAX_NAMES the record means every resource: the walk then checks each entry, undrawn ones too
    StringBuilder ops = new StringBuilder();
    for (int i = 0; i <= DrawnResources.MAX_NAMES; i++) {
      ops.append("/X").append(i).append(" Do\n");
    }
    byte[] pdf = build((document, page) -> {
      xobjects(page.getResources()).setItem(BIG, oversizedForm(document));
      setContents(document, page, ops + TEXT);
    });
    assertEquals(PdfFailureCode.DECODE_LIMIT, assertThrows(PageFailure.class, () -> check(pdf, 0)).code());
  }

  // ------------------------------------------------------------------ helpers

  /** The first operand PDFBox's own content parser hands to the first {@code Do} of {@code content}. */
  private static String pdfBoxDoOperand(String content) throws IOException {
    PDFStreamParser parser = new PDFStreamParser(content.getBytes(StandardCharsets.ISO_8859_1));
    List<COSBase> operands = new ArrayList<>();
    for (Object token = parser.parseNextToken(); token != null; token = parser.parseNextToken()) {
      if (token instanceof Operator) {
        if ("Do".equals(((Operator) token).getName())) {
          return operands.isEmpty() || !(operands.get(0) instanceof COSName) ? null : ((COSName) operands.get(0)).getName();
        }
        operands.clear();
      } else {
        operands.add((COSBase) token);
      }
    }
    return null;
  }

  private interface PageBuilder {
    void build(PDDocument document, PDPage page) throws IOException;
  }

  /** One page with Helvetica as /F1, shaped by {@code builder}. */
  private static byte[] build(PageBuilder builder) throws IOException {
    try (PDDocument document = new PDDocument()) {
      PDPage page = new PDPage();
      page.setResources(PdfFixtures.helvetica());
      document.addPage(page);
      builder.build(document, page);
      return PdfFixtures.save(document);
    }
  }

  private void check(byte[] pdf, int index) throws IOException {
    try (PDDocument document = Loader.loadPDF(pdf)) {
      new StreamDecodeGuard(CAP, staging.toFile()).checkPage(document.getPage(index), index + 1);
    }
  }

  private static void setContents(PDDocument document, PDPage page, String ops) throws IOException {
    page.getCOSObject().setItem(COSName.CONTENTS, PdfFixtures.stream(document, ops));
  }

  private static COSStream oversizedForm(PDDocument document) throws IOException {
    COSStream form = PdfFixtures.oversizedFlateStream(document, OVERSIZED);
    markForm(form);
    return form;
  }

  private static void markForm(COSStream stream) {
    stream.setItem(COSName.TYPE, COSName.XOBJECT);
    stream.setItem(COSName.SUBTYPE, COSName.FORM);
    COSArray box = new COSArray();
    for (int v : new int[] {0, 0, 612, 792}) {
      box.add(COSInteger.get(v));
    }
    stream.setItem(COSName.BBOX, box);
  }

  private static COSDictionary toUnicodeFont(COSStream toUnicode) {
    COSDictionary font = new COSDictionary();
    font.setItem(COSName.TYPE, COSName.FONT);
    font.setItem(COSName.SUBTYPE, COSName.TYPE1);
    font.setItem(COSName.BASE_FONT, COSName.getPDFName("Helvetica"));
    font.setItem(COSName.TO_UNICODE, toUnicode);
    return font;
  }

  private static COSDictionary stateWithFont(PDDocument document) throws IOException {
    COSArray fontRef = new COSArray();
    fontRef.add(toUnicodeFont(PdfFixtures.oversizedFlateStream(document, OVERSIZED)));
    fontRef.add(COSInteger.get(10));
    COSDictionary state = new COSDictionary();
    state.setItem(COSName.FONT, fontRef);
    COSDictionary states = new COSDictionary();
    states.setItem(COSName.getPDFName("GS1"), state);
    return states;
  }

  private static COSDictionary xobjects(PDResources resources) {
    return subDictionary(resources.getCOSObject(), COSName.XOBJECT);
  }

  private static COSDictionary fonts(PDResources resources) {
    return subDictionary(resources.getCOSObject(), COSName.FONT);
  }

  private static COSDictionary subDictionary(COSDictionary resources, COSName key) {
    COSDictionary sub = resources.getCOSDictionary(key);
    if (sub == null) {
      sub = new COSDictionary();
      resources.setItem(key, sub);
    }
    return sub;
  }
}
