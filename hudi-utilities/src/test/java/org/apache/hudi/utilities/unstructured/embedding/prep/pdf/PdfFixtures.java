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

import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.PDResources;
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

/**
 * Builds the PDF fixtures of the unit tests with PDFBox's writer, deterministically.
 * Text uses the standard Helvetica font (WinAnsi, so fixture text is ASCII).
 */
public final class PdfFixtures {

  /** The document id every fixture is saved with unless a caller passes its own: a fixed trailer /ID and fixed dates. */
  static final long DOCUMENT_ID = 42L;

  private static final COSName F1 = COSName.getPDFName("F1");

  private PdfFixtures() {
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

  // ------------------------------------------------------------------ helpers

  static PDResources helvetica() {
    PDResources resources = new PDResources();
    resources.put(F1, new PDType1Font(Standard14Fonts.FontName.HELVETICA));
    return resources;
  }

  static COSStream stream(PDDocument doc, String ops) throws IOException {
    return rawStream(doc, ops.getBytes(StandardCharsets.ISO_8859_1));
  }

  private static COSStream rawStream(PDDocument doc, byte[] bytes) throws IOException {
    COSStream stream = doc.getDocument().createCOSStream();
    try (OutputStream out = stream.createRawOutputStream()) {
      out.write(bytes);
    }
    return stream;
  }

  static String escape(String text) {
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)");
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
