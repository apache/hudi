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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Top-level content streams skipped by PDFBox must produce durable loss from PDF introduction onward. */
class TestTopLevelDecodeLoss {
  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @Test
  void validTopLevelTextHasNoLoss() throws Exception {
    List<PdfChunk> chunks = chunks(false, true);
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.OK, chunks.get(0).status());
    assertTrue(chunks.get(0).text().contains("Readable text."));
    assertNull(chunks.get(0).statusDetail());
  }

  @Test
  void emptyPageIsNoTextWithoutLoss() throws Exception {
    List<PdfChunk> chunks = chunks(false, false);
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.NO_TEXT, chunks.get(0).status());
    assertNull(chunks.get(0).statusDetail());
  }

  @Test
  void undecodableTopLevelStreamIsAFailureInsteadOfNoText() throws Exception {
    List<PdfChunk> chunks = chunks(true, false);
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("DECODE_ERROR:"), chunks.get(0).statusDetail());
  }

  @Test
  void mixedContentsKeepReadableTextAndDurableLoss() throws Exception {
    List<PdfChunk> chunks = chunks(true, true);
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.OK, chunks.get(0).status());
    assertTrue(chunks.get(0).text().contains("Readable text."));
    assertTrue(chunks.get(0).statusDetail() != null && chunks.get(0).statusDetail().contains("PAGES_LOST"),
        chunks.get(0).statusDetail());
  }

  private static List<PdfChunk> chunks(boolean unreadable, boolean readable) throws IOException {
    byte[] pdf;
    try (PDDocument document = new PDDocument(); ByteArrayOutputStream saved = new ByteArrayOutputStream()) {
      PDPage page = new PDPage();
      document.addPage(page);
      PDResources resources = new PDResources();
      resources.put(COSName.getPDFName("F1"), new PDType1Font(Standard14Fonts.FontName.HELVETICA));
      page.setResources(resources);
      COSArray contents = new COSArray();
      if (readable) {
        contents.add(stream(document, "BT /F1 12 Tf 50 700 Td (Readable text.) Tj ET"));
      }
      if (unreadable) {
        COSStream broken = stream(document, "unreadable stream");
        broken.setItem(COSName.FILTER, COSName.getPDFName("UnknownDecode"));
        contents.add(broken);
      }
      if (contents.size() == 1) {
        page.getCOSObject().setItem(COSName.CONTENTS, contents.get(0));
      } else if (contents.size() > 1) {
        page.getCOSObject().setItem(COSName.CONTENTS, contents);
      }
      document.save(saved);
      pdf = saved.toByteArray();
    }
    List<PdfChunk> chunks = new ArrayList<>();
    PdfChunkerConfig config = PdfChunkerConfig.from(new TypedProperties(), new WordCountTokenizer());
    try (ClosableIterator<PdfChunk> iterator = new PdfBoxChunker().open(() -> new ByteArrayInputStream(pdf),
        new PdfFileMeta("top-level-loss", "memory.pdf", pdf.length), config)) {
      iterator.forEachRemaining(chunks::add);
    }
    return chunks;
  }

  private static COSStream stream(PDDocument document, String text) throws IOException {
    COSStream stream = document.getDocument().createCOSStream();
    try (OutputStream output = stream.createRawOutputStream()) {
      output.write(text.getBytes(StandardCharsets.US_ASCII));
    }
    return stream;
  }
}
