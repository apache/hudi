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

package org.apache.hudi.utilities.sources.helpers.unstructured;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the never-throw contract and status mapping of the Tika-backed parser.
 * Fixtures are generated in-line (no binary files in the repo).
 */
public class TestTikaDocumentParser {

  private final TikaDocumentParser parser = new TikaDocumentParser();

  private ParseResult parse(byte[] content, String fileName, int maxChars) {
    return parser.parse(new ByteArrayInputStream(content), fileName, maxChars);
  }

  @Test
  public void testPlainTextAndHtmlSuccessWithMetadata() {
    ParseResult text = parse("hudi ingests unstructured data".getBytes(StandardCharsets.UTF_8),
        "note.txt", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, text.getStatus());
    assertTrue(text.getText().contains("unstructured"));
    assertFalse(text.getMetadata().isEmpty()); // content-type at minimum

    ParseResult html = parse("<html><title>t</title><body><p>lakehouse body</p></body></html>"
        .getBytes(StandardCharsets.UTF_8), "page.html", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, html.getStatus());
    assertTrue(html.getText().contains("lakehouse body"));
    assertFalse(html.getText().contains("<p>")); // markup stripped
  }

  @Test
  public void testTruncationAtCharCap() {
    byte[] longText = new byte[5000];
    java.util.Arrays.fill(longText, (byte) 'a');
    ParseResult result = parse(longText, "big.txt", 100);
    assertEquals(ParseResult.ParseStatus.TRUNCATED, result.getStatus());
    assertTrue(result.getText().length() <= 100 + 1);
  }

  @Test
  public void testPdfWithTextLayerExtractsTextAndMetadata() {
    ParseResult pdf = parse(minimalPdf("Espresso extraction physics for lakehouses"),
        "doc.pdf", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, pdf.getStatus());
    assertTrue(pdf.getText().contains("Espresso extraction physics"));
    assertTrue(pdf.getMetadata().getOrDefault("Content-Type", "").contains("pdf"));
  }

  @Test
  public void testDocxExtractsText() {
    ParseResult docx = parse(minimalDocx("Coral reef ecology field notes"), "doc.docx", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, docx.getStatus());
    assertTrue(docx.getText().contains("Coral reef ecology"));
  }

  @Test
  public void testMarkdownAndCsvParseAsText() {
    ParseResult md = parse("# Heading\n\nBody with *emphasis*".getBytes(StandardCharsets.UTF_8),
        "notes.md", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, md.getStatus());
    assertTrue(md.getText().contains("Body with"));

    ParseResult csv = parse("city,count\nparis,2".getBytes(StandardCharsets.UTF_8),
        "table.csv", 1000);
    assertEquals(ParseResult.ParseStatus.SUCCESS, csv.getStatus());
    assertTrue(csv.getText().contains("paris"));
  }

  @Test
  public void testCorruptAndBinaryInputsNeverThrow() {
    // Claims to be PDF (magic bytes) but is corrupt: parser error -> FAILED, not thrown
    ParseResult corrupt = parse("%PDF-1.4 this is not really a pdf".getBytes(StandardCharsets.UTF_8),
        "corrupt.pdf", 1000);
    assertEquals(ParseResult.ParseStatus.FAILED, corrupt.getStatus());
    assertNotNull(corrupt.getError());

    // Unrecognizable binary: no text, no error -> EMPTY
    ParseResult binary = parse(new byte[] {0x00, 0x11, 0x22, 0x33, (byte) 0xff}, "blob.bin", 1000);
    assertEquals(ParseResult.ParseStatus.EMPTY, binary.getStatus());
    assertEquals("", binary.getText());
  }

  /**
   * Smallest well-formed single-page PDF with a real text layer.
   */
  private static byte[] minimalPdf(String text) {
    byte[] stream = ("BT /F1 12 Tf 50 750 Td (" + text + ") Tj ET").getBytes(StandardCharsets.UTF_8);
    String[] objects = {
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R "
            + "/Resources << /Font << /F1 5 0 R >> >> >>",
        "<< /Length " + stream.length + " >>\nstream\n" + new String(stream, StandardCharsets.UTF_8)
            + "\nendstream",
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"};
    StringBuilder pdf = new StringBuilder("%PDF-1.4\n");
    int[] offsets = new int[objects.length];
    for (int i = 0; i < objects.length; i++) {
      offsets[i] = pdf.length();
      pdf.append(i + 1).append(" 0 obj\n").append(objects[i]).append("\nendobj\n");
    }
    int xref = pdf.length();
    pdf.append("xref\n0 ").append(objects.length + 1).append("\n0000000000 65535 f \n");
    for (int offset : offsets) {
      pdf.append(String.format("%010d 00000 n \n", offset));
    }
    pdf.append("trailer\n<< /Size ").append(objects.length + 1)
        .append(" /Root 1 0 R >>\nstartxref\n").append(xref).append("\n%%EOF");
    return pdf.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Smallest well-formed DOCX (OOXML zip with content types, relationships, one paragraph).
   */
  private static byte[] minimalDocx(String text) {
    String contentTypes = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
        + "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
        + "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
        + "<Override PartName=\"/word/document.xml\" "
        + "ContentType=\"application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml\"/></Types>";
    String rels = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        + "<Relationship Id=\"rId1\" "
        + "Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" "
        + "Target=\"word/document.xml\"/></Relationships>";
    String document = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        + "<w:document xmlns:w=\"http://schemas.openxmlformats.org/wordprocessingml/2006/main\">"
        + "<w:body><w:p><w:r><w:t>" + text + "</w:t></w:r></w:p></w:body></w:document>";
    try {
      java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
      try (java.util.zip.ZipOutputStream zip = new java.util.zip.ZipOutputStream(buffer)) {
        for (String[] entry : new String[][] {
            {"[Content_Types].xml", contentTypes}, {"_rels/.rels", rels}, {"word/document.xml", document}}) {
          zip.putNextEntry(new java.util.zip.ZipEntry(entry[0]));
          zip.write(entry[1].getBytes(StandardCharsets.UTF_8));
          zip.closeEntry();
        }
      }
      return buffer.toByteArray();
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }
}
