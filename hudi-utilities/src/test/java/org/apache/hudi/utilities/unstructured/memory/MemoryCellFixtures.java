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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.common.PDStream;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;

import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/**
 * Inputs of the memory cells: a text PDF of about a given size, written page by page, and a PDF of a few text pages with a large
 * unreferenced binary stream, written as a stream.
 */
public final class MemoryCellFixtures {

  private static final String[] WORDS = ("lake table record index stream chunk page file vector model search data commit timeline "
      + "the of and to in is that for it as with by on").split(" ");

  private MemoryCellFixtures() {
  }

  /**
   * Writes a text PDF of about {@code targetBytes} to {@code path}: pages of {@code linesPerPage} lines of seeded words, each
   * content stream uncompressed, so the file is as large as its text.
   *
   * @return the number of pages
   */
  public static int writeTextPdf(Path path, long targetBytes, int linesPerPage, long seed) throws IOException {
    Random random = new Random(seed);
    int pages = 0;
    try (PDDocument doc = new PDDocument()) {
      PDResources resources = new PDResources();
      resources.put(COSName.getPDFName("F1"), new PDType1Font(Standard14Fonts.FontName.HELVETICA));
      long written = 0;
      while (written < targetBytes) {
        PDPage page = new PDPage();
        page.setResources(resources);
        StringBuilder ops = new StringBuilder("BT /F1 8 Tf 9 TL 36 760 Td\n");
        for (int line = 0; line < linesPerPage; line++) {
          ops.append('(');
          for (int w = 0; w < 12; w++) {
            ops.append(WORDS[random.nextInt(WORDS.length)]).append(w == 11 ? ". " : " ");
          }
          ops.append(") Tj T*\n");
        }
        ops.append("ET\n");
        byte[] content = ops.toString().getBytes(StandardCharsets.US_ASCII);
        PDStream stream = new PDStream(doc);
        try (OutputStream out = stream.createOutputStream()) {
          out.write(content);
        }
        page.setContents(stream);
        doc.addPage(page);
        written += content.length;
        pages++;
      }
      doc.save(path.toFile());
    }
    return pages;
  }

  /**
   * Writes, without holding it in memory, a valid PDF of {@code pages} pages of ordinary text ({@code linesPerPage} lines of
   * seeded words each, Helvetica, uncompressed content streams) followed by one binary stream object of {@code bulkBytes} bytes
   * that no page, resource or catalog entry refers to, then the cross-reference table and trailer. The bulk is one 64 KiB block
   * of seeded bytes with the high bit set, repeated: binary, and it never spells a PDF keyword ({@code obj}, {@code endstream}).
   * Every object's offset is counted as it is written, so the xref table is exact and a reader never has to scan the file for
   * objects.
   *
   * @return the file's size in bytes
   */
  public static long writePdfWithUnreferencedBulk(Path path, int pages, int linesPerPage, long bulkBytes, long seed)
      throws IOException {
    Random random = new Random(seed);
    // objects: 1 catalog, 2 page tree, 3 font, then a page and its content stream per page, then the bulk stream
    int bulkObject = 4 + 2 * pages;
    long[] offsets = new long[bulkObject + 1];
    try (CountingOutput out = new CountingOutput(new BufferedOutputStream(Files.newOutputStream(path), 1 << 16))) {
      out.ascii("%PDF-1.7\n");
      out.write(new byte[] {'%', (byte) 0xE2, (byte) 0xE3, (byte) 0xCF, (byte) 0xD3, '\n'});
      offsets[1] = out.count;
      out.ascii("1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n");
      StringBuilder kids = new StringBuilder();
      for (int i = 0; i < pages; i++) {
        kids.append(i == 0 ? "" : " ").append(4 + 2 * i).append(" 0 R");
      }
      offsets[2] = out.count;
      out.ascii("2 0 obj\n<< /Type /Pages /Kids [" + kids + "] /Count " + pages + " >>\nendobj\n");
      offsets[3] = out.count;
      out.ascii("3 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>\nendobj\n");
      for (int i = 0; i < pages; i++) {
        int page = 4 + 2 * i;
        offsets[page] = out.count;
        out.ascii(page + " 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> "
            + "/Contents " + (page + 1) + " 0 R >>\nendobj\n");
        StringBuilder ops = new StringBuilder("BT /F1 9 Tf 11 TL 40 760 Td\n");
        for (int line = 0; line < linesPerPage; line++) {
          ops.append('(');
          for (int w = 0; w < 12; w++) {
            ops.append(WORDS[random.nextInt(WORDS.length)]).append(w == 11 ? ". " : " ");
          }
          ops.append(") Tj T*\n");
        }
        ops.append("ET\n");
        byte[] content = ops.toString().getBytes(StandardCharsets.US_ASCII);
        offsets[page + 1] = out.count;
        out.ascii((page + 1) + " 0 obj\n<< /Length " + content.length + " >>\nstream\n");
        out.write(content);
        out.ascii("\nendstream\nendobj\n");
      }
      byte[] block = new byte[1 << 16];
      random.nextBytes(block);
      for (int i = 0; i < block.length; i++) {
        block[i] |= (byte) 0x80;
      }
      offsets[bulkObject] = out.count;
      out.ascii(bulkObject + " 0 obj\n<< /Length " + bulkBytes + " >>\nstream\n");
      for (long left = bulkBytes; left > 0; left -= block.length) {
        out.write(block, 0, (int) Math.min(block.length, left));
      }
      out.ascii("\nendstream\nendobj\n");
      long xref = out.count;
      StringBuilder table = new StringBuilder("xref\n0 " + (bulkObject + 1) + "\n0000000000 65535 f \n");
      for (int object = 1; object <= bulkObject; object++) {
        table.append(String.format("%010d 00000 n \n", offsets[object]));
      }
      table.append("trailer\n<< /Size ").append(bulkObject + 1).append(" /Root 1 0 R >>\nstartxref\n").append(xref).append("\n%%EOF\n");
      out.ascii(table.toString());
    }
    return Files.size(path);
  }

  /** An output stream that counts the bytes written through it: the next object's offset. */
  private static final class CountingOutput extends FilterOutputStream {
    private long count;

    CountingOutput(OutputStream out) {
      super(out);
    }

    void ascii(String text) throws IOException {
      write(text.getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
      count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
      count += len;
    }
  }

  /** The bytes of a small text file of {@code parts} chunks separated by {@code |}. */
  public static byte[] pipeText(String prefix, int parts) {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < parts; i++) {
      text.append(i == 0 ? "" : "|").append(prefix).append(" part ").append(i);
    }
    return text.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** {@code path}'s size. */
  public static long size(Path path) throws IOException {
    return Files.size(path);
  }
}
