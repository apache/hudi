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

import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.apache.fontbox.type1.Type1Font;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@link Type1FontCheck.SubrsFinder}: /Subrs counts as fontbox 3.0.8's Type1Lexer reads them. */
class TestType1FontCheck {

  private static void scan(String plain) {
    scan(plain, Type1FontCheck.MAX_SUBRS);
  }

  private static void scan(String plain, long maxSubrs) {
    Type1FontCheck.SubrsFinder finder = new Type1FontCheck.SubrsFinder("test", maxSubrs);
    for (byte b : plain.getBytes(StandardCharsets.ISO_8859_1)) {
      finder.accept(b & 0xFF);
    }
    finder.end();
  }

  @Test
  void refusesWhatFontboxWouldAllocate() {
    String[] over = {
        "/Subrs 65537 array",
        "/Subrs 2000000000abc",                       // tryReadNumber stops at the first non-digit: INTEGER 2000000000
        "/Subrs 99999999999999999999999999999999999 array",   // (int) Float.parseFloat saturates at Integer.MAX_VALUE
        "/Subrs " + "9".repeat(100_000) + " array",    // a long significant run saturates however long it is
        "/Subrs 2#" + "1".repeat(31) + " array",       // 31 significant binary digits: 2147483647
        "/Subrs 16#77359400 array",                    // radix: 2000000000 in hex digits that are all decimal
        "/Subrs % a comment\n 100000 array",           // comments between tokens
        "/Subrs\t\u0000100000 array",                   // tab and NUL between tokens are skipped
        "/Subrs 100000",                               // at the end of the section
        "x/Subrs 1 array /Subrs 100000 array",         // every occurrence counts
        "/Subrs 00000000000000000000000000000000002000000000 array",   // leading zeros: fontbox reads all digits
        "/Subrs +00000000000000000000000000000000000100000 array",     // sign and zeros
        "/Subrs 0000000000000000000000000000000000000000000000000000000000000000065537 array",
        "/Subrs 10#0000000000000000000000000000000000000000" + Integer.MAX_VALUE + " array",   // zeros after #
        "/Subrs 10#000000000000000000000000000000000000000000000000000000000000000065537 array",
        "/Subrs 16#7e999999 array",                    // e and digits after radix digits: 2123995545
        "/Subrs 0016#000000000000000000000000000000000000000000000000000000000000000010001 array",   // 65537
        "/Subrs 16#1e99999 array",                     // 0x1e99999 = 32086425: digits, e, decimal digits after #
        "/Mask (/Subrs %) def /Subrs 100000 array",    // a literal and % inside a string do not hide the next key
        "/Mask (/Subrs%) def /Subrs 100000 array",
        "/Subrs % /Subrs 100000",                      // a literal inside a comment still counts (refuses more only)
    };
    for (String plain : over) {
      assertThrows(StreamStructureScanner.Refusal.class, () -> scan(plain), plain);
    }
  }

  private static byte[] pfb(int emptyBinaryRecords) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] clear = "%!PS-AdobeFont-1.0: T 001.000\ncurrentfile eexec\n".getBytes(StandardCharsets.US_ASCII);
    writeRecord(out, 1, clear);
    for (int i = 0; i < emptyBinaryRecords; i++) {
      writeRecord(out, 2, new byte[0]);
    }
    writeRecord(out, 2, PdfFixtures.eexecEncrypt("abcd /Subrs 10 array".getBytes(StandardCharsets.US_ASCII)));
    out.write(0x80);
    out.write(3);
    return out.toByteArray();
  }

  private static void writeRecord(ByteArrayOutputStream out, int type, byte[] data) {
    out.write(0x80);
    out.write(type);
    for (int shift = 0; shift < 32; shift += 8) {
      out.write(data.length >> shift);
    }
    out.write(data, 0, data.length);
  }

  private static void check(byte[] font) throws IOException {
    File file = Files.createTempFile("type1-check", ".bin").toFile();
    try {
      Files.write(file.toPath(), font);
      Type1FontCheck.check(file, -1, -1, "test");
    } finally {
      Files.delete(file.toPath());
    }
  }

  /**
   * The spool is a scratch file this chunker wrote, so a local failure to read it is the executor's disk
   * ({@link PrepPlatformException}, the round fails), never an undecodable font that is skipped past the /Subrs check.
   */
  @Test
  void unreadableSpoolIsAPlatformFailure() throws IOException {
    File directory = Files.createTempDirectory("type1-check").toFile();
    try {
      assertThrows(PrepPlatformException.class, () -> Type1FontCheck.check(directory, -1, -1, "test"));
      File gone = new File(directory, "gone.bin");
      assertThrows(PrepPlatformException.class, () -> Type1FontCheck.check(gone, -1, -1, "test"));
    } finally {
      Files.delete(directory.toPath());
    }
  }

  /**
   * A spool that shrinks after the window measured it is a platform failure, never a stale buffer byte read as font
   * data; a byte the file still holds reads as written.
   */
  @Test
  void shortReadOfAShrunkSpoolIsAPlatformFailure() throws IOException {
    File file = Files.createTempFile("type1-window", ".bin").toFile();
    try {
      byte[] data = new byte[1000];
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) (i + 1);
      }
      Files.write(file.toPath(), data);
      try (Type1FontCheck.Window window = new Type1FontCheck.Window(file)) {
        assertEquals(1000, window.size());
        try (RandomAccessFile shrink = new RandomAccessFile(file, "rw")) {
          shrink.setLength(10);
        }
        assertEquals(10, window.get(9));                 // neighbour: a byte still in the file
        assertThrows(PrepPlatformException.class, () -> window.get(500));
      }
    } finally {
      Files.delete(file.toPath());
    }
  }

  @Test
  void manyRecordPfbIsRefusedBeforeCollecting() throws IOException {
    // the record count is checked before any per-record state is kept; a normal PFB has three records
    byte[] manyRecords = pfb(100_000);
    assertThrows(StreamStructureScanner.Refusal.class, () -> check(manyRecords));
    check(pfb(0));                                     // neighbour: a three-record PFB with 10 Subrs passes
  }

  @Test
  void zeroSizeRecordAtEofIsDamagedAsPfbParserSeesIt() throws IOException {
    // PfbParser reads a zero-size record at EOF as -1 bytes (EOFException), so PDFBox never parses the font
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeRecord(out, 1, "%!PS-AdobeFont-1.0: T 001.000\ncurrentfile eexec\n".getBytes(StandardCharsets.US_ASCII));
    writeRecord(out, 2, PdfFixtures.eexecEncrypt("abcd /Subrs 100000 array".getBytes(StandardCharsets.US_ASCII)));
    byte[] complete = out.toByteArray();
    writeRecord(out, 2, new byte[0]);
    check(out.toByteArray());
    assertThrows(StreamStructureScanner.Refusal.class, () -> check(complete));   // neighbour: EOF after the record
  }

  @Test
  void acceptsWhatAllocatesLittle() {
    String[] fine = {
        "/Subrs 65536 array",
        "/Subrs 0000000000000000000000000000000000000000000000000000000065536 array",   // padded, at the cap
        "/Subrs -5 array",
        "/Subrsx 100000 array",                        // a different literal
        "/Subrs array 100000",                         // not an integer token after the literal
        "/Subrs abc",
        "",
        "/Subrs\u0000 100000 array",                   // fontbox's literal is "Subrs\0": NUL does not end it
        "/Subrs 00+2000000000 array",                  // INTEGER 00, then a new token
        "/Subrs 10#2147483648 array",                  // radix overflow: Integer.parseInt throws, nothing is allocated
        "/Subrs 16#7e-99999 array",                    // a minus inside radix digits: Integer.parseInt throws
        "/Subrs 14#7e999999 array",                    // e is no digit in radix 14: Integer.parseInt throws
        "/Subrs 100000.5 array",                       // REAL: read(Token.INTEGER) throws
        "/Subrs 1e9 array",                            // REAL
        "/Subrs 1E array",                             // no exponent digit: a NAME
        "/Subrs 37#100000 array",                      // radix out of range
        "/Subrs -16#100000 array",                     // a negative radix
        "/Subrs #100000 array",                        // an empty radix
    };
    for (String plain : fine) {
      assertDoesNotThrow(() -> scan(plain), plain);
    }
  }

  /**
   * Forms of a /Subrs count fontbox's Type1Lexer.tryReadNumber accepts or rejects, each with the number of Subrs
   * Type1Parser.readSubrs allocates for it ({@code null}: fontbox allocates no Subrs and its parse throws). Each is checked
   * against fontbox itself (parsing a font whose private dict carries it) and against the guard, which must refuse one
   * Subr below that number and pass at it.
   */
  @Test
  void parsesTheCountAsFontboxDoes() throws IOException {
    Object[][] catalogue = {
        {" 300", 300L},
        {" 000000000000000000000000000000000000000000000000000000000000000000300", 300L},   // zero padding
        {" +300", 300L},
        {" -300", 0L},
        {"\t\u0000 % comment\n300", 300L},                // whitespace, NUL and a comment between the tokens
        {" 300.5", null},                                   // REAL
        {" 3e2", null},                                     // REAL
        {" 3E", null},                                      // a NAME
        {" 10#300", 300L},
        {" 10#000000000000000000000000000000000000000000000000000000000000000000300", 300L},   // zeros after #
        {" 016#100", 256L},                                 // a zero-padded radix
        {" +16#100", 256L},
        {" -16#100", null},                                 // a negative radix
        {" #100", null},                                    // an empty radix
        {" 1#1", null},
        {" 37#1", null},
        {" 2#101", 5L},
        {" 16#12e4", 4836L},                                // e and digits after radix digits
        {" 16#e4", 228L},                                   // radix digits starting with e
        {" 16#E4", 228L},
        {" 16#12e", null},                                  // e without a digit: a NAME
        {" 16#1e-4", null},                                 // a minus inside radix digits
        {" 8#1e4", null},                                   // e is no digit in radix 8
        {" 10#2147483648", null},                           // radix overflow
        {" 10#99999999999999999999999999999999999999", null},   // a long significant radix run overflows: fontbox throws
        {" 2#" + "1".repeat(32), null},                          // 32 significant binary digits: 2^32-1 overflows
        {" 2#" + "0".repeat(40) + "1".repeat(16), 65535L},       // 16 significant binary digits after 40 zeros
        {"\u0000 300", null},                               // the key is "Subrs\0", not Subrs: no Subrs, then a parse error
    };
    for (Object[] form : catalogue) {
      String after = (String) form[0];
      Long subrs = (Long) form[1];
      String plain = PdfFixtures.type1Private(after);
      if (subrs == null) {
        assertThrows(IOException.class, () -> fontbox(plain), after);
        scan(plain, 0);
      } else {
        assertEquals(subrs.intValue(), fontbox(plain).getSubrsArray().size(), after);
        scan(plain, subrs);
        if (subrs > 0) {
          assertThrows(StreamStructureScanner.Refusal.class, () -> scan(plain, subrs - 1), after);
        }
      }
    }
  }

  /**
   * Every /Subrs key fontbox's parser reaches is reached by the guard, whatever precedes it on its line:
   * each private dict below is parsed by fontbox, which must allocate {@code subrs}, and the guard pins that count.
   */
  @Test
  void reachesEveryKeyFontboxReads() throws IOException {
    String[][] dicts = {
        {PdfFixtures.MASK_ENTRY, " 300"},                          // a string holding "/Subrs %" before the key
        {"/Mask (/Subrs%) def ", " 300"},
        {"/Mask (/Subrs) def ", " 300"},                          // a string holding the bare literal
        {"/Mask (% /Subrs) def ", " 300"},                        // a comment marker inside a string
        {"/Mask (/Subrs %) def /Mask2 (/Subrs %) def ", " 300"},   // two masks on one line
    };
    for (String[] dict : dicts) {
      String plain = PdfFixtures.type1Private(dict[0], dict[1]);
      assertEquals(300, fontbox(plain).getSubrsArray().size(), dict[0]);
      scan(plain, 300);
      assertThrows(StreamStructureScanner.Refusal.class, () -> scan(plain, 299), dict[0]);
    }
  }

  /**
   * A second /Subrs literal inside the comment after a /Subrs key: fontbox skips the comment to its line end and reads the
   * next token as the key's count, so that token is judged, whatever follows the literal inside the comment.
   */
  @ParameterizedTest
  @ValueSource(strings = {
      " % /Subrs x\n300",
      " %/Subrs(\n300",
      " % /Subrs 5\n300",
      " % /Subrs 5 x\r300",
      " % /Subrs\n300",                                   // the literal ends with the comment: one count for both
      " % /Subrs % x\n300",
  })
  void subrsLiteralInsideACommentKeepsTheCountOfTheKeyBeforeIt(String afterKey) throws IOException {
    String plain = PdfFixtures.type1Private(afterKey);
    assertEquals(300, fontbox(plain).getSubrsArray().size(), afterKey);
    scan(plain, 300);
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(plain, 299), afterKey);
  }

  /** fontbox 3.0.8 parsing a font with the fixtures' clear text and {@code privatePart} (plus an empty /CharStrings). */
  private static Type1Font fontbox(String privatePart) throws IOException {
    byte[] plain = ("abcd" + privatePart + "/CharStrings 1 dict dup begin\nend\n").getBytes(StandardCharsets.ISO_8859_1);
    return Type1Font.createWithSegments(PdfFixtures.type1Clear(false), PdfFixtures.eexecEncrypt(plain));
  }
}
