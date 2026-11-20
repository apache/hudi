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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Refuses an embedded Type1 font file whose private dictionary declares more than {@value #MAX_SUBRS} {@code /Subrs}:
 * fontbox 3.0.8's {@code Type1Parser.readSubrs} adds one list entry per declared Subr as soon as
 * it reads {@code /Subrs <integer>}, before reading any. To see exactly what fontbox sees, this check reproduces PDFBox
 * 3.0.8's own steps on the decoded font file, which it reads from a spool file through a small window (no heap copy):
 * <ol>
 *   <li>a file starting with 0x80 is a PFB: its binary (type 2) records, in order, are the eexec section
 *       ({@code PDType1Font} → {@code Type1Font.createWithPFB} → {@code PfbParser}); a malformed record makes PDFBox
 *       give up before parsing, so nothing is refused;</li>
 *   <li>otherwise {@code /Length1} and {@code /Length2} are repaired as {@code PDType1Font.repairLength1/2} do (the last
 *       {@code exec} found scanning backwards from {@code Length1 - 4}, then CR, LF, space and tab skipped), and the
 *       eexec section is {@code [Length1, Length1 + Length2)};</li>
 *   <li>the section is hex when none of its first four bytes is outside hex digits and CR, LF, space, tab
 *       ({@code Type1Parser.isBinary}); it is decrypted with r = 55665, discarding four bytes;</li>
 *   <li>every {@code /Subrs} literal in the plain text (ended by whitespace or a delimiter, not NUL; then whitespace, NUL
 *       or {@code %} comments) is followed by a token lexed as {@code Type1Lexer.tryReadNumber} lexes it
 *       ({@link CountToken}), and the count {@code Type1Parser.readSubrs} would allocate for it is checked. Matching every
 *       occurrence, not only the one fontbox's lexer would reach — inside strings and comments too, and while skipping a
 *       comment between a literal and its count — can only refuse more.</li>
 * </ol>
 */
final class Type1FontCheck {

  private static final String FAILED = " failed";

  /** Declared /Subrs entries of one Type1 font. */
  static final long MAX_SUBRS = 65_536;
  private static final int EEXEC_KEY = 55665;
  private static final int PFB_START = 0x80;
  private static final int PFB_HEADER_LENGTH = 18;
  /** PFB records accepted in one font file; a PFB has three (ASCII, binary, ASCII trailer). */
  static final int MAX_PFB_RECORDS = 16;
  private static final byte[] SUBRS = "/Subrs".getBytes(StandardCharsets.US_ASCII);

  private Type1FontCheck() {
  }

  /**
   * @param length1 the stream's {@code /Length1} ({@code -1} when absent, as {@code COSStream.getInt} returns)
   * @throws StreamStructureScanner.Refusal when the font declares too many Subrs
   */
  static void check(File font, int length1, int length2, String where) {
    try (Window bytes = new Window(font)) {
      long size = bytes.size();
      if (size == 0 || size > Integer.MAX_VALUE) {
        return;                                        // PDFBox: "Font data unavailable" / no byte[] that large
      }
      int n = (int) size;
      SubrsFinder finder = new SubrsFinder(where);
      if (bytes.get(0) == PFB_START) {
        int records = walkPfb(bytes, n, null);           // count first, keeping nothing per record
        if (records > MAX_PFB_RECORDS) {
          throw new StreamStructureScanner.Refusal("a PFB font file with more than " + MAX_PFB_RECORDS + " records in " + where
              + " (refused before PDFBox parses it)");
        }
        if (records >= 0) {
          List<long[]> binary = new ArrayList<>(records);
          walkPfb(bytes, n, binary);
          eexecSection(bytes, binary, finder);
        }
        return;
      }
      int l1 = repairLength1(bytes, n, length1);
      int l2 = length2 < 0 || length2 > n - l1 ? n - l1 : length2;
      if (l1 < 0 || l1 > l1 + l2) {
        return;                                        // PDFBox throws "Invalid length data": the font is damaged
      }
      if (l1 > 0 && l2 > 0) {
        List<long[]> section = new ArrayList<>(1);
        section.add(new long[] {l1, l2});
        eexecSection(bytes, section, finder);
      }
    }
  }

  /** {@code PDType1Font.repairLength1} on the spooled bytes. */
  private static int repairLength1(Window bytes, int n, int length1) {
    int offset = Math.max(0, length1 - 4);
    if (offset <= 0 || offset > n - 4) {
      offset = n - 4;
    }
    offset = findBinaryOffsetAfterExec(bytes, n, offset);
    if (offset == 0 && length1 > 0) {
      offset = findBinaryOffsetAfterExec(bytes, n, n - 4);
    }
    if (length1 - offset != 0 && offset > 0) {
      return offset;
    }
    return length1;
  }

  /** {@code PDType1Font.findBinaryOffsetAfterExec}. */
  private static int findBinaryOffsetAfterExec(Window bytes, int n, int startOffset) {
    int offset = startOffset;
    while (offset > 0) {
      if (bytes.get(offset) == 'e' && bytes.get(offset + 1L) == 'x' && bytes.get(offset + 2L) == 'e' && bytes.get(offset + 3L) == 'c') {
        offset += 4;
        while (offset < n) {
          int b = bytes.get(offset);
          if (b != '\r' && b != '\n' && b != ' ' && b != '\t') {
            break;
          }
          offset++;
        }
        break;
      }
      offset--;
    }
    return offset;
  }

  /**
   * Walks the records as {@code PfbParser} reads them: returns the record count, or -1 where PfbParser throws; when
   * {@code binary} is not null, adds the {offset, length} of each binary record to it (called only once the count is
   * known to be at most {@link #MAX_PFB_RECORDS}).
   */
  private static int walkPfb(Window bytes, int n, List<long[]> binary) {
    if (n < PFB_HEADER_LENGTH) {
      return -1;
    }
    int records = 0;
    long position = 0;
    long total = 0;
    while (position < n || total == 0) {               // until EOF after at least one record
      int type = pfbRecordType(bytes, n, position);
      if (type < 0) {
        return -1;
      }
      if (type == 3) {
        break;
      }
      long size = pfbRecordSize(bytes, n, position);
      if (size < 0) {
        return -1;
      }
      addBinaryRecord(binary, type, position, size);
      if (++records > MAX_PFB_RECORDS && binary == null) {
        return records;                                // over the bound: no need to read further
      }
      total += size;
      position += 6 + size;
    }
    return total > n ? -1 : records;
  }

  /** The type of the PFB record at {@code position} (1, 2, or 3 for EOF), or -1 where PfbParser throws. */
  private static int pfbRecordType(Window bytes, int n, long position) {
    if (position >= n || bytes.get(position) != PFB_START) {
      return -1;                                     // "Start marker missing"
    }
    if (position + 1 >= n) {
      return -1;
    }
    int type = bytes.get(position + 1);
    if (type == 3) {
      return type;
    }
    if (type != 1 && type != 2) {
      return -1;
    }
    return type;
  }

  /** The size of the type 1 or 2 PFB record at {@code position}, or -1 where PfbParser throws. */
  private static long pfbRecordSize(Window bytes, int n, long position) {
    if (position + 6 > n) {
      return -1;                                     // PfbParser would read -1 bytes into the size and fail
    }
    long size = bytes.get(position + 2) | (bytes.get(position + 3) << 8) | (bytes.get(position + 4) << 16)
        | ((long) bytes.get(position + 5) << 24);
    size = (int) size;                               // PfbParser sums ints: a set top bit is a negative size
    if (size < 0 || size > n || position + 6 + size > n) {
      return -1;                                     // negative, larger than the input, or EOF inside the record
    }
    if (size == 0 && position + 6 >= n) {
      return -1;                                     // reading 0 bytes at EOF returns -1: "EOF while reading"
    }
    return size;
  }

  /** Adds the {offset, length} of a binary (type 2) record to {@code binary}, when it is not null. */
  private static void addBinaryRecord(List<long[]> binary, int type, long position, long size) {
    if (type == 2 && binary != null) {
      binary.add(new long[] {position + 6, size});
    }
  }

  /** {@code Type1Parser.parseBinary}: hex detection, hex decoding, eexec decryption, fed to {@code finder}. */
  private static void eexecSection(Window bytes, List<long[]> ranges, SubrsFinder finder) {
    decrypt(bytes, ranges, isBinary(bytes, ranges), finder);
  }

  /** {@code Type1Parser.parseBinary}'s test: binary unless the first 4 bytes are all whitespace or hex digits. */
  private static boolean isBinary(Window bytes, List<long[]> ranges) {
    long length = 0;
    for (long[] range : ranges) {
      length += range[1];
    }
    boolean binary = length < 4;
    for (int i = 0; i < 4 && i < length && !binary; i++) {
      int b = at(bytes, ranges, i);
      binary = b != 0x0a && b != 0x0d && b != 0x20 && b != '\t' && hexDigit(b) == -1;
    }
    return binary;
  }

  /** Hex-decodes (unless {@code binary}) and eexec-decrypts the ranges, feeding the plain bytes after the first 4 to {@code finder}. */
  private static void decrypt(Window bytes, List<long[]> ranges, boolean binary, SubrsFinder finder) {
    Eexec eexec = new Eexec(finder);
    HexPairs hex = new HexPairs();
    for (long[] range : ranges) {
      for (long p = range[0]; p < range[0] + range[1]; p++) {
        int b = bytes.get(p);
        if (binary) {
          eexec.accept(b);
        } else if (hex.accept(b)) {
          eexec.accept(hex.value());
        }
      }
    }
    finder.end();
  }

  /** eexec decryption: the plain byte of each cipher byte after the first 4 goes to {@code finder}. */
  private static final class Eexec {
    private final SubrsFinder finder;
    private int r = EEXEC_KEY;
    private long cipherIndex;

    Eexec(SubrsFinder finder) {
      this.finder = finder;
    }

    void accept(int b) {
      int plain = (b ^ (r >> 8)) & 0xFF;
      r = ((b + r) * 52845 + 22719) & 0xFFFF;
      if (cipherIndex++ >= 4) {
        finder.accept(plain);
      }
    }
  }

  /** Hex decoding: a non-hex byte is skipped; a first hex digit waits for its pair. */
  private static final class HexPairs {
    private int high = -1;
    private int value;

    /** True when {@code b} completes a pair, whose byte is then {@link #value()}. */
    boolean accept(int b) {
      int digit = hexDigit(b);
      if (digit < 0) {
        return false;
      }
      if (high < 0) {
        high = digit;
        return false;
      }
      value = high * 16 + digit;
      high = -1;
      return true;
    }

    int value() {
      return value;
    }
  }

  /** {@code Character.digit((char) byte, 16)} as fontbox computes it on a signed byte. */
  private static int hexDigit(int b) {
    return Character.digit((char) (byte) b, 16);
  }

  private static int at(Window bytes, List<long[]> ranges, long index) {
    for (long[] range : ranges) {
      if (index < range[1]) {
        return bytes.get(range[0] + index);
      }
      index -= range[1];
    }
    return -1;
  }

  /** Finds {@code /Subrs <integer>} in decrypted plain text, as fontbox's Type1Lexer tokenises it. */
  static final class SubrsFinder {
    private final String where;
    private final long maxSubrs;
    private final CountToken count = new CountToken();
    private int matched;                               // bytes of "/Subrs" matched so far
    private int state;                                 // 0 searching, 1 after the literal, 2 in a comment after it, 3 in the count
    /** A /Subrs literal was found inside the comment after a key, and that comment has not ended: the key's count is still to come. */
    private boolean keyInComment;

    SubrsFinder(String where) {
      this(where, MAX_SUBRS);
    }

    /** @param maxSubrs the largest count passed (tests pin the parsed value with it) */
    SubrsFinder(String where, long maxSubrs) {
      this.where = where;
      this.maxSubrs = maxSubrs;
    }

    void accept(int b) {
      switch (state) {
        case 1:
          acceptAfterLiteral(b);
          return;
        case 2:
          acceptInComment(b);
          return;
        case 3:
          acceptInCount(b);
          return;
        default:
          if (literalEndsAt(b)) {
            state = 1;
            accept(b);
          }
      }
    }

    /** A byte after a /Subrs literal: a comment, the first byte of its count token, or whitespace. */
    private void acceptAfterLiteral(int b) {
      if (b == '%') {
        state = 2;
      } else if (!isFontboxWhitespace(b)) {
        state = 3;
        count.reset();
        accept(b);
      } else if (b == '\r' || b == '\n') {
        keyInComment = false;                          // the comment ended: the next token is the count of both
      }
    }

    /** A byte of a comment between a /Subrs literal and its count. */
    private void acceptInComment(int b) {
      if (b == '\r' || b == '\n') {
        state = 1;
        keyInComment = false;
      } else if (literalEndsAt(b)) {                   // a /Subrs inside the comment may be one fontbox reads
        state = 1;
        keyInComment = true;                           // fontbox may skip it: the key before the comment still waits
        accept(b);
      }
    }

    /** A byte of the count token; the byte that ends it has the count checked against the limit, then is read again. */
    private void acceptInCount(int b) {
      if (count.accept(b)) {
        return;
      }
      checkCount();
      state = keyInComment ? 2 : 0;                    // back in the comment: the token after its line end is judged too
      keyInComment = false;
      matched = 0;
      accept(b);                                       // fontbox puts the character after a token back
    }

    /** Advances the "/Subrs" match by {@code b}; true when {@code b} ends a complete literal. */
    private boolean literalEndsAt(int b) {
      if (matched == SUBRS.length) {
        matched = 0;
        if (Character.isWhitespace((char) (byte) b) || isDelimiter(b)) {   // Type1Lexer.readRegular: NUL does not end it
          return true;
        }
      }
      if (b == SUBRS[matched]) {
        matched++;
      } else {
        matched = b == '/' ? 1 : 0;
      }
      return false;
    }

    /** A count cut by the end of the section is judged as complete (fontbox throws there; this can only refuse more). */
    void end() {
      if (state == 3) {
        checkCount();
      }
    }

    private void checkCount() {
      long subrs = count.subrs();
      if (subrs > maxSubrs) {
        throw new StreamStructureScanner.Refusal("a Type1 /Subrs of " + subrs + " entries (more than " + maxSubrs + ") in " + where
            + " (refused before PDFBox parses it)");
      }
    }

    /** {@code Character.isWhitespace} on fontbox's {@code (char) byte}, plus NUL, which fontbox skips between tokens. */
    private static boolean isFontboxWhitespace(int b) {
      return b == 0 || Character.isWhitespace((char) (byte) b);
    }

    private static boolean isDelimiter(int b) {
      return b == '(' || b == ')' || b == '<' || b == '>' || b == '[' || b == ']' || b == '{' || b == '}' || b == '/' || b == '%';
    }
  }

  /**
   * One token after {@code /Subrs}, lexed character by character exactly as fontbox 3.0.8's {@code Type1Lexer.tryReadNumber}
   * does, keeping O(1) state however long it is: {@code [+-]? digit*}, then {@code .} (a REAL),
   * {@code #} (the text so far is the radix of an integer that follows), or the end of an INTEGER; a REAL or radix
   * continues with a digit or {@code e|E}, digits, and an optional {@code e|E -? digit+}. {@link #subrs} is what
   * {@code Type1Parser.readSubrs} then allocates: an INTEGER as {@code (int) Float.parseFloat} reads it (leading zeros free,
   * saturating at {@code Integer.MAX_VALUE}, nothing when negative); a radix token as {@code Integer.parseInt(digits, radix)}
   * reads it, nothing when that throws (bad radix, a character that is no digit in the radix, overflow); nothing for a REAL
   * or a non-number, which {@code read(Token.INTEGER)} rejects.
   */
  static final class CountToken {
    private static final long SATURATED = Integer.MAX_VALUE;
    private static final int START = 0;
    private static final int INTEGER = 1;              // optional sign read, in the digits before . or #
    private static final int REQUIRED_DIGIT = 2;       // after . or #
    private static final int DIGITS = 3;
    private static final int AFTER_E = 4;
    private static final int EXPONENT_DIGIT = 5;       // after e-
    private static final int EXPONENT_DIGITS = 6;
    private int phase;
    private boolean negative;
    private boolean hasDigit;
    private long integerPart;                              // the digits before . or #, saturating
    private boolean real;
    private int radix;                                 // 0 until #; -1 for a radix Integer.parseInt rejects
    private long radixValue;
    private boolean radixFails;                        // Integer.parseInt(digits, radix) throws
    private boolean notANumber;

    void reset() {
      phase = START;
      negative = false;
      hasDigit = false;
      integerPart = 0;
      real = false;
      radix = 0;
      radixValue = 0;
      radixFails = false;
      notANumber = false;
    }

    /** @return whether {@code b} belongs to the token; once it does not, {@link #subrs} is final */
    boolean accept(int b) {
      boolean digit = b >= '0' && b <= '9';
      boolean e = b == 'e' || b == 'E';
      switch (phase) {
        case START:
          return acceptStart(b);
        case INTEGER:
          return acceptInteger(b, digit, e);
        case REQUIRED_DIGIT:
          return acceptRequiredDigit(b, digit, e);
        case DIGITS:
          return acceptDigits(b, digit, e);
        case AFTER_E:
          return acceptAfterE(b, digit);
        case EXPONENT_DIGIT:
          return acceptExponentDigit(b, digit);
        default:                                       // EXPONENT_DIGITS
          return acceptExponentDigits(b, digit);
      }
    }

    private boolean acceptStart(int b) {
      phase = INTEGER;
      if (b == '+' || b == '-') {
        negative = b == '-';
        return true;
      }
      return accept(b);
    }

    private boolean acceptInteger(int b, boolean digit, boolean e) {
      if (digit) {
        hasDigit = true;
        integerPart = Math.min(integerPart * 10 + (b - '0'), SATURATED);
        return true;
      }
      if (b == '.') {
        real = true;
        phase = REQUIRED_DIGIT;
        return true;
      }
      if (b == '#') {
        radix = hasDigit && !negative && integerPart >= Character.MIN_RADIX && integerPart <= Character.MAX_RADIX ? (int) integerPart : -1;
        phase = REQUIRED_DIGIT;
        return true;
      }
      if (!hasDigit) {
        notANumber = true;                             // fontbox re-reads it as a NAME
        return false;
      }
      if (!e) {
        return false;                                  // INTEGER
      }
      real = true;                                     // digits then e: fontbox's required-digit test lets e through
      phase = AFTER_E;
      return true;
    }

    private boolean acceptRequiredDigit(int b, boolean digit, boolean e) {
      if (!digit && !e) {
        notANumber = true;
        return false;
      }
      addRadixDigit(b);
      phase = digit ? DIGITS : AFTER_E;
      return true;
    }

    private boolean acceptDigits(int b, boolean digit, boolean e) {
      if (!digit && !e) {
        return false;
      }
      addRadixDigit(b);
      phase = digit ? DIGITS : AFTER_E;
      return true;
    }

    private boolean acceptAfterE(int b, boolean digit) {
      if (b != '-' && !digit) {
        notANumber = true;
        return false;
      }
      addRadixDigit(b);
      phase = digit ? EXPONENT_DIGITS : EXPONENT_DIGIT;
      return true;
    }

    private boolean acceptExponentDigit(int b, boolean digit) {
      if (!digit) {
        notANumber = true;
        return false;
      }
      addRadixDigit(b);
      phase = EXPONENT_DIGITS;
      return true;
    }

    private boolean acceptExponentDigits(int b, boolean digit) {
      if (!digit) {
        return false;
      }
      addRadixDigit(b);
      return true;
    }

    /** Integer.parseInt(digits, radix) one character at a time: '-' inside, a non-digit or overflow makes it throw. */
    private void addRadixDigit(int b) {
      if (radix == 0 || radixFails) {
        return;                                        // a REAL, or already failing
      }
      int d = radix < 0 ? -1 : Character.digit((char) b, radix);
      if (d < 0) {
        radixFails = true;
        return;
      }
      radixValue = radixValue * radix + d;
      radixFails = radixValue > Integer.MAX_VALUE;
    }

    /** The number of Subrs fontbox allocates for this token. */
    long subrs() {
      if (notANumber) {
        return 0;
      }
      if (radix != 0) {
        return radixFails || radix < 0 || !(phase == DIGITS || phase == EXPONENT_DIGITS) ? 0 : radixValue;
      }
      if (real || negative) {
        return 0;
      }
      return integerPart;
    }
  }

  /**
   * Random access to a spool file through one 64 KiB window, forwards or backwards. The spool is a scratch file this chunker
   * wrote, so each local I/O failure is a {@link ScratchFiles} failure (the round fails), never the font's.
   */
  static final class Window implements AutoCloseable {
    private static final int SIZE = 64 * 1024;
    private final File path;
    private final RandomAccessFile file;
    private final long length;
    private final byte[] buffer = new byte[SIZE];
    private long start = -1;
    private int filled;

    Window(File path) {
      this.path = path;
      try {
        this.file = new RandomAccessFile(path, "r");
      } catch (IOException | RuntimeException e) {
        throw ScratchFiles.failure("cannot read the scratch file " + path, e);
      }
      try {
        this.length = file.length();
      } catch (IOException | RuntimeException e) {
        RuntimeException failure = ScratchFiles.failure("reading the scratch file " + path + FAILED, e);
        try {
          file.close();
        } catch (IOException closeFailure) {
          failure.addSuppressed(closeFailure);
        }
        throw failure;
      }
    }

    long size() {
      return length;
    }

    /**
     * The byte at {@code position}, or -1 outside the length measured at open.
     *
     * @throws org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException when a read of the file fails, or
     *     the file no longer holds a byte inside that length (it shrank after it was measured)
     */
    int get(long position) {
      if (position < 0 || position >= length) {
        return -1;
      }
      if (start < 0 || position < start || position >= start + filled) {
        start = position < start ? Math.max(0, position - SIZE + 1) : position;
        filled = fill((int) Math.min(SIZE, length - start));
        if (position >= start + filled) {
          start = -1;
          throw ScratchFiles.failure("the scratch file " + path + " ends before byte " + position + "; it held " + length
              + " bytes when opened", null);
        }
      }
      return buffer[(int) (position - start)] & 0xFF;
    }

    /** Reads up to {@code wanted} bytes at {@link #start}, as many as the file holds; a read may return fewer than asked. */
    private int fill(int wanted) {
      int total = 0;
      try {
        file.seek(start);
        while (total < wanted) {
          int read = file.read(buffer, total, wanted - total);
          if (read < 0) {
            break;
          }
          total += read;
        }
      } catch (IOException e) {
        start = -1;
        throw ScratchFiles.failure("reading the scratch file " + path + FAILED, e);
      }
      return total;
    }

    @Override
    public void close() {
      try {
        file.close();
      } catch (IOException e) {
        throw ScratchFiles.failure("closing the scratch file " + path + FAILED, e);
      }
    }
  }
}
