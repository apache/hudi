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

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Streaming, non-allocating pre-scans of decoded stream bytes, for the structures PDFBox 3.0.8 builds while PARSING a
 * stream that passes the decoded-size cap. Each scanner sees the bytes once, keeps a
 * few counters, and throws a {@link PageFailure} with {@code DECODE_LIMIT} once a cap is passed:
 * <ul>
 *   <li>{@link Operands}: operands pending between two operators of a content stream (PDFStreamEngine keeps them all in
 *       one list), the elements of an open array or dictionary operand (a huge {@code TJ} array), and the bytes of one
 *       literal string, hex string or inline-image body (PDFStreamParser copies each into one buffer, even when the stream
 *       itself is read as a view on the file);</li>
 *   <li>{@link CMapRanges}: the total mappings of the {@code bfrange} sections of a CMap (PDFBox's CMapParser adds one
 *       mapping per code of a range, or per entry of a destination array).</li>
 * </ul>
 * Embedded Type1 fonts are checked by {@link Type1FontCheck}, which needs PDFBox's segmentation of the whole
 * file.
 *
 * <p>These scanners read the guard's own decoding with their own grammar, which on some token sequences differs from
 * PDFBox's, and a single /FlateDecode /Contents stream is parsed by PDFBox without its predictor; the content budgets
 * are therefore enforced again on PDFBox's own tokens by {@link BoundedContentParser}.
 */
final class StreamStructureScanner {

  /** Operands between operators, or elements of one open array/dictionary operand. */
  static final int MAX_OPERANDS = 65_536;
  /** Total codes covered by the bfrange sections of one CMap (four full two-byte planes). */
  static final long MAX_CMAP_RANGE_CODES = 262_144;

  private StreamStructureScanner() {
  }

  static boolean isWhitespace(int b) {
    return b == 0 || b == 9 || b == 10 || b == 12 || b == 13 || b == 32;
  }

  static boolean isDelimiter(int b) {
    return b == '(' || b == ')' || b == '<' || b == '>' || b == '[' || b == ']' || b == '{' || b == '}' || b == '/' || b == '%';
  }

  /** A byte sink that inspects bytes one at a time. */
  abstract static class Scanner extends OutputStream {
    protected final String where;

    Scanner(String where) {
      this.where = where;
    }

    @Override
    public void write(byte[] bytes, int offset, int length) {
      for (int i = offset; i < offset + length; i++) {
        accept(bytes[i] & 0xFF);
      }
    }

    @Override
    public void write(int b) {
      accept(b & 0xFF);
    }

    abstract void accept(int b);

    protected PageFailure fail(String what) {
      return new Refusal(what + " in " + where + " (refused before PDFBox parses it)");
    }
  }

  /** A structure cap refusal: DECODE_LIMIT raised by a scanner (as opposed to the decoded-size cap). */
  static final class Refusal extends PageFailure {
    Refusal(String message) {
      super(PdfFailureCode.DECODE_LIMIT, message);
    }
  }

  /** The PostScript-like token grammar shared by content streams and CMaps; subclasses react to complete tokens. */
  abstract static class Tokens extends Scanner implements Cloneable {
    private static final int MAX_TOKEN = 64;
    private StringBuilder token = new StringBuilder(MAX_TOKEN);
    private boolean tokenTruncated;
    private boolean numericToken;
    private boolean numberHasDot;
    private boolean readStringToken;
    private int lastTokenByte;
    private int stringDepth;
    private boolean escape;
    private boolean inHex;
    private boolean inComment;
    private boolean inName;
    private int pendingLt;
    private int pendingGt;

    Tokens(String where) {
      super(where);
    }

    @Override
    void accept(int b) {
      if (inComment) {
        acceptCommentByte(b);
        return;
      }
      if (stringDepth > 0) {
        acceptStringByte(b);
        return;
      }
      if (inHex) {
        acceptHexByte(b);
        return;
      }
      if (pendingLt > 0) {
        acceptAfterLt(b);
        return;
      }
      if (pendingGt > 0) {
        pendingGt = 0;
        if (b == '>') {
          onDictionaryClose();                         // ">>" closes a dictionary
          return;
        }
      }
      if (endsContentToken(b) || endsCmapToken(b)) {
        endToken();
        accept(b);
        return;
      }
      if (cmapGrammar() && (b == '{' || b == '}')) {
        endToken();                                    // CMapParser reads a brace as the start of an operator
        lastTokenByte = b;
        token.append((char) b);
        return;
      }
      if (isWhitespace(b) || isDelimiter(b)) {
        endToken();
        acceptDelimiter(b);
        return;
      }
      appendTokenByte(b);
    }

    /** A byte inside a comment: a line end closes it. */
    private void acceptCommentByte(int b) {
      if (b == 10 || b == 13) {
        inComment = false;
      }
    }

    /** A byte inside a literal string: tracks escapes and nesting until the closing parenthesis. */
    private void acceptStringByte(int b) {
      if (cmapGrammar()) {
        if (b == ')') {                                // CMapParser.readString: no escapes, no nesting
          stringDepth = 0;
          onOperand();
        }
        return;
      }
      if (escape) {
        escape = false;
      } else if (b == '\\') {
        escape = true;
      } else if (b == '(') {
        stringDepth++;
      } else if (b == ')' && --stringDepth == 0) {
        onOperand();
      }
    }

    /** A byte inside a hex string: {@code >} ends it, whitespace is skipped. */
    private void acceptHexByte(int b) {
      if (b == '>') {
        inHex = false;
        onHexEnd();
      } else if (!isWhitespace(b)) {
        onHexDigit(b);
      }
    }

    /** The byte after a {@code <}: a second {@code <} opens a dictionary, anything else starts a hex string. */
    private void acceptAfterLt(int b) {
      pendingLt = 0;
      if (b == '<') {
        onDictionaryOpen();                            // "<<" opens a dictionary
        return;
      }
      inHex = true;                                    // "<" starts a hex string
      onHexStart();
      accept(b);
    }

    /** Whether {@code b} ends the token being read at a content-stream number or operator boundary. */
    private boolean endsContentToken(int b) {
      if (splitContentNumbers() && numericToken && !Character.isDigit((char) b) && b != '-'
          && !(b == '.' && !numberHasDot)) {
        return true;
      }
      return splitContentNumbers() && !inName && !numericToken && token.length() > 0 && !readStringToken
          && b >= '0' && b <= '9' && !(lastTokenByte == 'd' && (b == '0' || b == '1'));
    }

    /** Whether {@code b} ends the token being read where CMapParser.readNumber and readOperator stop. */
    private boolean endsCmapToken(int b) {
      return cmapGrammar() && !inName && token.length() > 0
          && (numericToken ? !Character.isDigit((char) b) && b != '.' : b >= '0' && b <= '9');
    }

    /** Reacts to a whitespace or delimiter byte, the token before it already ended. */
    private void acceptDelimiter(int b) {
      switch (b) {
        case '(':
          stringDepth = 1;
          break;
        case '<':
          pendingLt = 1;
          break;
        case '>':
          pendingGt = 1;
          break;
        case '[':
        case '{':
          onOpen();
          break;
        case ']':
        case '}':
          onClose();
          break;
        case '/':
          inName = true;
          break;
        case '%':
          inComment = true;
          break;
        default:
          break;
      }
    }

    /** Adds a regular byte to the token being read, classifying the token at its first byte; past the limit, marks it truncated. */
    private void appendTokenByte(int b) {
      if (splitContentNumbers() && token.length() == 0 && !inName) {
        numericToken = b >= '0' && b <= '9' || b == '+' || b == '-' || b == '.';
        readStringToken = b == 'B' || b == 'n' || b == 't' || b == 'f';
      } else if (cmapGrammar() && token.length() == 0 && !inName) {
        numericToken = b >= '0' && b <= '9';
      }
      if (numericToken && b == '.') {
        numberHasDot = true;
      }
      lastTokenByte = b;
      if (token.length() < MAX_TOKEN) {
        token.append((char) b);
      } else {
        tokenTruncated = true;
      }
    }

    /** Ends the regular token being read, if any. */
    protected void endToken() {
      if (token.length() == 0 && !inName) {
        return;
      }
      String t = tokenTruncated ? token + "~" : token.toString();
      boolean name = inName;
      boolean number = numericToken;
      numericToken = false;
      numberHasDot = false;
      readStringToken = false;
      token.setLength(0);
      tokenTruncated = false;
      inName = false;
      onToken(t, name, number);
    }

    @Override
    public void close() {
      endToken();
    }

    /** An independent copy of this scanner's state. */
    protected Tokens copyTokens() {
      try {
        Tokens copy = (Tokens) super.clone();
        copy.token = new StringBuilder(token);
        return copy;
      } catch (CloneNotSupportedException e) {
        throw new IllegalStateException(e);
      }
    }

    /** Whether the byte being read belongs to an open literal or hex string. */
    protected boolean inStringBody() {
      return stringDepth > 0 || inHex;
    }

    /** A complete string operand. */
    protected void onOperand() {
    }

    protected void onHexStart() {
    }

    protected void onHexDigit(int b) {
    }

    protected void onHexEnd() {
      onOperand();
    }

    protected abstract void onOpen();

    protected abstract void onClose();

    protected void onDictionaryOpen() {
      onOpen();
    }

    protected void onDictionaryClose() {
      onClose();
    }

    /** CMaps keep their own grammar; only content streams follow PDFStreamParser's number/operator boundaries. */
    protected boolean splitContentNumbers() {
      return false;
    }

    /**
     * Whether tokens follow fontbox 3.0.8's CMapParser: a literal string ends at the first {@code )}, a number at its first
     * byte that is not a digit or {@code .}, an operator at a digit, and {@code {} and {@code }} start operators instead
     * of opening or closing containers.
     */
    protected boolean cmapGrammar() {
      return false;
    }

    /** Numeric category is retained independently of the capped token text. */
    protected abstract void onToken(String token, boolean name, boolean number);

  }

  /** Content streams: operands pending between operators, elements of one open array/dictionary, and body bytes. */
  static final class Operands extends Tokens {
    private long pending;
    private int depth;
    private long elements;
    private boolean inlineImage;
    private int inlineState;                           // 0 searching, 1 saw E, 2 saw EI, 3 checking following bytes
    /** PDFStreamParser 3.0.8 checks exactly ten bytes after EI before accepting it as an image terminator. */
    private byte[] inlineLookahead = new byte[10];
    private int lookaheadLength;
    private final boolean finishOnClose;
    /** Bytes one literal string, hex string or inline-image body may hold: the decoded-size budget. */
    private final long maxBodyBytes;
    private long bodyBytes;
    /** Whether the bytes being read are lookahead bytes read again, which were counted when they first arrived. */
    private boolean replaying;

    /** @param maxBodyBytes bytes one string or inline-image body may hold */
    Operands(String where, long maxBodyBytes) {
      this(where, true, maxBodyBytes);
    }

    /**
     * Page /Contents streams share one scanner, so their individual sink closes are not end of input.
     *
     * @param maxBodyBytes bytes one string or inline-image body may hold
     */
    Operands(String where, boolean finishOnClose, long maxBodyBytes) {
      super(where);
      this.finishOnClose = finishOnClose;
      this.maxBodyBytes = maxBodyBytes;
    }

    @Override
    void accept(int b) {
      if (!replaying) {
        countBody();
      }
      if (!inlineImage) {
        super.accept(b);
        return;
      }
      acceptInline(b);
    }

    /** The byte after an {@code EI} candidate: whitespace starts the lookahead, else the search restarts. */
    private void acceptAfterEi(int b) {
      if (b == 10 || b == 13 || b == 32) {
        inlineState = 3;
        inlineLookahead[lookaheadLength++] = (byte) b;
      } else {
        inlineState = b == 'E' ? 1 : 0;
      }
    }

    /** One byte of an inline image's data: looks for its {@code EI} end, followed by whitespace and then a lookahead. */
    private void acceptInline(int b) {
      if (inlineState == 3) {
        inlineLookahead[lookaheadLength++] = (byte) b;
        if (lookaheadLength == inlineLookahead.length) {
          resolveInlineCandidate();
        }
      } else if (inlineState == 2) {
        acceptAfterEi(b);
      } else if (inlineState == 1 && b == 'I') {
        inlineState = 2;
      } else {
        inlineState = b == 'E' ? 1 : 0;
      }
    }

    /** An independent copy of this scanner, to go back to when the bytes fed after it must be forgotten. */
    Operands copy() {
      Operands copy = (Operands) copyTokens();
      copy.inlineLookahead = inlineLookahead.clone();
      return copy;
    }

    /** Counts the byte about to be read when it belongs to an open string or inline-image body. */
    private void countBody() {
      if (!inlineImage && !inStringBody()) {
        bodyBytes = 0;
        return;
      }
      if (++bodyBytes > maxBodyBytes) {
        throw fail("a string or inline-image body of more than " + maxBodyBytes + " bytes");
      }
    }

    private void resolveInlineCandidate() {
      int length = lookaheadLength;
      boolean terminates = hasNoFollowingBinaryData(length);
      inlineState = 0;
      lookaheadLength = 0;
      if (terminates) {
        inlineImage = false;
      }
      // Revisit lookahead as ordinary tokens, or image bytes that may themselves contain another EI candidate.
      // Any new candidate writes behind this read position in the same fixed buffer.
      boolean outerReplay = replaying;
      replaying = true;
      try {
        for (int i = 0; i < length; i++) {
          accept(inlineLookahead[i] & 0xFF);
        }
      } finally {
        replaying = outerReplay;
      }
    }

    /** Matches PDFStreamParser.hasNoFollowingBinData, including its signed-byte and short-EOF behavior. */
    private boolean hasNoFollowingBinaryData(int length) {
      if (containsBinaryByte(length)) {
        return false;
      }
      int start = firstNonBlank(length);
      int end = firstBlankAfter(start, length);
      if (end != -1 && start != -1
          && !endsInlineImage(new String(inlineLookahead, start, end - start, StandardCharsets.US_ASCII))) {
        return false;
      }
      if (start != -1 && length == inlineLookahead.length) {
        return isShortOrNumberToken(start, end, length);
      }
      return true;
    }

    /** Whether the first {@code length} lookahead bytes hold a byte PDFBox takes for binary data. */
    private boolean containsBinaryByte(int length) {
      for (int i = 0; i < length; i++) {
        if (isBinaryByte(inlineLookahead[i])) {
          return true;
        }
      }
      return false;
    }

    private static boolean isBinaryByte(byte value) {
      return value != 0 && value < 9 || value > 10 && value < 32 && value != 13;
    }

    private static boolean isBlank(byte value) {
      return value == 0 || value == 9 || value == 32 || value == 10 || value == 13;
    }

    /** The index of the first non-blank lookahead byte; -1 when there is none. */
    private int firstNonBlank(int length) {
      for (int i = 0; i < length; i++) {
        if (!isBlank(inlineLookahead[i])) {
          return i;
        }
      }
      return -1;
    }

    /** The index of the first blank lookahead byte after {@code start}; -1 when there is none or no start. */
    private int firstBlankAfter(int start, int length) {
      if (start == -1) {
        return -1;
      }
      for (int i = start + 1; i < length; i++) {
        if (isBlank(inlineLookahead[i])) {
          return i;
        }
      }
      return -1;
    }

    /** Whether a token after {@code EI} lets it end the inline image, as PDFStreamParser accepts it. */
    private static boolean endsInlineImage(String next) {
      return "Q".equals(next) || "EMC".equals(next) || "S".equals(next) || isInlineNumber(next);
    }

    /** A full lookahead: whether its token (to the end when unterminated) is short or a number. */
    private boolean isShortOrNumberToken(int start, int end, int length) {
      int tokenEnd = end == -1 ? length : end;
      return tokenEnd - start <= 3
          || isInlineNumber(new String(inlineLookahead, start, tokenEnd - start, StandardCharsets.US_ASCII));
    }

    private static boolean isInlineNumber(String text) {
      boolean dot = false;
      for (int i = 0; i < text.length(); i++) {
        char value = text.charAt(i);
        if (value == '.' && !dot) {
          dot = true;
        } else if (value < '0' || value > '9') {
          return false;
        }
      }
      return true;
    }

    /** Ends the complete content sequence; a short final lookahead is judged as PDFBox judges EOF. */
    void finish() {
      while (inlineImage && inlineState == 3) {
        resolveInlineCandidate();
      }
      super.close();
    }

    @Override
    public void close() {
      if (finishOnClose) {
        finish();
      } else {
        super.close();
      }
    }

    private void operand() {
      if (depth > 0) {
        if (++elements > MAX_OPERANDS) {
          throw fail("an array or dictionary operand with more than " + MAX_OPERANDS + " elements");
        }
        return;
      }
      if (++pending > MAX_OPERANDS) {
        throw fail("more than " + MAX_OPERANDS + " operands before an operator");
      }
    }

    @Override
    protected void onOperand() {
      operand();
    }

    @Override
    protected void onOpen() {
      if (depth == 0) {
        elements = 0;
      } else {
        operand();
      }
      depth++;
    }

    @Override
    protected void onClose() {
      if (depth > 0 && --depth == 0) {
        operand();                                     // the closed container is one operand
      }
    }

    @Override
    protected boolean splitContentNumbers() {
      // Arrays and dictionaries use BaseParser.parseCOSNumber, whose grammar differs from top-level content numbers.
      return depth == 0;
    }

    @Override
    protected void onToken(String token, boolean name, boolean number) {
      if (name || number || "true".equals(token) || "false".equals(token) || "null".equals(token) || depth > 0) {
        operand();
        return;
      }
      pending = 0;                                     // an operator consumes the operands before it
      if ("ID".equals(token)) {
        inlineImage = true;
        inlineState = 0;
      }
    }
  }

  /**
   * CMaps: total mappings of the bfrange sections, counted as fontbox 3.0.8's CMapParser adds them: the endpoints are
   * signed ints (CMap.toInt over the hex string's bytes, an odd last digit padded with zero), a hex-string destination
   * adds one mapping per code from start to end, and a destination array adds one mapping per hex-string entry,
   * whatever span the endpoints declare.
   */
  static final class CMapRanges extends Tokens {
    private boolean inRanges;
    private int position;                              // 0 start, 1 end, 2 destination
    private int start;
    private int end;
    private int value;
    private int digits;
    private int arrayDepth;
    private long arrayEntries;
    private long total;

    CMapRanges(String where) {
      super(where);
    }

    @Override
    protected void onHexStart() {
      value = 0;
      digits = 0;
    }

    @Override
    protected void onHexDigit(int b) {
      int v = Character.digit(b, 16);
      if (v < 0) {
        return;
      }
      digits++;
      value = (value << 4) | v;                        // CMap.toInt keeps the low 32 bits, as int arithmetic does
    }

    @Override
    protected void onHexEnd() {
      if (!inRanges) {
        return;
      }
      if (arrayDepth > 0) {
        if (arrayDepth == 1 && position == 2) {
          arrayEntries++;                              // each hex-string entry of the destination array is one mapping
        }
        return;
      }
      if (digits % 2 == 1) {
        value <<= 4;                                   // an odd last digit is the high half of its byte
      }
      if (position == 0) {
        start = value;
        position = 1;
      } else if (position == 1) {
        end = value;
        position = 2;
      } else {
        range();
      }
    }

    /** A range with a single destination: one mapping per code from start to end (none when end is below start). */
    private void range() {
      charge(end >= start ? (long) end - start + 1 : 0);
    }

    private void charge(long codes) {
      total = codes > MAX_CMAP_RANGE_CODES ? MAX_CMAP_RANGE_CODES + 1 : total + codes;
      position = 0;
      if (total > MAX_CMAP_RANGE_CODES) {
        throw fail("bfrange sections covering more than " + MAX_CMAP_RANGE_CODES + " codes");
      }
    }

    @Override
    protected boolean cmapGrammar() {
      return true;
    }

    /** A string destination: CMapParser ignores it and reads the next range. */
    @Override
    protected void onOperand() {
      if (inRanges && position == 2 && arrayDepth == 0) {
        range();
      }
    }

    @Override
    protected void onOpen() {
      if (arrayDepth++ == 0) {
        arrayEntries = 0;
      }
    }

    /**
     * CMapParser ends a dictionary at its first key that is not a name, so "<<" and ">>" do not pair up as containers do;
     * inside a bfrange section, where they would shift the count, they are refused.
     */
    @Override
    protected void onDictionaryOpen() {
      refuseDictionaryInRanges();
      onOpen();
    }

    @Override
    protected void onDictionaryClose() {
      refuseDictionaryInRanges();
      onClose();
    }

    private void refuseDictionaryInRanges() {
      if (inRanges) {
        throw fail("a dictionary delimiter inside a bfrange section");
      }
    }

    @Override
    protected void onClose() {
      if (arrayDepth > 0 && --arrayDepth == 0 && inRanges && position == 2) {
        charge(end >= start ? arrayEntries : 0);       // a destination array completes the range
      }
    }

    @Override
    protected void onToken(String token, boolean name, boolean number) {
      if (!name && "beginbfrange".equals(token)) {
        if (!inRanges) {
          arrayDepth = 0;                              // a dictionary left open before the section may have ended already
        }
        inRanges = true;
        position = 0;
      } else if (!name && "endbfrange".equals(token) && arrayDepth == 0) {
        inRanges = false;                              // inside an array, CMapParser reads it as an array element
      } else if (inRanges && position == 2 && arrayDepth == 0) {
        range();                                       // a non-hex destination (a name) still completes it
      }
    }
  }
}
