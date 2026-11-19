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

package org.apache.hudi.utilities.unstructured.embedding.text;

import java.text.Normalizer;

/**
 * Standard normalization of the text of ONE page. The steps run in this fixed
 * order, which is what makes {@code normalize} idempotent:
 * <ol>
 *   <li>unpaired surrogates become U+FFFD, counted in {@link Result#unpairedSurrogates()};</li>
 *   <li>CRLF, CR, U+2028 and U+0085 become {@code \n}; U+2029 becomes {@code \n\n};</li>
 *   <li>every whitespace character other than {@code \n} becomes a space;</li>
 *   <li>the remaining {@code Cc} characters are removed, and the zero-width characters are handled by context:
 *     <ul>
 *       <li>U+2060 WORD JOINER, U+FEFF (BOM / zero-width no-break space) and U+00AD SOFT HYPHEN are removed: each asks
 *           for no break, so removing it keeps the word whole;</li>
 *       <li>U+200B ZERO WIDTH SPACE marks a word boundary. Between two visible characters it becomes a space, so
 *           {@code alpha<200B>beta} stays two words, except where either neighbour belongs to a script written without
 *           spaces (Thai, Lao, Khmer, Myanmar, Tibetan, Han, Hiragana, Katakana), where it is removed. Elsewhere (next to
 *           whitespace or at the page edge) it is removed;</li>
 *       <li>U+200C ZWNJ and U+200D ZWJ are kept where they are part of the text: inside an emoji ZWJ sequence, and
 *           after a letter or mark of a script that joins or forms conjuncts (Arabic, Syriac, N'Ko, Mongolian and
 *           the Brahmic scripts, e.g. Persian and Devanagari spellings) when a visible character follows, or after a
 *           virama when nothing does (Malayalam chillu forms end a word with one). Anywhere else (Latin, Greek or
 *           Cyrillic ligature control, next to whitespace, at the page start, repeating the previous joiner) it is
 *           removed.</li>
 *     </ul></li>
 *   <li>NFKC, then the result is cut at {@code maxPageChars} on a code-point boundary (a {@code maxPageChars} of 0 or less
 *       means no cap);</li>
 *   <li>steps 3-4 again and, when that removed a character between combining marks, NFKC again (which only reorders or
 *       composes, so the text never grows past the cap); then spaces collapse, lines are trimmed, a single {@code \n}
 *       becomes a space and two or more become exactly {@code \n\n}, a paragraph boundary;</li>
 *   <li>the page is trimmed.</li>
 * </ol>
 * No lower-casing, stemming, punctuation stripping or de-hyphenation. One instance may be shared; it holds no state.
 */
public final class TextNormalizer {

  /** Bump when the same text normalizes differently; version 2 preserves contextual word boundaries and joiners. */
  public static final String LOGIC_VERSION = "text-normalizer-v2";

  private static final char REPLACEMENT = '\uFFFD';  // U+FFFD, the Unicode replacement character
  private static final char ZERO_WIDTH_SPACE = '\u200B';
  private static final char ZERO_WIDTH_NON_JOINER = '\u200C';
  private static final char ZERO_WIDTH_JOINER = '\u200D';
  private static final char WORD_JOINER = '\u2060';
  private static final char ZERO_WIDTH_NO_BREAK_SPACE = '\uFEFF';
  private static final char SOFT_HYPHEN = '\u00AD';

  private final int maxPageChars;

  /** @param maxPageChars UTF-16 units of a normalized page at most; 0 or less for no cap */
  public TextNormalizer(int maxPageChars) {
    this.maxPageChars = maxPageChars;
  }

  /** The normalized page and what normalization had to change to get there. */
  public static final class Result {
    private final String text;
    private final boolean truncated;
    private final int unpairedSurrogates;

    Result(String text, boolean truncated, int unpairedSurrogates) {
      this.text = text;
      this.truncated = truncated;
      this.unpairedSurrogates = unpairedSurrogates;
    }

    public String text() {
      return text;
    }

    /** Whether the NFKC output was longer than {@code maxPageChars} and was cut. */
    public boolean truncated() {
      return truncated;
    }

    /** Unpaired UTF-16 surrogates replaced by U+FFFD. */
    public int unpairedSurrogates() {
      return unpairedSurrogates;
    }
  }

  public Result normalize(String page) {
    StringBuilder sb = new StringBuilder(page.length());
    // steps 1 and 2
    int unpaired = breaksAndSurrogates(page, sb);
    // steps 3 and 4
    String cleaned = spacesAndInvisibles(sb);
    // step 5
    String nfkc = Normalizer.normalize(cleaned, Normalizer.Form.NFKC);
    boolean truncated = nfkc.length() > maxPageChars && maxPageChars > 0;
    if (truncated) {
      int end = Utf16.floor(nfkc, maxPageChars);
      nfkc = nfkc.substring(0, end);
    }
    // step 6 (3-4 again, then collapse) and step 7
    String cleanedAgain = spacesAndInvisibles(new StringBuilder(nfkc));
    if (!Normalizer.isNormalized(cleanedAgain, Normalizer.Form.NFKC)) {
      // a joiner kept in step 4 but removed here (e.g. after harakat that NFKC split off a presentation form) leaves
      // marks out of canonical order; reordering now keeps normalize idempotent
      cleanedAgain = Normalizer.normalize(cleanedAgain, Normalizer.Form.NFKC);
    }
    String text = collapse(cleanedAgain);
    return new Result(text, truncated, unpaired);
  }

  /** Steps 1 and 2: line breaks to {@code \n}, unpaired surrogates to U+FFFD, appended to {@code sb}; returns the unpaired count. */
  private static int breaksAndSurrogates(String page, StringBuilder sb) {
    int unpaired = 0;
    int i = 0;
    while (i < page.length()) {
      char c = page.charAt(i);
      int consumed = 1;
      if (Character.isHighSurrogate(c) && i + 1 < page.length() && Character.isLowSurrogate(page.charAt(i + 1))) {
        sb.append(c).append(page.charAt(i + 1));
        consumed = 2;
      } else if (Character.isSurrogate(c)) {
        sb.append(REPLACEMENT);
        unpaired++;
      } else if (c == '\r') {
        sb.append('\n');
        if (i + 1 < page.length() && page.charAt(i + 1) == '\n') {
          consumed = 2;
        }
      } else if (c == '\u2028' || c == '\u0085') {    // U+0085 NEL is a line break, not a control to delete
        sb.append('\n');
      } else if (c == '\u2029') {
        sb.append("\n\n");
      } else {
        sb.append(c);
      }
      i += consumed;
    }
    return unpaired;
  }

  /** Steps 3 and 4: whitespace other than {@code \n} to a space, {@code Cc} removed, zero-width characters by context. */
  private static String spacesAndInvisibles(CharSequence text) {
    StringBuilder out = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\n') {
        out.append(c);
      } else if (Character.isWhitespace(c) || Character.getType(c) == Character.SPACE_SEPARATOR) {
        out.append(' ');
      } else if (Character.getType(c) == Character.CONTROL || isRemovedInvisible(c)) {
        // removed
      } else if (c == ZERO_WIDTH_SPACE || c == ZERO_WIDTH_JOINER || c == ZERO_WIDTH_NON_JOINER) {
        appendZeroWidth(out, text, i, c);
      } else {
        out.append(c);
      }
    }
    return out.toString();
  }

  /**
   * Step 4 for the zero-width character {@code c} at {@code i}: a zero-width space becomes a space between two visible characters
   * of spaced scripts, else nothing; a joiner is kept where {@link #keepsJoiner} keeps it.
   */
  private static void appendZeroWidth(StringBuilder out, CharSequence text, int i, char c) {
    int before = previousCodePoint(out);
    int after = nextCodePoint(text, i + 1);
    if (c == ZERO_WIDTH_SPACE) {
      if (isVisible(before) && isVisible(after) && !isUnspacedScript(before) && !isUnspacedScript(after)) {
        out.append(' ');
      }
    } else if (keepsJoiner(out, before, after)) {
      out.append(c);
    }
  }

  /** Zero-width characters that only ever ask for no break; removing them keeps the word whole. */
  private static boolean isRemovedInvisible(char c) {
    return c == WORD_JOINER || c == ZERO_WIDTH_NO_BREAK_SPACE || c == SOFT_HYPHEN;
  }

  /** The code point last written to {@code out}, or -1 at the page start. */
  private static int previousCodePoint(StringBuilder out) {
    return out.length() == 0 ? -1 : Character.codePointBefore(out, out.length());
  }

  /**
   * The next code point of {@code text} from {@code from}, skipping the characters this step always removes (non-whitespace
   * {@code Cc} and {@link #isRemovedInvisible}); -1 at the end. A zero-width space or joiner is returned as
   * itself, so a run of them never counts as visible.
   */
  private static int nextCodePoint(CharSequence text, int from) {
    int j = from;
    while (j < text.length()) {
      char c = text.charAt(j);
      boolean removed = (Character.getType(c) == Character.CONTROL && !Character.isWhitespace(c)) || isRemovedInvisible(c);
      if (removed) {
        j++;
        continue;
      }
      return Character.codePointAt(text, j);
    }
    return -1;
  }

  /** A code point that renders as something: not the page edge, whitespace, or another zero-width separator/joiner. */
  private static boolean isVisible(int codePoint) {
    return codePoint >= 0
        && codePoint != '\n'
        && !Character.isWhitespace(codePoint)
        && Character.getType(codePoint) != Character.SPACE_SEPARATOR
        && codePoint != ZERO_WIDTH_SPACE
        && codePoint != ZERO_WIDTH_JOINER
        && codePoint != ZERO_WIDTH_NON_JOINER;
  }

  /**
   * Whether a ZWJ/ZWNJ after {@code out} (whose last code point is {@code before}) and before {@code after} is kept. The
   * script is that of the base character before any trailing combining marks, and the virama test looks at the whole
   * trailing mark run: NFKC may reorder those marks between the two passes, so neither may depend on their order.
   */
  private static boolean keepsJoiner(CharSequence out, int before, int after) {
    if (!isVisible(before)) {
      return false;
    }
    if (isEmojiSequencePart(before) && isVisible(after) && isEmojiSequenceFollower(after)) {
      return true;
    }
    int end = out.length();
    boolean virama = false;
    while (end > 0 && isCombiningMark(Character.codePointBefore(out, end))) {
      int mark = Character.codePointBefore(out, end);
      virama |= isVirama(mark);
      end -= Character.charCount(mark);
    }
    int base = end > 0 ? Character.codePointBefore(out, end) : -1;   // marks with no base letter join nothing
    if (!isVisible(base) || !isJoiningScript(base)) {
      return false;
    }
    return isVisible(after) || virama;
  }

  private static boolean isCombiningMark(int codePoint) {
    int type = Character.getType(codePoint);
    return type == Character.NON_SPACING_MARK || type == Character.COMBINING_SPACING_MARK
        || type == Character.ENCLOSING_MARK;
  }

  /**
   * What may follow a ZWJ in an emoji sequence: a symbol (most emoji are So; some, like U+2194, are Sm), or a code point
   * of the emoji planes this JDK may not know yet.
   */
  private static boolean isEmojiSequenceFollower(int codePoint) {
    int type = Character.getType(codePoint);
    return type == Character.OTHER_SYMBOL || type == Character.MATH_SYMBOL
        || (codePoint >= 0x1F000 && codePoint <= 0x1FAFF);
  }

  /** An emoji, or what may end one before a ZWJ: VS16, a skin-tone modifier, a tag character. */
  private static boolean isEmojiSequencePart(int codePoint) {
    return Character.getType(codePoint) == Character.OTHER_SYMBOL
        || codePoint == 0xFE0F
        || (codePoint >= 0x1F3FB && codePoint <= 0x1F3FF)
        || (codePoint >= 0xE0020 && codePoint <= 0xE007F);
  }

  /** Scripts in which ZWJ/ZWNJ select joining or conjunct forms, so they are part of the spelling. */
  private static boolean isJoiningScript(int codePoint) {
    switch (Character.UnicodeScript.of(codePoint)) {
      case ARABIC:
      case SYRIAC:
      case NKO:
      case MONGOLIAN:
      case DEVANAGARI:
      case BENGALI:
      case GURMUKHI:
      case GUJARATI:
      case ORIYA:
      case TAMIL:
      case TELUGU:
      case KANNADA:
      case MALAYALAM:
      case SINHALA:
      case MYANMAR:
      case KHMER:
      case TIBETAN:
      case JAVANESE:
      case BALINESE:
        return true;
      default:
        return false;
    }
  }

  /** A virama (also named al-lakuna, pangkon, adeg adeg): the only mark after which a word-final joiner is meaningful. */
  private static boolean isVirama(int codePoint) {
    String name = Character.getName(codePoint);
    return name != null && (name.contains("VIRAMA") || name.contains("AL-LAKUNA") || name.endsWith("PANGKON")
        || name.endsWith("ADEG ADEG"));
  }

  /**
   * Scripts written without spaces between words, where U+200B is a line-break hint rather than a word space. The CJK
   * Symbols and Punctuation block, the Katakana block and halfwidth Katakana count too: some of their characters (the
   * ideographic full stop, the prolonged sound mark) are Common script but only occur in unspaced text.
   */
  private static boolean isUnspacedScript(int codePoint) {
    if ((codePoint >= 0x3000 && codePoint <= 0x303F) || (codePoint >= 0x30A0 && codePoint <= 0x30FF)
        || (codePoint >= 0xFF61 && codePoint <= 0xFF9F)) {
      return true;
    }
    switch (Character.UnicodeScript.of(codePoint)) {
      case THAI:
      case LAO:
      case KHMER:
      case MYANMAR:
      case TIBETAN:
      case HAN:
      case HIRAGANA:
      case KATAKANA:
        return true;
      default:
        return false;
    }
  }

  /**
   * Step 6's collapse and step 7: lines trimmed of spaces, a run of one {@code \n} becomes a space and a run of two or
   * more becomes {@code \n\n}, space runs collapse to one space, and the page is trimmed. Input holds only spaces and
   * {@code \n} as whitespace.
   */
  private static String collapse(String text) {
    StringBuilder out = new StringBuilder(text.length());
    int i = 0;
    int n = text.length();
    while (i < n) {
      char c = text.charAt(i);
      if (c != ' ' && c != '\n') {
        out.append(c);
        i++;
      } else {
        int end = whitespaceRunEnd(text, i);
        // leading or trailing whitespace of the page is dropped
        if (out.length() > 0 && end < n) {
          out.append(newlines(text, i, end) >= 2 ? "\n\n" : " ");
        }
        i = end;
      }
    }
    return out.toString();
  }

  /** The end of the run of spaces and {@code \n} starting at {@code from}. */
  private static int whitespaceRunEnd(String text, int from) {
    int end = from;
    while (end < text.length() && (text.charAt(end) == ' ' || text.charAt(end) == '\n')) {
      end++;
    }
    return end;
  }

  /** The {@code \n} in {@code text[from, end)}. */
  private static int newlines(String text, int from, int end) {
    int count = 0;
    for (int i = from; i < end; i++) {
      if (text.charAt(i) == '\n') {
        count++;
      }
    }
    return count;
  }
}
