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

import java.util.Arrays;

/**
 * The hard limits of one chunk: {@code fits(s)} holds when {@code s} has at most
 * {@code windowChars} UTF-16 units, at most {@code maxWords} words and at most {@code maxTokens} tokens of the
 * configured tokenizer, checked in that order so the tokenizer never sees more than {@code windowChars} chars.
 * Every tokenizer call goes through this class: a {@link RuntimeException} from the tokenizer becomes the caller's
 * {@link PackFailures.Kind#TOKENIZER_ERROR} failure, built by its {@link PackFailures}.
 */
public final class TokenBudget {

  private final Tokenizer tokenizer;
  private final int maxTokens;
  private final int maxWords;
  private final int windowChars;
  private final PackFailures failures;

  /**
   * A budget whose failures (an internal limit breach, a tokenizer failure) are built by {@code failures}.
   *
   * @param maxTokens   tokens of one chunk at most
   * @param maxWords    words of one chunk at most
   * @param windowChars UTF-16 units of one chunk at most; the tokenizer never sees a longer input
   */
  public TokenBudget(Tokenizer tokenizer, int maxTokens, int maxWords, int windowChars, PackFailures failures) {
    this.tokenizer = tokenizer;
    this.maxTokens = maxTokens;
    this.maxWords = maxWords;
    this.windowChars = windowChars;
    this.failures = failures;
  }

  /** Tokens of one chunk at most. */
  public int maxTokens() {
    return maxTokens;
  }

  /** Words of one chunk at most. */
  public int maxWords() {
    return maxWords;
  }

  /** UTF-16 units of one chunk at most; the tokenizer never sees a longer input. */
  public int windowChars() {
    return windowChars;
  }

  /** The caller's failure of {@code kind}, to be thrown. */
  RuntimeException failure(PackFailures.Kind kind, String message) {
    return failures.failure(kind, message, null);
  }

  /** Whether {@code s} is within the window, the word limit and the token limit, checked in that order. */
  public boolean fits(String s) {
    return s.length() <= windowChars && words(s) <= maxWords && count(s) <= maxTokens;
  }

  /** Tokens of {@code s}; {@code s} must not be longer than the window. */
  public int count(String s) {
    if (s.length() > windowChars) {
      throw failure(PackFailures.Kind.INTERNAL, "tokenizer input of " + s.length() + " chars exceeds the window of "
          + windowChars);
    }
    try {
      return tokenizer.countTokens(s);
    } catch (RuntimeException e) {
      throw failures.failure(PackFailures.Kind.TOKENIZER_ERROR, "tokenizer " + tokenizer.id() + " failed", e);
    }
  }

  /** The tokenizer's own (unchecked) prefix end of {@code s}, which is at most a window long. */
  int prefixEndWithin(String s, int limit) {
    if (s.length() > windowChars) {
      throw failure(PackFailures.Kind.INTERNAL, "tokenizer input of " + s.length() + " chars exceeds the window");
    }
    try {
      return tokenizer.prefixEndWithin(s, limit);
    } catch (RuntimeException e) {
      throw failures.failure(PackFailures.Kind.TOKENIZER_ERROR, "tokenizer " + tokenizer.id() + " failed", e);
    }
  }

  /**
   * A large code-point end {@code e} of {@code s} with {@code fits(head + s[0, e))}, found by a binary search whose lower bound
   * only moves to ends it has checked; 0 when not even one code point fits after {@code head}.
   */
  int checkedPrefixEnd(String head, String s) {
    if (fits(head + s)) {
      return s.length();
    }
    int fitsEnd = 0;
    int exceeds = s.length();
    while (true) {
      int mid = (fitsEnd + exceeds) >>> 1;
      mid = Utf16.floor(s, mid);
      if (mid <= fitsEnd) {
        mid = s.offsetByCodePoints(fitsEnd, 1);
      }
      if (mid >= exceeds) {
        return fitsEnd;
      }
      if (fits(head + s.substring(0, mid))) {
        fitsEnd = mid;
      } else {
        exceeds = mid;
      }
    }
  }

  /**
   * Whether {@link #prefixes} and {@link #suffixes} count a text by its new part: the tokenizer
   * {@link Tokenizer#addsUpAtWordSpaces adds up at word spaces}. When false their counts are full counts.
   */
  boolean countsIncrementally() {
    return tokenizer.addsUpAtWordSpaces();
  }

  /**
   * Whether char {@code i} of {@code s} is a word space: a U+0020 after a char that is not whitespace in any sense a
   * tokenizer may use (Java's, a Unicode space separator, U+0085). An adding-up tokenizer's count splits there.
   */
  static boolean isWordSpace(CharSequence s, int i) {
    return i > 0 && s.charAt(i) == ' ' && !isAnyWhitespace(s.charAt(i - 1));
  }

  private static boolean isAnyWhitespace(char c) {
    return Character.isWhitespace(c) || Character.isSpaceChar(c) || c == '\u0085';
  }

  /** Whether a word of {@link #words} starts at char {@code i} of {@code s}, counting from char 0. */
  private static boolean startsWord(CharSequence s, int i) {
    return !Character.isWhitespace(s.charAt(i)) && (i == 0 || Character.isWhitespace(s.charAt(i - 1)));
  }

  /** The prefixes of {@code s}, counted by their new part when {@link #countsIncrementally}. */
  Prefixes prefixes(String s) {
    return new Prefixes(s, countsIncrementally());
  }

  /** The suffixes of {@code s} that start at or after {@code from}, counted by parts when {@link #countsIncrementally}. */
  Suffixes suffixes(String s, int from) {
    return new Suffixes(s, from, countsIncrementally());
  }

  /**
   * {@link #fits}, {@link #count} and {@link #words} of the prefixes {@code s[0, end)} of one text, for ends asked in any
   * order, with the same results as those methods on {@code s.substring(0, end)} (a tokenizer failure may be raised by a
   * piece of the prefix rather than by the whole). Words come from one scan.
   * An incremental count is the sum of the counts of the pieces between the {@link #isWordSpace word spaces} before
   * {@code end}, each piece counted once, plus the count from the last of those spaces to {@code end}: a growing prefix
   * costs its new part, not its length.
   */
  final class Prefixes {
    private final String text;
    private final boolean incremental;
    /** wordsBefore[i]: words of text[0, i), valid for i up to wordsScanned. */
    private int[] wordsBefore;
    private int wordsScanned;
    /** The word spaces found, ascending after a leading 0, and the count of the text before each. */
    private int[] splits = {0};
    private int[] countsBefore = {0};
    private int splitCount = 1;
    /** Chars before this index have been searched for word spaces. */
    private int splitsScanned = 1;

    private Prefixes(String text, boolean incremental) {
      this.text = text;
      this.incremental = incremental;
    }

    /** {@link TokenBudget#fits} of {@code text[0, end)}. */
    boolean fits(int end) {
      return end <= windowChars && words(end) <= maxWords && count(end) <= maxTokens;
    }

    /** {@link TokenBudget#words} of {@code text[0, end)}. */
    int words(int end) {
      if (wordsBefore == null) {
        wordsBefore = new int[text.length() + 1];
      }
      for (int i = wordsScanned; i < end; i++) {
        wordsBefore[i + 1] = wordsBefore[i] + (startsWord(text, i) ? 1 : 0);
      }
      wordsScanned = Math.max(wordsScanned, end);
      return wordsBefore[end];
    }

    /** {@link TokenBudget#count} of {@code text[0, end)}. */
    int count(int end) {
      if (!incremental || end == 0 || end > windowChars) {
        return TokenBudget.this.count(text.substring(0, end));
      }
      for (int i = splitsScanned; i < end; i++) {
        if (isWordSpace(text, i)) {
          int last = splits[splitCount - 1];
          addSplit(i, countsBefore[splitCount - 1] + TokenBudget.this.count(text.substring(last, i)));
        }
      }
      splitsScanned = Math.max(splitsScanned, end);
      int k = Arrays.binarySearch(splits, 0, splitCount, end);
      k = k >= 0 ? k - 1 : -k - 2;                    // the last split before end
      int start = splits[k];
      return countsBefore[k] + (start < end ? TokenBudget.this.count(text.substring(start, end)) : 0);
    }

    private void addSplit(int at, int countBefore) {
      if (splitCount == splits.length) {
        splits = Arrays.copyOf(splits, splitCount * 2);
        countsBefore = Arrays.copyOf(countsBefore, splitCount * 2);
      }
      splits[splitCount] = at;
      countsBefore[splitCount++] = countBefore;
    }
  }

  /**
   * {@link #count} and {@link #words} of the suffixes {@code s[start, n)} of one text with {@code start >= from}, with the
   * same results as those methods on {@code s.substring(start)} (a tokenizer failure may be raised by a piece). An
   * incremental count is the count from
   * {@code start} to the first {@link #isWordSpace word space} after it plus the count of the rest, which is summed once
   * from the end over the pieces between the word spaces after {@code from}.
   */
  final class Suffixes {
    private final String text;
    private final int from;
    private final boolean incremental;
    /** startsFrom[i - from]: word starts r with i <= r < n and whitespace before r; null until first asked. */
    private int[] startsFrom;
    /** The word spaces after from, ascending, and the count of the text from each to the end; null until first asked. */
    private int[] splits;
    private int[] countsFrom;

    private Suffixes(String text, int from, boolean incremental) {
      this.text = text;
      this.from = from;
      this.incremental = incremental;
    }

    /** {@link TokenBudget#words} of {@code text[start, n)}. */
    int words(int start) {
      int n = text.length();
      if (start >= n) {
        return 0;
      }
      if (startsFrom == null) {
        startsFrom = new int[n - from + 1];
        for (int r = n - 1; r >= from; r--) {
          boolean wordStart = r > from && !Character.isWhitespace(text.charAt(r)) && Character.isWhitespace(text.charAt(r - 1));
          startsFrom[r - from] = startsFrom[r - from + 1] + (wordStart ? 1 : 0);
        }
      }
      return (Character.isWhitespace(text.charAt(start)) ? 0 : 1) + startsFrom[start + 1 - from];
    }

    /** {@link TokenBudget#count} of {@code text[start, n)}. */
    int count(int start) {
      int n = text.length();
      if (!incremental || n - start > windowChars) {
        return TokenBudget.this.count(text.substring(start));
      }
      if (splits == null) {
        computeSplits(n);
      }
      int k = Arrays.binarySearch(splits, start + 1);
      k = k >= 0 ? k : -k - 1;                        // the first split after start
      if (k == splits.length) {
        return TokenBudget.this.count(text.substring(start));
      }
      return TokenBudget.this.count(text.substring(start, splits[k])) + countsFrom[k];
    }

    /** The word-space split points after {@code from}, and the token count from each split to {@code n}. */
    private void computeSplits(int n) {
      int[] found = new int[16];
      int count = 0;
      for (int i = from + 1; i < n; i++) {
        if (isWordSpace(text, i)) {
          if (count == found.length) {
            found = Arrays.copyOf(found, count * 2);
          }
          found[count++] = i;
        }
      }
      splits = Arrays.copyOf(found, count);
      countsFrom = new int[count];
      for (int k = count - 1; k >= 0; k--) {
        int end = k + 1 < count ? splits[k + 1] : n;
        countsFrom[k] = TokenBudget.this.count(text.substring(splits[k], end)) + (k + 1 < count ? countsFrom[k + 1] : 0);
      }
    }
  }

  /** maximal runs of non-whitespace code points. */
  public static int words(CharSequence s) {
    int count = 0;
    boolean inWord = false;
    int i = 0;
    while (i < s.length()) {
      int cp = Character.codePointAt(s, i);
      boolean whitespace = Character.isWhitespace(cp);
      if (!whitespace && !inWord) {
        count++;
      }
      inWord = !whitespace;
      i += Character.charCount(cp);
    }
    return count;
  }
}
