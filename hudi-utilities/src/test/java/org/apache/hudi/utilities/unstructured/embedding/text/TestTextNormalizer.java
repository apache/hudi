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

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The text normalization rules of the PDF chunker, and the NFKC half of its error handling.
 */
class TestTextNormalizer {

  private final TextNormalizer normalizer = new TextNormalizer(262_144);

  private String norm(String text) {
    return normalizer.normalize(text).text();
  }

  @Test
  void nfkc() {
    assertEquals("text-normalizer-v2", TextNormalizer.LOGIC_VERSION);
    assertEquals("file", norm("\uFB01le"));  // escaped non-ASCII test data
    assertEquals("AB", norm("\uFF21\uFF22"));  // escaped non-ASCII test data
    assertEquals("...", norm("\u2026"));  // escaped non-ASCII test data
    assertEquals("Case, punctuation - kept.", norm("Case, punctuation - kept."));
  }

  @Test
  void lineEndings() {
    assertEquals("a b c d", norm("a\r\nb\rc\u2028d"));
    assertEquals("a\n\nb", norm("a\u2029b"));
    assertEquals("word word", norm("word\u0085word"), "NEL is a line break; the words never merge");
  }

  @Test
  void whitespaceCollapse() {
    assertEquals("a b c\n\nd", norm("a \t b\n c\n\n\n d"));
    assertEquals("a b", norm("  a\u00A0\u2003 b  "));
    assertEquals("a\n\nb", norm("a \n \n b"));
  }

  @Test
  void invisiblesRemoved() {
    assertEquals("abcd", norm("a\u2060b\uFEFFc\u00ADd"), "word joiner, BOM, soft hyphen join"); // escaped non-ASCII test data
    assertEquals("ab", norm("a\u0001\u007Fb"));  // escaped non-ASCII test data
  }

  @Test
  void zeroWidthSpaceSeparatesWordsInSpacedScripts() {
    assertEquals("alpha beta", norm("alpha\u200Bbeta"));  // escaped non-ASCII test data
    assertEquals("alpha beta", norm("alpha\u200B\u200Bbeta"));  // escaped non-ASCII test data
    assertEquals("a b", norm("a \u200B b"));  // escaped non-ASCII test data
    assertEquals("\u0430\u0431 \u0432\u0433", norm("\u0430\u0431\u200B\u0432\u0433"), "Cyrillic"); // escaped non-ASCII test data
    assertEquals("ab", norm("\u200Bab\u200B"), "at the page edges it separates nothing"); // escaped non-ASCII test data
  }

  @Test
  void zeroWidthSpaceDroppedInUnspacedScripts() {
    // Thai, Chinese and Japanese write no spaces between words; U+200B there is a break hint, not a space.
    assertEquals("\u0E2A\u0E27\u0E31\u0E2A\u0E14\u0E35", norm("\u0E2A\u0E27\u0E31\u0E2A\u200B\u0E14\u0E35")); // escaped non-ASCII test data
    assertEquals("\u4E2D\u6587", norm("\u4E2D\u200B\u6587"));  // escaped non-ASCII test data
    assertEquals("\u3072\u3089\u30AB\u30BF", norm("\u3072\u3089\u200B\u30AB\u30BF")); // escaped non-ASCII test data
  }

  @Test
  void zeroWidthSpaceNextToCjkPunctuationIsDropped() {
    // the prolonged sound mark and the ideographic full stop are Common script but belong to unspaced text
    assertEquals("\u30B3\u30FC\u30D2\u30FCcoffee", norm("\u30B3\u30FC\u30D2\u30FC\u200Bcoffee")); // escaped non-ASCII test data
    assertEquals("\u4E2D\u3002xyz", norm("\u4E2D\u3002\u200Bxyz"));  // escaped non-ASCII test data
  }

  @Test
  void emojiZwjSequencesKept() {
    String family = "\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67";  // escaped non-ASCII test data
    assertEquals(family, norm(family));
    String eyeInBubble = "\uD83D\uDC41\uFE0F\u200D\uD83D\uDDE8\uFE0F";  // escaped non-ASCII test data
    assertEquals(eyeInBubble, norm(eyeInBubble));
    String skinToneTechnologist = "\uD83D\uDC69\uD83C\uDFFD\u200D\uD83D\uDCBB"; // escaped non-ASCII test data
    assertEquals("x " + skinToneTechnologist + " y", norm("x " + skinToneTechnologist + " y"));
  }

  @Test
  void joinersKeptInsideWords() {
    String persian = "\u0645\u06CC\u200C\u062E\u0648\u0627\u0647\u0645";  // escaped non-ASCII test data
    assertEquals(persian, norm(persian), "Persian ZWNJ is part of the spelling");
    String devanagari = "\u0915\u094D\u200D\u0937";  // escaped non-ASCII test data
    assertEquals(devanagari, norm(devanagari), "Devanagari ZWJ selects the half form");
    String malayalam = "\u0D28\u0D4D\u200D";  // escaped non-ASCII test data
    assertEquals(malayalam + " x", norm(malayalam + " x"), "Malayalam chillu: ZWJ after virama ends the word"); // escaped non-ASCII test data
    String sinhala = "\u0DC1\u0DCA\u200D\u0DBB\u0DD3";  // escaped non-ASCII test data
    assertEquals(sinhala, norm(sinhala));
  }

  @Test
  void strayJoinersRemoved() {
    assertEquals("a b", norm("a \u200D b"));  // escaped non-ASCII test data
    assertEquals("abc", norm("\u200Cabc\u200D"));  // escaped non-ASCII test data
    String manWoman = "\uD83D\uDC68\u200D\uD83D\uDC69";  // escaped non-ASCII test data
    assertEquals(manWoman, norm("\uD83D\uDC68\u200D\u200D\uD83D\uDC69"), "a run of joiners keeps one"); // escaped non-ASCII test data
  }

  @Test
  void joinerAfterVowelMarksKeepsTheScriptOfItsBaseLetter() {
    // a ZWNJ after a vocalized letter follows a combining mark (Inherited script); the base letter decides
    String kasra = "\u0628\u0650\u200C\u0647\u0627";  // escaped non-ASCII test data
    assertEquals(kasra, norm(kasra), "Persian with a kasra before the ZWNJ");
    String shadda = "\u0645\u0651\u200C\u0647";  // escaped non-ASCII test data
    assertEquals(shadda, norm(shadda), "Persian with a shadda before the ZWNJ");
  }

  @Test
  void emojiZwjSequenceEndingInANonEmojiSymbolKeepsTheJoiner() {
    String headShakingSideways = "\uD83D\uDE42\u200D\u2194\uFE0F";  // escaped non-ASCII test data
    assertEquals(headShakingSideways, norm(headShakingSideways), "U+2194 is a math symbol in an emoji ZWJ sequence");
  }

  @Test
  void joinersInScriptsThatDoNotJoinAreRemoved() {
    // in Latin, Greek or Cyrillic a ZWNJ/ZWJ only steers ligatures; keeping it would split the word for exact matching
    assertEquals("Auflage", norm("Auf\u200Clage"));  // escaped non-ASCII test data
    assertEquals("alphabeta", norm("alpha\u200Dbeta"));  // escaped non-ASCII test data
    assertEquals("a\uD83D\uDE00", norm("a\u200D\uD83D\uDE00"), "a letter and an emoji form no sequence"); // escaped non-ASCII test data
    assertEquals("\u03B1\u03B2", norm("\u03B1\u200C\u03B2"), "Greek");  // escaped non-ASCII test data
  }

  @Test
  void joinerAfterAVariationSelectorAtAWordEndIsRemoved() {
    // only a virama (Malayalam chillu) keeps a joiner that nothing follows; VS16 is a combining mark but not a virama
    assertEquals("\uD83D\uDC41\uFE0F x", norm("\uD83D\uDC41\uFE0F\u200D x"));  // escaped non-ASCII test data
  }

  @Test
  void unpairedSurrogateReplacedAndCounted() {
    TextNormalizer.Result result = normalizer.normalize("ab\uD83Dcd\uDE00e\uD83D\uDE00"); // non-ASCII data
    assertEquals("ab\uFFFDcd\uFFFDe\uD83D\uDE00", result.text());  // escaped non-ASCII test data
    assertEquals(2, result.unpairedSurrogates());
  }

  @Test
  void tabDoesNotMergeWords() {
    assertEquals("a b", norm("a\tb"));
    assertEquals("a b", norm("a\u000Bb"));
    assertEquals("a b", norm("a\fb"));
  }

  @Test
  void normalizeIdempotent() {
    String[] fixed = {"a \u200B b", "e\u00AD\u0301", "x\u00A8y", "a\u200B\u200Db", "\u200D\u0915\u200D", "\u00B4\u0F72\u200D\u0327", "\uFE70\u200C\u0BCD", // escaped non-ASCII test data
        "\uFDFA \uFDFA", "a\n\u0301b", " \n\n ", "\uD800"}; // escaped non-ASCII test data
    for (String x : fixed) {
      String once = norm(x);
      assertEquals(once, norm(once), "not idempotent on fixed case " + escape(x));
    }
    Random random = new Random(42);
    String alphabet = "ab .\n\t\r\u00A0\u00AD\u200B\u200C\u200D\u2060\uFE0F\u0301\u00A8\u0E2A" // escaped non-ASCII test data
        + "\u0628\u0650\u0651\u0915\u094D\u0F72\u0327\u00B4\u2194\uFE70\uFC5E\u0BCD" // escaped non-ASCII test data
        + "\uFB01\uFF21\u2028\u2029\u4E2D\uFDFA\u0000\u001F"; // escaped non-ASCII test data
    for (int round = 0; round < 5_000; round++) {
      StringBuilder sb = new StringBuilder();
      int length = random.nextInt(40);
      for (int i = 0; i < length; i++) {
        int pick = random.nextInt(alphabet.length() + 3);
        if (pick < alphabet.length()) {
          sb.append(alphabet.charAt(pick));
        } else if (pick == alphabet.length()) {
          sb.append('\uD83D');                       // lone high surrogate
        } else if (pick == alphabet.length() + 1) {
          sb.append('\uDE00');                       // lone low surrogate
        } else {
          sb.append("\uD83D\uDE00");                 // a valid pair
        }
      }
      String once = norm(sb.toString());
      assertEquals(once, norm(once), "not idempotent on " + escape(sb.toString()));
      assertFalse(once.startsWith(" ") || once.endsWith(" ") || once.startsWith("\n") || once.endsWith("\n"));
      assertFalse(once.contains("  ") || once.contains("\n\n\n") || once.contains(" \n") || once.contains("\n "));
    }
  }

  @Test
  void nfkcExpansionTruncated() {
    StringBuilder page = new StringBuilder();
    for (int i = 0; i < 262_144; i++) {
      page.append('\uFDFA');                         // NFKC expands each to 18 chars
    }
    TextNormalizer.Result result = normalizer.normalize(page.toString());
    assertTrue(result.truncated());
    assertTrue(result.text().length() <= 262_144, "length " + result.text().length());
    assertFalse(result.text().endsWith(" "));
    assertFalse(new TextNormalizer(262_144).normalize("short").truncated());
  }

  @Test
  void truncationNeverSplitsSurrogate() {
    TextNormalizer small = new TextNormalizer(5);
    TextNormalizer.Result result = small.normalize("abcd\uD83D\uDE00ef");  // escaped non-ASCII test data
    assertEquals("abcd", result.text());
    assertTrue(result.truncated());
    for (int max = 1; max < 12; max++) {
      String emojis = "a\uD83D\uDE00b\uD83D\uDE00\uD83D\uDE00c"; // escaped non-ASCII test data
      String text = new TextNormalizer(max).normalize(emojis).text();
      assertFalse(!text.isEmpty() && Character.isHighSurrogate(text.charAt(text.length() - 1)), "max " + max);
    }
  }

  private static String escape(String s) {
    StringBuilder sb = new StringBuilder();
    for (char c : s.toCharArray()) {
      sb.append(c < 0x20 || c > 0x7E ? String.format("\\u%04X", (int) c) : String.valueOf(c));
    }
    return sb.toString();
  }
}
