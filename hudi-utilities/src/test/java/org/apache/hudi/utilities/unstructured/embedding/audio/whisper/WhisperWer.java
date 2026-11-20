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

package org.apache.hudi.utilities.unstructured.embedding.audio.whisper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Word error rate, the rule the whisper tests grade transcripts with. Both texts are normalised the same way ({@link #normalise});
 * the error count is the word-level Levenshtein distance (substitutions + deletions + insertions) and the rate divides it by the reference word count.
 *
 * <p>{@link #global} aligns the whole hypothesis against the whole reference. {@link #body} is a semi-global alignment:
 * hypothesis words before the first and after the last aligned reference word cost nothing, so speech the reference text
 * does not hold at the edges of a recording (a reader's spoken preamble and closing) is not charged; every reference word
 * must still be matched, substituted or deleted, and insertions between reference words still count.
 */
final class WhisperWer {

  /** Error counts of one alignment. */
  public static final class Result {
    public final int referenceWords;
    public final int hypothesisWords;
    public final int substitutions;
    public final int deletions;
    public final int insertions;

    public Result(int referenceWords, int hypothesisWords, int substitutions, int deletions, int insertions) {
      this.referenceWords = referenceWords;
      this.hypothesisWords = hypothesisWords;
      this.substitutions = substitutions;
      this.deletions = deletions;
      this.insertions = insertions;
    }

    public int errors() {
      return substitutions + deletions + insertions;
    }

    public double rate() {
      return referenceWords == 0 ? 0 : (double) errors() / referenceWords;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "WER %.2f%% (S=%d D=%d I=%d over %d reference words, %d hypothesis words)",
          rate() * 100, substitutions, deletions, insertions, referenceWords, hypothesisWords);
    }
  }

  private WhisperWer() {
  }

  /**
   * Lowercase; square-bracketed annotations removed ({@code [BLANK_AUDIO]} from the model, {@code [Picture: ...]} from the
   * text); apostrophes and the ASCII hyphen removed, so {@code giant's} and {@code to-day} become {@code giants} and
   * {@code today}; every other non-letter, non-digit character is a word break.
   */
  public static List<String> normalise(String text) {
    String lower = text.toLowerCase(Locale.ROOT).replaceAll("\\[[^\\]]*\\]", " ")
        .replace("'", "").replace("’", "").replace("‘", "").replace("-", "");
    String spaced = lower.replaceAll("[^\\p{L}\\p{Nd}]+", " ").trim();
    return spaced.isEmpty() ? new ArrayList<>() : Arrays.asList(spaced.split(" "));
  }

  public static Result global(String reference, String hypothesis) {
    return align(normalise(reference), joinCompounds(compounds(reference), normalise(hypothesis)), false);
  }

  public static Result body(String reference, String hypothesis) {
    return align(normalise(reference), joinCompounds(compounds(reference), normalise(hypothesis)), true);
  }

  /**
   * The reference's hyphenated compounds, normalised and split: {@code peach-trees} gives {@code peachtrees -> [peach,
   * trees]}. {@link #normalise} joins them, so a hypothesis that writes the compound as separate words is joined back
   * by {@link #joinCompounds} and the comparison is insensitive to how the compound is spelt.
   */
  static Map<String, List<String>> compounds(String reference) {
    Map<String, List<String>> compounds = new HashMap<>();
    Matcher matcher = COMPOUND.matcher(reference);
    while (matcher.find()) {
      List<String> parts = new ArrayList<>();
      for (String part : matcher.group().split("-")) {
        parts.addAll(normalise(part));
      }
      if (parts.size() > 1) {
        compounds.put(String.join("", parts), parts);
      }
    }
    return compounds;
  }

  /** Replaces every run of hypothesis words that spells a reference compound's parts with the joined compound. */
  static List<String> joinCompounds(Map<String, List<String>> compounds, List<String> words) {
    if (compounds.isEmpty()) {
      return words;
    }
    List<String> joined = new ArrayList<>(words.size());
    int i = 0;
    while (i < words.size()) {
      String match = null;
      int width = 0;
      for (Map.Entry<String, List<String>> compound : compounds.entrySet()) {
        List<String> parts = compound.getValue();
        if (parts.size() > width && i + parts.size() <= words.size()
            && words.subList(i, i + parts.size()).equals(parts)) {
          match = compound.getKey();
          width = parts.size();
        }
      }
      if (match == null) {
        joined.add(words.get(i));
        i++;
      } else {
        joined.add(match);
        i += width;
      }
    }
    return joined;
  }

  private static final Pattern COMPOUND = Pattern.compile("[\\p{L}\\p{Nd}]+(?:-[\\p{L}\\p{Nd}]+)+");

  private static final int MATCH = 0;
  private static final int SUBSTITUTE = 1;
  private static final int DELETE = 2;
  private static final int INSERT = 3;
  private static final int FREE = 4;

  static Result align(List<String> ref, List<String> hyp, boolean freeEdges) {
    int m = ref.size();
    int n = hyp.size();
    int[][] cost = new int[m + 1][n + 1];
    byte[][] op = new byte[m + 1][n + 1];
    for (int j = 1; j <= n; j++) {
      cost[0][j] = freeEdges ? 0 : j;
      op[0][j] = (byte) (freeEdges ? FREE : INSERT);
    }
    for (int i = 1; i <= m; i++) {
      cost[i][0] = i;
      op[i][0] = DELETE;
      for (int j = 1; j <= n; j++) {
        boolean same = ref.get(i - 1).equals(hyp.get(j - 1));
        int best = cost[i - 1][j - 1] + (same ? 0 : 1);
        int bestOp = same ? MATCH : SUBSTITUTE;
        if (cost[i - 1][j] + 1 < best) {
          best = cost[i - 1][j] + 1;
          bestOp = DELETE;
        }
        if (cost[i][j - 1] + 1 < best) {
          best = cost[i][j - 1] + 1;
          bestOp = INSERT;
        }
        cost[i][j] = best;
        op[i][j] = (byte) bestOp;
      }
    }
    int end = n;
    if (freeEdges) {
      for (int j = 0; j <= n; j++) {
        if (cost[m][j] < cost[m][end]) {
          end = j;
        }
      }
    }
    int substitutions = 0;
    int deletions = 0;
    int insertions = 0;
    int i = m;
    int j = end;
    while (i > 0 || j > 0) {
      switch (op[i][j]) {
        case MATCH:
          i--;
          j--;
          break;
        case SUBSTITUTE:
          substitutions++;
          i--;
          j--;
          break;
        case DELETE:
          deletions++;
          i--;
          break;
        case INSERT:
          insertions++;
          j--;
          break;
        default:
          j--;
          break;
      }
    }
    return new Result(m, n, substitutions, deletions, insertions);
  }
}
