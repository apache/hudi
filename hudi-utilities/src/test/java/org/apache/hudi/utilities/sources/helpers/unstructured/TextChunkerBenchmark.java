/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources.helpers.unstructured;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Manual benchmark for the bounded chunk-boundary search. Not a test: named *Benchmark so
 * surefire leaves it alone. Run with
 * {@code mvn -pl hudi-utilities exec:java -Dexec.classpathScope=test
 * -Dexec.mainClass=org.apache.hudi.utilities.sources.helpers.unstructured.TextChunkerBenchmark}.
 *
 * <p>Reimplements the pre-fix findBreak so the two can be compared in one process, and
 * asserts they produce identical chunk boundaries before reporting any timing - a speedup
 * that changed the output would not be a speedup.
 */
public class TextChunkerBenchmark {

  private static final String[] BOUNDARIES = {"\n\n", "\n", ". ", "! ", "? ", " "};

  public static void main(String[] args) {
    int chunkSize = 2000;
    int overlap = 200;
    System.out.printf("%-12s %12s %14s %14s %9s%n",
        "chars", "chunks", "unbounded(ms)", "bounded(ms)", "speedup");
    for (int chars : new int[] {50_000, 100_000, 250_000, 500_000, 1_000_000}) {
      String text = unbrokenText(chars);
      TextChunker chunker = new TextChunker(chunkSize, overlap);

      List<int[]> boundedBreaks = run(() -> boundaries(chunker.chunk(text)));
      List<int[]> unboundedBreaks = run(() -> chunkUnbounded(text, chunkSize, overlap));
      if (!sameBreaks(boundedBreaks.get(0), unboundedBreaks.get(0))) {
        throw new IllegalStateException("bounded search changed chunk boundaries at " + chars);
      }

      long unbounded = time(() -> chunkUnbounded(text, chunkSize, overlap));
      long bounded = time(() -> chunker.chunk(text));
      System.out.printf("%-12d %12d %14.1f %14.1f %8.1fx%n",
          chars, boundedBreaks.get(0).length, unbounded / 1e6, bounded / 1e6,
          (double) unbounded / Math.max(bounded, 1));
    }
  }

  /** Text with no paragraph break, no newline and no sentence punctuation: only " " matches. */
  private static String unbrokenText(int chars) {
    Random random = new Random(7);
    StringBuilder builder = new StringBuilder(chars + 32);
    while (builder.length() < chars) {
      int wordLength = 2 + random.nextInt(10);
      for (int i = 0; i < wordLength; i++) {
        builder.append((char) ('a' + random.nextInt(26)));
      }
      builder.append(' ');
    }
    return builder.substring(0, chars);
  }

  private static int[] boundaries(List<TextChunker.Chunk> chunks) {
    int[] starts = new int[chunks.size()];
    for (int i = 0; i < chunks.size(); i++) {
      starts[i] = chunks.get(i).charStart;
    }
    return starts;
  }

  private static boolean sameBreaks(int[] a, int[] b) {
    if (a.length != b.length) {
      return false;
    }
    for (int i = 0; i < a.length; i++) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  }

  /** The pre-fix implementation: lastIndexOf with no lower bound. */
  private static int[] chunkUnbounded(String text, int chunkSizeChars, int overlapChars) {
    List<Integer> starts = new ArrayList<>();
    int start = 0;
    while (start < text.length()) {
      int windowEnd = Math.min(start + chunkSizeChars, text.length());
      int end = windowEnd;
      if (windowEnd != text.length()) {
        int minBreak = start + Math.max(overlapChars + 1, chunkSizeChars / 2);
        end = windowEnd;
        for (String boundary : BOUNDARIES) {
          int idx = text.lastIndexOf(boundary, windowEnd - boundary.length());
          int breakEnd = idx + boundary.length();
          if (idx >= 0 && breakEnd > minBreak && breakEnd <= windowEnd) {
            end = breakEnd;
            break;
          }
        }
      }
      starts.add(start);
      if (end == text.length()) {
        break;
      }
      start = end - overlapChars;
    }
    int[] result = new int[starts.size()];
    for (int i = 0; i < result.length; i++) {
      result[i] = starts.get(i);
    }
    return result;
  }

  private static <T> List<T> run(java.util.function.Supplier<T> body) {
    List<T> out = new ArrayList<>();
    out.add(body.get());
    return out;
  }

  private static long time(Runnable body) {
    for (int i = 0; i < 3; i++) {
      body.run();
    }
    long start = System.nanoTime();
    for (int i = 0; i < 5; i++) {
      body.run();
    }
    return (System.nanoTime() - start) / 5;
  }
}
