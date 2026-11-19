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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Static rules over the PDF chunker's main sources.
 */
public class TestPdfBoxChunker {

  @BeforeAll
  public static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  /** Pseudo-English page text with PGTOKEN<n> tokens and paragraphs. */
  static String pageText(int page, int sentences, Random random) {
    String[] words = "lake table record index stream chunk page file vector model search data commit timeline".split(" ");
    StringBuilder sb = new StringBuilder("PGTOKEN" + page + " opens the page. ");
    for (int s = 0; s < sentences; s++) {
      int n = 4 + random.nextInt(10);
      for (int w = 0; w < n; w++) {
        String word = words[random.nextInt(words.length)];
        sb.append(w == 0 ? Character.toUpperCase(word.charAt(0)) + word.substring(1) : word).append(w + 1 < n ? " " : ". ");
      }
      if (s % 7 == 6) {
        sb.append("PGTOKEN").append(page).append(" again.\n\n");
      }
    }
    return sb.toString().trim();
  }

  // ------------------------------------------------------------------ static rules

  /** The PDF chunker's main sources: its own package and the text packing package it packs with. */
  private static List<Path> mainSources() throws IOException {
    List<Path> sources = new ArrayList<>();
    for (String dir : Arrays.asList("src/main/java/org/apache/hudi/utilities/unstructured/embedding/prep/pdf",
        "src/main/java/org/apache/hudi/utilities/unstructured/embedding/text")) {
      try (Stream<Path> files = Files.list(Paths.get(dir))) {
        files.filter(p -> p.toString().endsWith(".java")).forEach(sources::add);
      }
    }
    return sources;
  }

  /** The PDF chunker's main sources import nothing from the separate unstructured helpers and transform packages. */
  @Test
  public void noImportsFromUnstructuredHelpers() throws IOException {
    for (Path source : mainSources()) {
      String code = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
      for (String forbidden : Arrays.asList("helpers.unstructured", "transform.embedding", "transform.unstructured")) {
        assertFalse(code.contains(forbidden), source + " imports from the separate unstructured helpers package " + forbidden);
      }
    }
  }

  @Test
  public void noForbiddenWholeDocumentCalls() throws IOException {
    List<Path> sources = mainSources();
    assertTrue(sources.size() > 10);
    for (Path source : sources) {
      String code = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
      for (String forbidden : Arrays.asList("getText(", "parseToString", "readAllBytes", "ByteArrayOutputStream",
          "getPage(")) {
        assertFalse(code.contains(forbidden), source + " uses " + forbidden);
      }
    }
  }

}
