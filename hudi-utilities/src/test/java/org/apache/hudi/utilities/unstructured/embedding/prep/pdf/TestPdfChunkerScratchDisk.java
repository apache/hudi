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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PDFBox's scratch disk under the chunker (class P): a full local disk while PDFBox loads
 * a file or extracts a page fails the round with {@link PrepPlatformException} instead of recording the file's {@code LOAD_ERROR} or
 * {@code PAGE_ERROR}. The disk failure is injected into PDFBox's own scratch cache ({@link FailingScratchCache}); the fixture is
 * an owner-password-only PDF, the shape whose loading and extraction write PDFBox's scratch.
 */
class TestPdfChunkerScratchDisk {

  @TempDir
  Path staging;
  private final List<String> warnings = new ArrayList<>();

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @Test
  void scratchDiskFullWhileLoadingFailsTheRound() throws IOException {
    FailingScratchCache cache = new FailingScratchCache().arm();
    Outcome outcome = run(pdfSource -> pdfSource, cache, PdfFixtures.ownerPasswordOnly("Secret page one.", "Secret page two."));
    assertTrue(cache.failedWrites() > 0, "the load wrote PDFBox's scratch");
    assertPlatformFailure(outcome);
  }

  @Test
  void scratchDiskFullWhileExtractingFailsTheRound() throws IOException {
    FailingScratchCache cache = new FailingScratchCache();
    Outcome outcome = run(cache::armedAfterOpen, cache, PdfFixtures.ownerPasswordOnly("Secret page one.", "Secret page two."));
    assertTrue(cache.failedWrites() > 0, "page extraction wrote PDFBox's scratch");
    assertPlatformFailure(outcome);
  }

  @Test
  void ownerPasswordOnlyFileIsChunkedWhenTheDiskIsFine() throws IOException {
    FailingScratchCache cache = new FailingScratchCache();
    Outcome outcome = run(pdfSource -> pdfSource, cache, PdfFixtures.ownerPasswordOnly("Secret page one.", "Secret page two."));
    assertNull(outcome.failure, String.valueOf(outcome.failure));
    assertEquals(1, outcome.chunks.size(), outcome.chunks.toString());
    assertEquals(PdfChunk.Status.OK, outcome.chunks.get(0).status());
    assertEquals("Secret page one. Secret page two.", outcome.chunks.get(0).text().replaceAll("\\s+", " ").trim());
    assertTrue(stagingContents().isEmpty());
  }

  @Test
  void corruptFileStaysLoadErrorWithTheScratchCacheInPlace() {
    // PDFBox gives up before it writes any scratch: the classifying cache changes nothing for a file that is not a PDF
    FailingScratchCache cache = new FailingScratchCache().arm();
    Outcome outcome = run(pdfSource -> pdfSource, cache, "%PDF-1.7 broken".getBytes(StandardCharsets.US_ASCII));
    assertEquals(0, cache.failedWrites());
    assertNull(outcome.failure, String.valueOf(outcome.failure));
    assertEquals(1, outcome.chunks.size(), outcome.chunks.toString());
    assertEquals(PdfChunk.Status.FAILED, outcome.chunks.get(0).status());
    assertTrue(outcome.chunks.get(0).statusDetail().startsWith("LOAD_ERROR"), outcome.chunks.get(0).statusDetail());
  }

  @Test
  void undecodablePageOfAnEncryptedFileRecordsLossWhileItsScratchIsWritten() throws IOException {
    // page 2's stream is decrypted into PDFBox's scratch, then fails to decode: the file's own fault, not the disk's, and
    // page 2 fails decoding and the round goes on with a durable loss summary
    byte[] pdf = PdfFixtures.ownerPasswordOnly(PdfFixtures.brokenPage2("Page one text.", "Page two text.", "Page three text."));
    Outcome outcome = run(pdfSource -> pdfSource, new FailingScratchCache(), pdf);
    assertNull(outcome.failure, String.valueOf(outcome.failure));
    String text = outcome.chunks.stream().map(PdfChunk::text).collect(Collectors.joining(" ")).replaceAll("\\s+", " ");
    assertTrue(text.contains("Page one text.") && text.contains("Page three text.") && !text.contains("Page two"), text);
    assertTrue(outcome.chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK), outcome.chunks.toString());
    assertTrue(outcome.chunks.get(outcome.chunks.size() - 1).statusDetail().contains("decode_failed=1"));
    assertEquals(1, warnings.stream().filter(w -> w.startsWith("PDF-PAGE") && w.contains("code=DECODE_ERROR")).count(),
        String.join("\n", warnings));
  }

  private void assertPlatformFailure(Outcome outcome) throws IOException {
    assertTrue(outcome.failure instanceof PrepPlatformException, "a platform failure, not the file's: " + outcome.failure
        + "; records " + outcome.chunks + "; warnings " + warnings);
    assertTrue(causes(outcome.failure).contains(FailingScratchCache.NO_SPACE), causes(outcome.failure));
    assertTrue(outcome.chunks.stream().noneMatch(c -> c.status() == PdfChunk.Status.FAILED), "no failed-file record: "
        + outcome.chunks);
    assertTrue(stagingContents().isEmpty(), "scratch deleted after the failure");
    assertTrue(openStagedFiles().isEmpty(), "the staged input is closed after the failure: " + openStagedFiles());
  }

  /** This JVM's open file descriptors under the staging dir (Linux /proc; none elsewhere). */
  private List<String> openStagedFiles() throws IOException {
    Path fds = Paths.get("/proc/self/fd");
    List<String> open = new ArrayList<>();
    if (!Files.isDirectory(fds)) {
      return open;
    }
    try (Stream<Path> links = Files.list(fds)) {
      for (Path link : links.collect(Collectors.toList())) {
        try {
          String target = Files.readSymbolicLink(link).toString();
          if (target.startsWith(staging.toString())) {
            open.add(target);
          }
        } catch (IOException e) {
          // the descriptor closed while listing
        }
      }
    }
    return open;
  }

  private Outcome run(UnaryOperator<PageSource> decorate, FailingScratchCache cache, byte[] pdf) {
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> decorate.apply(new PdfBoxPageSource(s, m, c, staging.toFile(), true,
        cache)));
    Outcome outcome = new Outcome();
    try (PdfChunkIterator iterator = chunker.open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("s", "s.pdf", pdf.length),
        config(), warnings::add)) {
      while (iterator.hasNext()) {
        outcome.chunks.add(iterator.next());
      }
    } catch (RuntimeException e) {
      outcome.failure = e;
    }
    return outcome;
  }

  private static PdfChunkerConfig config() {
    return PdfChunkerConfig.from(new TypedProperties(), new WordCountTokenizer());
  }

  private List<Path> stagingContents() throws IOException {
    try (Stream<Path> paths = Files.walk(staging)) {
      return paths.filter(p -> !p.equals(staging)).collect(Collectors.toList());
    }
  }

  private static String causes(Throwable error) {
    StringBuilder result = new StringBuilder();
    for (Throwable cause = error; cause != null && cause != cause.getCause(); cause = cause.getCause()) {
      result.append(cause).append('\n');
    }
    return result.toString();
  }

  private static final class Outcome {
    private final List<PdfChunk> chunks = new ArrayList<>();
    private RuntimeException failure;
  }
}
