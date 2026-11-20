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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chunker over real PDFs built with PDFBox's writer: staging, loading, page
 * isolation, the decoded-size guard on every oversizedInput construction, the glyph/text and depth caps, and the static rules.
 */
class TestPdfBoxChunker {

  private static final Tokenizer BPE = new OpenAiBpeTokenizer();
  private static final Tokenizer WORDS = new WordCountTokenizer();
  private static final Pattern PAGE_TOKEN = Pattern.compile("PGTOKEN(\\d+)");

  @TempDir
  Path staging;
  private final List<String> warnings = new ArrayList<>();

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  private PdfBoxChunker chunker() {
    return new PdfBoxChunker((s, m, c) -> new PdfBoxPageSource(s, m, c, staging.toFile(), true));
  }

  private List<PdfChunk> chunk(byte[] pdf, PdfChunkerConfig config) {
    return chunk(chunker(), pdf, config);
  }

  private List<PdfChunk> chunk(PdfBoxChunker chunker, byte[] pdf, PdfChunkerConfig config) {
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = chunker.open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("source_path_sha", "test.pdf",
        pdf.length), config, warnings::add)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    return chunks;
  }

  private static PdfChunkerConfig config(Tokenizer tokenizer, String... keyValues) {
    TypedProperties props = new TypedProperties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return PdfChunkerConfig.from(props, tokenizer);
  }

  private static PdfChunkerConfig defaults() {
    return config(BPE);
  }

  /** Caps small enough for unit fixtures: window 256, 1 000 chars and glyphs per page. */
  private static PdfChunkerConfig smallCaps() {
    return config(BPE, EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key(), "256", EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key(), "1000",
        EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "1000");
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

  static byte[] textPdf(int pages, int sentences) throws IOException {
    Random random = new Random(42);
    List<String> texts = new ArrayList<>();
    for (int p = 1; p <= pages; p++) {
      texts.add(pageText(p, sentences, random));
    }
    return PdfFixtures.text(texts);
  }

  private static String all(List<PdfChunk> chunks) {
    return chunks.stream().map(PdfChunk::text).collect(Collectors.joining("|"));
  }

  private static PdfChunk last(List<PdfChunk> chunks) {
    return chunks.get(chunks.size() - 1);
  }

  private List<Path> stagingContents() throws IOException {
    try (Stream<Path> paths = Files.walk(staging)) {
      return paths.filter(p -> !p.equals(staging)).collect(Collectors.toList());
    }
  }

  @Test
  void validPdfControl() throws IOException {
    List<PdfChunk> chunks = chunk(PdfFixtures.text("Readable control page."), config(WORDS));
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.OK, chunks.get(0).status());
    assertEquals("Readable control page.", chunks.get(0).text());
  }

  // ------------------------------------------------------------------ records and order

  @Test
  void chunkIdOrderMatchesPageOrder() throws IOException {
    List<PdfChunk> chunks = chunk(textPdf(3, 40), defaults());
    assertTrue(chunks.size() > 3);
    int lastPageSeen = 0;
    for (int i = 0; i < chunks.size(); i++) {
      PdfChunk chunk = chunks.get(i);
      assertEquals(i, chunk.chunkId());
      Matcher m = PAGE_TOKEN.matcher(chunk.text().substring(chunk.overlapChars()));
      while (m.find()) {
        int page = Integer.parseInt(m.group(1));
        assertTrue(page >= lastPageSeen, "new text of a later chunk never comes from an earlier page");
        lastPageSeen = page;
      }
    }
    assertEquals(3, lastPageSeen);
  }

  @Test
  void pageSpansMatchPrintedPageNumbers() throws IOException {
    for (PdfChunk chunk : chunk(textPdf(5, 30), defaults())) {
      Matcher m = PAGE_TOKEN.matcher(chunk.text());
      while (m.find()) {
        int page = Integer.parseInt(m.group(1));
        assertTrue(page >= chunk.pageStart() && page <= chunk.pageEnd(),
            "PGTOKEN" + page + " outside the span " + chunk.pageStart() + ".." + chunk.pageEnd());
      }
    }
  }

  @Test
  void sameInputTwiceSameRecords() throws IOException {
    byte[] pdf = textPdf(4, 30);
    List<PdfChunk> first = chunk(pdf, defaults());
    List<PdfChunk> second = chunk(pdf, defaults());
    assertEquals(first.size(), second.size());
    for (int i = 0; i < first.size(); i++) {
      PdfChunk a = first.get(i);
      PdfChunk b = second.get(i);
      assertEquals(Arrays.asList(a.chunkId(), a.text(), a.tokenCount(), a.wordCount(), a.overlapChars(), a.pageStart(),
          a.pageEnd(), a.status(), a.statusDetail()), Arrays.asList(b.chunkId(), b.text(), b.tokenCount(), b.wordCount(),
          b.overlapChars(), b.pageStart(), b.pageEnd(), b.status(), b.statusDetail()));
    }
  }

  @Test
  void sequentialFilesReuseChunker() throws IOException {
    PdfBoxChunker chunker = chunker();
    byte[] valid = textPdf(2, 20);
    List<PdfChunk> first = chunk(chunker, valid, defaults());
    List<PdfChunk> corrupt = chunk(chunker, "not a pdf at all".getBytes(StandardCharsets.US_ASCII), defaults());
    List<PdfChunk> third = chunk(chunker, valid, defaults());
    assertEquals(PdfChunk.Status.FAILED, corrupt.get(0).status());
    assertEquals(first.size(), third.size());
    assertEquals(all(first), all(third));
  }

  @Test
  void validCorruptValid() throws IOException {
    sequentialFilesReuseChunker();
  }

  @Test
  void sentenceAcrossPagesStaysOneSentence() throws IOException {
    byte[] pdf = PdfFixtures.text("Page one starts here. The quick brown", "fox jumps over the lazy dog. Page two ends.");
    List<PdfChunk> chunks = chunk(pdf, defaults());
    assertTrue(chunks.stream().anyMatch(c -> c.text().contains("The quick brown fox jumps over the lazy dog.")), all(chunks));
  }

  @Test
  void multiColumnOrderIsContentOrder() throws IOException {
    byte[] pdf = PdfFixtures.multiColumn("Left column words come second in content order.",
        "Right column words come first in content order.");
    String text = all(chunk(pdf, defaults()));
    assertTrue(text.indexOf("Right column") >= 0 && text.indexOf("Right column") < text.indexOf("Left column"), text);
  }

  @Test
  void specialTokenTextChunked() throws IOException {
    List<PdfChunk> chunks = chunk(PdfFixtures.text("Model text with <|endoftext|> inside a sentence."), defaults());
    assertTrue(all(chunks).contains("<|endoftext|>"));
  }

  @Test
  void statsMatchKnownFixture() throws IOException {
    byte[] pdf = PdfFixtures.text("First page. Some text.", "", "Third page. More text.");
    PdfChunkIterator iterator = chunker().open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("source_path_sha", "t", pdf.length),
        defaults(), warnings::add);
    List<PdfChunk> chunks = new ArrayList<>();
    while (iterator.hasNext()) {
      chunks.add(iterator.next());
    }
    iterator.close();
    PdfChunkerStats stats = iterator.stats();
    assertEquals(3, stats.pagesSeen());
    assertEquals(1, stats.pagesEmpty());
    assertEquals(0, stats.pagesFailed() + stats.pagesTruncated() + stats.decodeLimited());
    assertEquals(pdf.length, stats.stagedBytes());
    assertEquals(1, stats.chunksEmitted());
    assertEquals("First page. Some text. Third page. More text.", chunks.get(0).text());
    assertEquals(1, chunks.get(0).pageStart());
    assertEquals(3, chunks.get(0).pageEnd());
  }

  // ------------------------------------------------------------------ markers and isolation

  @Test
  void scannedPdfYieldsOneNoTextRow() throws IOException {
    List<PdfChunk> chunks = chunk(PdfFixtures.imageOnly(2, 40), defaults());
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.NO_TEXT, chunks.get(0).status());
    assertEquals(0, chunks.get(0).chunkId());
  }

  @Test
  void imageOnlyPdfYieldsNoText() throws IOException {
    scannedPdfYieldsOneNoTextRow();
  }

  @Test
  void corruptPdfYieldsOneFailedRow() throws IOException {
    byte[] garbage = new byte[100_000];
    new Random(1).nextBytes(garbage);
    List<PdfChunk> chunks = chunk(garbage, defaults());
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("LOAD_ERROR: "), chunks.get(0).statusDetail());
    assertTrue(stagingContents().isEmpty(), "staged file deleted on failure");
  }

  @Test
  void undecodablePageContentRetainsOtherPagesAndRecordsLoss() throws IOException {
    // PDFBox leaves out the undecodable page, and the last surviving chunk records the loss
    byte[] pdf = PdfFixtures.brokenPage2("PGTOKEN1 page one text. It is here.", "PGTOKEN2 page two text.",
        "PGTOKEN3 page three text. It is here.");
    List<PdfChunk> chunks = chunk(pdf, defaults());
    String text = all(chunks);
    assertTrue(text.contains("PGTOKEN1") && text.contains("PGTOKEN3") && !text.contains("PGTOKEN2"), text);
    assertTrue(chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK));
    assertTrue(last(chunks).statusDetail().contains("decode_failed=1"), last(chunks).statusDetail());
    assertEquals(1, warnings.stream().filter(w -> w.startsWith("PDF-PAGE") && w.contains("page=2 code=DECODE_ERROR")).count(),
        String.join("\n", warnings));
  }

  @Test
  void partialFailureYieldsNoMarker() throws IOException {
    // page 2 fails PAGE_ERROR (a Type3 font above the CharProc cap); pages 1 and 3 still yield OK chunks
    List<PdfChunk> chunks = chunk(PdfFixtures.type3WithCharProcs(StreamDecodeGuard.MAX_CHAR_PROCS_PER_FONT + 1), defaults());
    String text = all(chunks);
    assertTrue(text.contains("First page text") && text.contains("Third page text"), text);
    assertTrue(chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK));
    assertTrue(last(chunks).statusDetail().contains("failed=1"), last(chunks).statusDetail());
    assertEquals(1, warnings.stream().filter(w -> w.startsWith("PDF-PAGE") && w.contains("page=2 code=PAGE_ERROR")).count(),
        String.join("\n", warnings));
  }

  @Test
  void hugePageTruncatedWithWarning() throws IOException {
    StringBuilder page = new StringBuilder();
    while (page.length() < 300_000) {
      page.append("Words fill a very long page. ");
    }
    // A glyph budget above the default, so the page is truncated rather than refused.
    List<PdfChunk> chunks = chunk(PdfFixtures.text(page.toString()),
        config(BPE, EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "524288"));
    assertTrue(last(chunks).statusDetail().contains("truncated=1"), last(chunks).statusDetail());
    assertTrue(warnings.stream().anyMatch(w -> w.contains("code=PAGE_TRUNCATED")));
    int covered = chunks.stream().mapToInt(c -> c.charCount() - c.overlapChars()).sum();
    assertTrue(covered <= 262_144, "covered " + covered);
  }

  // ------------------------------------------------------------------ staging and loading

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void oversizeDeclaredRejectedWithoutRead(boolean explicitCap) {
    AtomicInteger opened = new AtomicInteger();
    List<PdfChunk> chunks = new ArrayList<>();
    // Both the default and an explicit limit refuse a larger declared size before any read.
    PdfChunkerConfig capped = explicitCap
        ? config(BPE, EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "21474836480") : defaults();
    try (PdfChunkIterator iterator = chunker().open(() -> {
      opened.incrementAndGet();
      throw new IOException("must not be read");
    }, new PdfFileMeta("big", "big.pdf", 20L * 1024 * 1024 * 1024 + 1), capped, warnings::add)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    assertEquals(0, opened.get());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("INPUT_LIMIT: "));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void scratchBudgetUsesTheConfiguredLongCapOnly(boolean configured) throws IOException {
    PdfChunkerConfig config = configured
        ? config(BPE, EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "21474836480") : defaults();
    AtomicLong scratchCap = new AtomicLong(Long.MIN_VALUE);
    PdfBoxChunker observing = new PdfBoxChunker((source, meta, cfg) -> new PdfBoxPageSource(source, meta, cfg,
        staging.toFile(), true, (dir, maxBytes) -> {
          scratchCap.set(maxBytes);
          return PdfBoxPageSource.PDFBOX_SCRATCH_FILE.create(dir, maxBytes);
        }));
    List<PdfChunk> chunks = chunk(observing, PdfFixtures.text("Scratch uses a long byte count."), config);
    assertEquals("Scratch uses a long byte count.", all(chunks).trim());
    assertEquals(configured ? 20L << 30 : -1, scratchCap.get());
    assertTrue(stagingContents().isEmpty());
  }

  @Test
  void largeDeclaredSizeIsPreparedByDefault() throws IOException {
    // 1 GiB declared is below the default 20 GiB cap.
    byte[] pdf = textPdf(3, 30);
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = chunker().open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("big", "big.pdf", 1L << 30),
        defaults(), warnings::add)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    assertFalse(chunks.isEmpty());
    for (PdfChunk chunk : chunks) {
      assertEquals(PdfChunk.Status.OK, chunk.status(), chunk.statusDetail());
    }
  }

  @Test
  void oversizeUndeclaredAbortsDuringCopy() throws IOException {
    byte[] pdf = textPdf(3, 30);
    PdfChunkerConfig config = config(BPE, EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "1000");
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = chunker().open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("u", "u.pdf", -1),
        config, warnings::add)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("INPUT_LIMIT: "), chunks.get(0).statusDetail());
    assertTrue(stagingContents().isEmpty());
  }

  @Test
  void unreadableSourceMarkedSourceIo() throws IOException {
    // the supplier (blob bytes) fails -> FAILED SOURCE_IO naming the root cause, staging dir cleaned
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = chunker().open(() -> {
      throw new IOException("blob copy /tbl/.hoodie/.blob/x.pdf is missing");
    }, new PdfFileMeta("s", "s.pdf", 100), defaults(), warnings::add)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("SOURCE_IO: java.io.IOException: blob copy /tbl/.hoodie/.blob/x.pdf"),
        chunks.get(0).statusDetail());
    assertTrue(stagingContents().isEmpty());
  }

  // ------------------------------------------------------------------ round failures while staging

  /** Drains a chunker over {@code supplier}; the failure that leaves the iterator, or null when it ends normally. */
  private RuntimeException failureOf(PdfBoxChunker chunker, InputStreamSupplier supplier) {
    try (PdfChunkIterator iterator = chunker.open(supplier, new PdfFileMeta("s", "s.pdf", 100), defaults(), warnings::add)) {
      while (iterator.hasNext()) {
        iterator.next();
      }
      return null;
    } catch (RuntimeException e) {
      return e;
    }
  }

  @Test
  void sourceReadFailureOpeningTheSourceFailsTheRound() throws IOException {
    // the framework's blob stream throws an unchecked SourceReadException for a throttled, reset or denied open
    SourceReadException read = new SourceReadException("Failed to read the blob source", new IOException("503 slow down"));
    RuntimeException e = failureOf(chunker(), () -> {
      throw read;
    });
    assertSame(read, e == null ? null : PrepFailures.roundFailure(e), String.valueOf(e));
    assertTrue(stagingContents().isEmpty());
  }

  @Test
  void sourceReadFailureWhileCopyingFailsTheRound() throws IOException {
    byte[] pdf = textPdf(3, 30);
    SourceReadException read = new SourceReadException("Failed to read the blob source", new IOException("connection reset"));
    RuntimeException e = failureOf(chunker(), () -> new InputStream() {
      private int served;

      @Override
      public int read() {
        if (served >= pdf.length / 2) {
          throw read;
        }
        return pdf[served++] & 0xff;
      }
    });
    assertSame(read, e == null ? null : PrepFailures.roundFailure(e), String.valueOf(e));
    assertTrue(stagingContents().isEmpty());
  }

  @Test
  void stagingDirThatCannotBeCreatedIsAPlatformFailure() throws IOException {
    Path notADir = Files.write(staging.resolve("plain-file"), new byte[1]);
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> new PdfBoxPageSource(s, m, c, notADir.toFile(), true));
    byte[] pdf = textPdf(1, 3);
    RuntimeException e = failureOf(chunker, () -> new ByteArrayInputStream(pdf));
    assertTrue(e instanceof PrepPlatformException, String.valueOf(e));
  }

  @Test
  void stagedFileThatCannotBeWrittenIsAPlatformFailure() throws IOException {
    // the local staging dir goes away between its creation and the copy (a scratch disk failure): not the file's fault
    byte[] pdf = textPdf(1, 3);
    RuntimeException e = failureOf(chunker(), () -> {
      try (Stream<Path> dirs = Files.list(staging)) {
        for (Path dir : dirs.collect(Collectors.toList())) {
          deleteDir(dir);
        }
      }
      return new ByteArrayInputStream(pdf);
    });
    assertTrue(e instanceof PrepPlatformException, String.valueOf(e));
  }

  @Test
  void missingSourceStaysTheFilesOwnSourceIo() {
    // a source that is gone (FileNotFoundException passed through as an IOException) is the file's: SOURCE_IO, no round failure
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = chunker().open(() -> {
      throw new java.io.FileNotFoundException("blob copy is gone");
    }, new PdfFileMeta("s", "s.pdf", 100), defaults(), warnings::add)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("SOURCE_IO: "), chunks.get(0).statusDetail());
  }

  @Test
  void guardSpoolThatCannotBeCreatedIsAPlatformFailure() throws IOException {
    // a multi-filter chain spools its intermediate to the scratch dir; a scratch dir that cannot take it is the platform's
    byte[] pdf = PdfFixtures.oversizedInput(PdfFixtures.OversizedInput.OVERSIZED_FILTER_CHAIN, 1000);
    File gone = staging.resolve("no-such-dir").toFile();
    try (PDDocument doc = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(EmbeddingTableConfig.DEFAULT_PDF_MAX_DECODED_STREAM_BYTES, gone);
      PDPage page = doc.getPage(1);
      RuntimeException e = assertThrows(RuntimeException.class, () -> guard.checkPage(page));
      assertTrue(e instanceof PrepPlatformException, String.valueOf(e));
    }
  }

  @Test
  void stagedFileDeletedOnCloseAndOnFailure() throws IOException {
    chunk(textPdf(2, 10), defaults());
    assertTrue(stagingContents().isEmpty(), "after a normal end");
    chunk("%PDF-1.7 broken".getBytes(StandardCharsets.US_ASCII), defaults());
    assertTrue(stagingContents().isEmpty(), "after a load failure");
  }

  @Test
  void closeMidFileDeletesStagedFileAndStops() throws IOException {
    byte[] pdf = textPdf(6, 60);
    PdfChunkIterator iterator = chunker().open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("m", "m.pdf",
        pdf.length), defaults(), warnings::add);
    assertTrue(iterator.hasNext());
    iterator.next();
    assertFalse(stagingContents().isEmpty(), "staged while open");
    iterator.close();
    assertTrue(stagingContents().isEmpty());
    assertFalse(iterator.hasNext());
  }

  @Test
  void scratchInStagingDirAndDeleted() throws IOException {
    byte[] pdf = PdfFixtures.encrypted("Decrypted streams go to scratch.");   // encrypted: PDFBox writes decrypted streams to scratch
    PdfChunkIterator iterator = chunker().open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("s", "s.pdf",
        pdf.length), defaults(), warnings::add);
    iterator.hasNext();
    List<Path> during = stagingContents();
    iterator.close();
    assertTrue(during.stream().allMatch(p -> p.startsWith(staging)));
    assertTrue(stagingContents().isEmpty());
  }

  @Test
  void encryptedPdfMarkedEncrypted() throws IOException {
    List<PdfChunk> chunks = chunk(PdfFixtures.encrypted("Secret text."), defaults());
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("ENCRYPTED: "), chunks.get(0).statusDetail());
  }

  @Test
  void truncatedPdfMarkedLoadError() throws IOException {
    byte[] pdf = textPdf(5, 40);
    byte[] cut = Arrays.copyOf(pdf, pdf.length * 6 / 10);
    List<PdfChunk> chunks = chunk(cut, defaults());
    boolean marker = chunks.size() == 1 && chunks.get(0).status() == PdfChunk.Status.FAILED
        && chunks.get(0).statusDetail().startsWith("LOAD_ERROR");
    boolean repaired = chunks.stream().anyMatch(c -> c.status() == PdfChunk.Status.OK);
    assertTrue(marker || repaired, "either the marker or at least one OK chunk");
    System.out.println("truncated-at-60%: " + (marker ? "LOAD_ERROR marker" : "repaired, " + chunks.size() + " chunks"));
  }

  @Test
  void pagesExtractedOneAtATime() throws IOException {
    byte[] pdf = textPdf(50, 40);
    AtomicInteger extracted = new AtomicInteger();
    PdfBoxChunker spying = new PdfBoxChunker((s, m, c) -> new CountingSource(new PdfBoxPageSource(s, m, c, staging.toFile(),
        true), extracted));
    try (PdfChunkIterator iterator = spying.open(() -> new ByteArrayInputStream(pdf), new PdfFileMeta("p", "p.pdf",
        pdf.length), defaults(), warnings::add)) {
      assertTrue(iterator.hasNext());
      iterator.next();
      // the first chunk needs two cuts (hold-back); a page is ~2 KiB here, so the pull rule needs a handful of pages
      assertTrue(extracted.get() <= 8, "pages extracted before the first chunk: " + extracted.get());
      int chunks = 1;
      while (iterator.hasNext()) {
        iterator.next();
        chunks++;
        assertTrue(extracted.get() <= 8 + chunks * 2, "pages are pulled only as chunks are cut");
      }
    }
    assertEquals(50, extracted.get());
  }

  /** Counts next() calls of a delegate page source. */
  static final class CountingSource implements PageSource {
    private final PageSource delegate;
    private final AtomicInteger counter;

    CountingSource(PageSource delegate, AtomicInteger counter) {
      this.delegate = delegate;
      this.counter = counter;
    }

    @Override
    public void open() throws IOException {
      delegate.open();
    }

    @Override
    public boolean hasMorePages() {
      return delegate.hasMorePages();
    }

    @Override
    public PageText next() throws IOException {
      counter.incrementAndGet();
      return delegate.next();
    }

    @Override
    public long stagedBytes() {
      return delegate.stagedBytes();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  // ------------------------------------------------------------------ the guard and the caps

  private void assertOnlyPage2Failed(List<PdfChunk> chunks, String counter) {
    String text = all(chunks);
    assertTrue(text.contains("First page text") && text.contains("Third page text"), text);
    assertFalse(text.contains("fill page") || text.contains("deep"), text);
    assertTrue(last(chunks).statusDetail().contains(counter + "=1"), last(chunks).statusDetail());
  }

  @Test
  void flateOversizedInputPageSkipped() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.oversizedInput(PdfFixtures.OversizedInput.OVERSIZED_STREAM, 8192), config(BPE, EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "4096")),
        "decode_limited");
    assertTrue(warnings.stream().anyMatch(w -> w.contains("page=2 code=DECODE_LIMIT")));
  }

  @ParameterizedTest
  @EnumSource(PdfFixtures.OversizedInput.class)
  void everyOversizedInputConstructionFailsOnlyItsPage(PdfFixtures.OversizedInput kind) throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.oversizedInput(kind, 8192), config(BPE, EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "4096")), "decode_limited");
  }

  @Test
  void highObjectNumberOversizedInputFailsOnlyItsPage() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.highObjectNumberOversizedStream(8192), config(BPE, EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "4096")), "decode_limited");
  }

  @Test
  void type1ShapesAreScannedAsPdfboxSplitsThem() throws IOException {
    for (PdfFixtures.Type1Shape shape : PdfFixtures.Type1Shape.values()) {
      assertOnlyPage2Failed(chunk(PdfFixtures.largeType1Subrs(100_000, shape), defaults()), "decode_limited");
      List<PdfChunk> legit = chunk(PdfFixtures.largeType1Subrs(200, shape), defaults());   // neighbours: 200 Subrs pass in every shape
      assertFalse(last(legit).statusDetail() != null && last(legit).statusDetail().contains("decode_limited=1"),
          shape + ": " + last(legit).statusDetail());
    }
  }

  @Test
  void unfilteredOperandPileIsScannedToo() throws IOException {
    // an unfiltered content stream is scanned like a filtered one (100 000 operands before an operator)
    assertOnlyPage2Failed(chunk(PdfFixtures.operandPile(400_000, false), defaults()), "decode_limited");
  }

  @Test
  void operandPileAcrossContentsArrayFailsPage() throws IOException {
    // PDFBox parses a /Contents array as one sequence, so operands left pending at the end of one stream add up
    assertOnlyPage2Failed(chunk(PdfFixtures.contentsArray(3, 30_000, "", false), defaults()), "decode_limited");
    // the same stream object three times: scanned once, its summary replayed for the other two
    assertOnlyPage2Failed(chunk(PdfFixtures.contentsArray(3, 30_000, "", true), defaults()), "decode_limited");
    // control: each stream ends with an operator, so no more than 30 000 operands are ever pending
    List<PdfChunk> closed = chunk(PdfFixtures.contentsArray(3, 30_000, "n", true), defaults());
    assertFalse(last(closed).statusDetail() != null && last(closed).statusDetail().contains("decode_limited=1"),
        String.valueOf(last(closed).statusDetail()));
  }

  private static String ones(int operands) {
    StringBuilder ops = new StringBuilder();
    for (int i = 0; i < operands; i++) {
      ops.append("1 ");
    }
    return ops.toString();
  }

  @Test
  void contentsStreamBoundaryEndsCommentsAndInlineImages() throws IOException {
    // PDFBox puts a newline between /Contents streams, so a comment or an inline image ending a stream ends there
    assertOnlyPage2Failed(chunk(PdfFixtures.contentsStreams("%", ones(100_000)), defaults()), "decode_limited");
    assertOnlyPage2Failed(chunk(PdfFixtures.contentsStreams("q BI /W 1 /H 1 /BPC 8 /CS /G ID x EI", ones(100_000) + "Q\n"),
        defaults()), "decode_limited");
    // neighbour: a string left open at the end of a stream continues into the next one in PDFBox too (no newline ends it)
    List<PdfChunk> open = chunk(PdfFixtures.contentsStreams("BT /F1 10 Tf 50 700 Td (abc", ones(100_000) + ") Tj ET\n"), defaults());
    assertFalse(last(open).statusDetail() != null && last(open).statusDetail().contains("decode_limited=1"),
        String.valueOf(last(open).statusDetail()));
  }

  @Test
  void formReusedAsPageContentPasses() throws IOException {
    // a stream first checked as a form, then met in a later page's /Contents, is scanned again in sequence (no failure)
    List<PdfChunk> chunks = chunk(PdfFixtures.formReusedAsContents(), defaults());
    assertNull(last(chunks).statusDetail(), String.valueOf(last(chunks).statusDetail()));
  }

  @Test
  void streamPassedAsOpaqueIsStillScannedAsCMap() throws IOException {
    // the PASSED cache is per kind: a stream first met as an unscanned /FontFile2 is scanned when reused as a /ToUnicode
    assertOnlyPage2Failed(chunk(PdfFixtures.largeBfrange(true), defaults()), "decode_limited");
  }

  @Test
  void hugeTjArrayFailsPage() throws IOException {
    StringBuilder ops = new StringBuilder("BT /F1 10 Tf 50 700 Td [");
    for (int i = 0; i < 70_000; i++) {
      ops.append("(a) -10 ");
    }
    byte[] pdf = PdfFixtures.threePagesWithPage2(ops.append("] TJ ET\n").toString());
    assertOnlyPage2Failed(chunk(pdf, defaults()), "decode_limited");
  }

  @Test
  void legitimateStructuresPassThePreScans() throws IOException {
    // controls: a full two-byte ToUnicode range (65 536 codes), a 200-entry Subrs, a 30 000-element TJ array, inline image
    List<PdfChunk> range = chunk(PdfFixtures.bfrangeOf("<0000> <FFFF> <0000>"), defaults());
    assertFalse(last(range).statusDetail() != null && last(range).statusDetail().contains("decode_limited=1"),
        String.valueOf(last(range).statusDetail()));
    List<PdfChunk> subrs = chunk(PdfFixtures.largeType1Subrs(200), defaults());
    assertFalse(last(subrs).statusDetail() != null && last(subrs).statusDetail().contains("decode_limited=1"),
        String.valueOf(last(subrs).statusDetail()));
    StringBuilder ops = new StringBuilder("BT /F1 10 Tf 50 700 Td [");
    for (int i = 0; i < 30_000; i++) {
      ops.append("(a) -10 ");
    }
    ops.append("] TJ ET\nq 10 0 0 10 100 100 cm BI /W 4 /H 4 /BPC 8 /CS /G ID \u0000[(<>%]\u00ff\u0001 EI Q\n");  // escaped binary inline-image bytes
    List<PdfChunk> tj = chunk(PdfFixtures.threePagesWithPage2(ops.toString()), defaults());
    assertNull(last(tj).statusDetail(), "30 000 TJ elements and an inline image with delimiter bytes pass");
  }

  @Test
  void failedChainLeavesNoGuardTempFile() throws IOException {
    // a multi-filter chain writes an intermediate temp file; a DECODE_LIMIT on it must not leave it behind
    byte[] pdf = PdfFixtures.oversizedInput(PdfFixtures.OversizedInput.OVERSIZED_FILTER_CHAIN, 8192);
    Path guardTemp = Files.createDirectories(staging.resolve("guard-temp"));
    try (PDDocument doc = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(4096,
          guardTemp.toFile());
      assertPageFails(guard, doc, 1, PdfFailureCode.DECODE_LIMIT);
      try (Stream<Path> left = Files.list(guardTemp)) {
        assertEquals(0, left.count(), "no guard temp file survives a failed chain");
      }
    }
  }

  private static void deleteDir(Path dir) throws IOException {
    try (Stream<Path> left = Files.list(dir)) {
      left.forEach(p -> p.toFile().delete());
    }
    dir.toFile().delete();
  }

  @Test
  void imageCodecsRefusedWithoutDecoding() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.dctForm(), defaults()), "decode_limited");
    assertOnlyPage2Failed(chunk(PdfFixtures.ccittToUnicode(), defaults()), "decode_limited");
  }

  @Test
  void type3CharProcCapFailsPage() throws IOException {
    List<PdfChunk> over = chunk(PdfFixtures.type3WithCharProcs(1025), defaults());
    assertTrue(last(over).statusDetail().contains("failed=1"), last(over).statusDetail());
    List<PdfChunk> atCap = chunk(PdfFixtures.type3WithCharProcs(1024), defaults());
    assertNull(last(atCap).statusDetail(), "1 024 CharProcs is within the cap");
  }

  @Test
  void sharedOversizedInputFailsFastOnSecondPage() throws IOException {
    byte[] pdf = PdfFixtures.sharedOversizedStream(8192);
    List<PdfChunk> chunks = chunk(pdf, config(BPE, EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES.key(), "4096"));
    assertTrue(last(chunks).statusDetail().contains("decode_limited=2"), last(chunks).statusDetail());
    try (PDDocument doc = Loader.loadPDF(pdf)) {
      StreamDecodeGuard guard = new StreamDecodeGuard(4096,
          staging.toFile());
      guard.checkPage(doc.getPage(0));
      long afterPage1 = guard.decodedStreams();
      assertPageFails(guard, doc, 1, PdfFailureCode.DECODE_LIMIT);
      long afterPage2 = guard.decodedStreams();
      assertEquals(afterPage1 + 2, afterPage2, "page 2: its content stream, then the oversized input");
      assertPageFails(guard, doc, 2, PdfFailureCode.DECODE_LIMIT);
      assertEquals(afterPage2 + 1, guard.decodedStreams(),
          "page 3 decodes only its own content stream; the shared oversized input fails from the FAILED cache, not decoded again");
    }
  }

  @Test
  void undecodableStreamIsDecodedOncePerDocument() throws IOException {
    // a stream that fails to decode is remembered like a capped one, not decoded again on every page
    try (PDDocument doc = Loader.loadPDF(PdfFixtures.undecodableSharedStream(false))) {
      StreamDecodeGuard guard = new StreamDecodeGuard(EmbeddingTableConfig.DEFAULT_PDF_MAX_DECODED_STREAM_BYTES,
          staging.toFile());
      guard.checkPage(doc.getPage(0));
      assertEquals(2, guard.decodedStreams(), "page 1: its content stream, then the font file that fails to decode");
      guard.checkPage(doc.getPage(1));
      guard.checkPage(doc.getPage(2));
      assertEquals(4, guard.decodedStreams(), "pages 2-3 decode only their own content streams; the font file is skipped");
    }
    try (PDDocument doc = Loader.loadPDF(PdfFixtures.undecodableSharedStream(true))) {
      StreamDecodeGuard guard = new StreamDecodeGuard(EmbeddingTableConfig.DEFAULT_PDF_MAX_DECODED_STREAM_BYTES,
          staging.toFile());
      assertPageFails(guard, doc, 0, PdfFailureCode.DECODE_ERROR);
      assertEquals(1, guard.decodedStreams());
      assertPageFails(guard, doc, 1, PdfFailureCode.DECODE_ERROR);
      assertPageFails(guard, doc, 2, PdfFailureCode.DECODE_ERROR);
      assertEquals(1, guard.decodedStreams(), "pages 2-3 classify the content stream from the cache, without decoding again");
    }
  }

  @Test
  void unfilteredStreamsAreScannedButNotCapped() throws IOException {
    // the decoded-size cap is about decoding; an unfiltered stream stays a view on the staged file, so only its
    // structure is checked
    StringBuilder ops = new StringBuilder();
    for (int i = 0; i < 2_000; i++) {
      ops.append("BT /F1 10 Tf 50 700 Td (Some page text here.) Tj ET\n");
    }
    try (PDDocument raw = Loader.loadPDF(PdfFixtures.contentsStreams(ops.toString()));
         PDDocument flate = Loader.loadPDF(PdfFixtures.threePagesWithPage2(ops.toString()))) {
      new StreamDecodeGuard(10_000, staging.toFile()).checkPage(raw.getPage(1));   // ~100 KB unfiltered: passes
      assertPageFails(new StreamDecodeGuard(10_000, staging.toFile()), flate, 1, PdfFailureCode.DECODE_LIMIT);
    }
  }

  @Test
  void unfilteredFontFilesAndCMapsKeepTheCap() throws IOException {
    // PDFBox copies a font file and a CMap onto the heap whole (PDType1Font: toByteArray, copyOfRange), so the
    // decoded-size cap applies to them filtered or not; a 200-byte guard refuses a ~700-byte unfiltered font file and CMap
    try (PDDocument font = Loader.loadPDF(PdfFixtures.largeType1Subrs(200));
         PDDocument cmap = Loader.loadPDF(PdfFixtures.bfrangeOf("<0000> <00FF> <0000>"))) {
      assertPageFails(new StreamDecodeGuard(200, staging.toFile()), font, 1, PdfFailureCode.DECODE_LIMIT);
      assertPageFails(new StreamDecodeGuard(100, staging.toFile()), cmap, 1, PdfFailureCode.DECODE_LIMIT);
    }
  }

  @Test
  void unfilteredFontFile2And3KeepTheCap() throws IOException {
    // fontbox copies an embedded TrueType (TTFParser) or CFF (CFFParser) font onto the heap, filtered or not
    PdfFixtures.OversizedInput[] kinds = {PdfFixtures.OversizedInput.LARGE_UNFILTERED_EMBEDDED_FONT, PdfFixtures.OversizedInput.LARGE_UNFILTERED_CFF_FONT};
    for (PdfFixtures.OversizedInput kind : kinds) {
      try (PDDocument doc = Loader.loadPDF(PdfFixtures.oversizedInput(kind, 1000))) {
        assertPageFails(new StreamDecodeGuard(999, staging.toFile()), doc, 1, PdfFailureCode.DECODE_LIMIT);
        new StreamDecodeGuard(1000, staging.toFile()).checkPage(doc.getPage(1));   // neighbour: a font of exactly the cap passes
      }
    }
  }

  @Test
  void unfilteredCidToGidMapKeepsTheCap() throws IOException {
    // an unfiltered CIDToGIDMap is copied onto the heap whole (PDCIDFont.readCIDToGIDMap), like a font file
    try (PDDocument map = Loader.loadPDF(PdfFixtures.oversizedInput(PdfFixtures.OversizedInput.LARGE_UNFILTERED_CID_TO_GID_MAP, 1000))) {
      assertPageFails(new StreamDecodeGuard(999, staging.toFile()), map, 1, PdfFailureCode.DECODE_LIMIT);
      new StreamDecodeGuard(1000, staging.toFile()).checkPage(map.getPage(1));   // neighbour: a map of exactly the cap passes
    }
  }

  private static void assertPageFails(StreamDecodeGuard guard, PDDocument doc, int index, PdfFailureCode code) {
    try {
      guard.checkPage(doc.getPage(index));
    } catch (PageFailure e) {
      assertEquals(code, e.code());
      return;
    }
    throw new AssertionError("page " + index + " passed the guard");
  }

  @Test
  void glyphCapStopsPage() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.manyGlyphs(5000), smallCaps()), "glyph_limited");
    assertTrue(warnings.stream().anyMatch(w -> w.contains("page=2 code=PAGE_GLYPH_LIMIT")));
    warnings.clear();
    List<PdfChunk> control = chunk(PdfFixtures.manyGlyphs(500), smallCaps());
    assertNull(last(control).statusDetail(), "500 glyphs pass");
  }

  @Test
  void formRepetitionCapped() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.repeatedForm(100, 50), smallCaps()), "glyph_limited");
  }

  @Test
  void actualTextRepetitionCapped() throws IOException {
    // 3 glyphs carrying 3 x 2 000 chars of /ActualText: above the 1 000 glyph-text cap, while the glyph count stays at 3
    assertOnlyPage2Failed(chunk(PdfFixtures.repeatedActualText(2000, 3), smallCaps()), "glyph_limited");
  }

  @Test
  void pageBetweenCharAndGlyphCapsIsTruncatedNotFailed() throws IOException {
    // 800 glyphs of text with max.page.chars 600 and max.page.glyphs 1 000 → truncated to 600 chars, page kept
    PdfChunkerConfig config = config(BPE, EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS.key(), "256",
        EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key(), "600", EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS.key(), "1000");
    StringBuilder text = new StringBuilder();
    while (text.length() < 800) {
      text.append("Short words here. ");
    }
    List<PdfChunk> chunks = chunk(PdfFixtures.text(text.toString()), config);
    assertTrue(last(chunks).statusDetail().contains("truncated=1 glyph_limited=0"), last(chunks).statusDetail());
  }

  @Test
  void graphicsStateStackCapped() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.graphicsStateStack(2000), defaults()), "graphics_limited");
    List<PdfChunk> control = chunk(PdfFixtures.graphicsStateStack(500), defaults());
    assertNull(last(control).statusDetail());
    assertTrue(all(control).contains("deep"));
  }

  @Test
  void bmcStackCapped() throws IOException {
    assertOnlyPage2Failed(chunk(PdfFixtures.bmcStack(2000), defaults()), "graphics_limited");
    assertNull(last(chunk(PdfFixtures.bmcStack(500), defaults())).statusDetail());
  }

  @Test
  void unbalancedBdcPerPageBounded() throws IOException {
    List<PdfChunk> chunks = chunk(PdfFixtures.unbalancedBdc(10, 1000), defaults());
    assertNull(last(chunks).statusDetail(), "a fresh stripper per page: 1 000 open spans per page never reach 1 024");
    for (int p = 1; p <= 10; p++) {
      assertTrue(all(chunks).contains("Page " + p + " text survives"));
    }
  }

  @Test
  void taggedPdfUnaffected() throws IOException {
    List<PdfChunk> chunks = chunk(PdfFixtures.tagged(3, 2000), defaults());
    assertNull(last(chunks).statusDetail());
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
  void noImportsFromUnstructuredHelpers() throws IOException {
    for (Path source : mainSources()) {
      String code = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
      for (String forbidden : Arrays.asList("helpers.unstructured", "transform.embedding", "transform.unstructured")) {
        assertFalse(code.contains(forbidden), source + " imports from the separate unstructured helpers package " + forbidden);
      }
    }
  }

  @Test
  void noForbiddenWholeDocumentCalls() throws IOException {
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

  @Test
  void wordCountConfigChunksRealPdf() throws IOException {
    List<PdfChunk> chunks = chunk(textPdf(2, 30), config(WORDS, EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "40"));
    assertTrue(chunks.stream().allMatch(c -> c.wordCount() <= 40 && c.tokenCount() == c.wordCount()));
    assertEquals("word-count", chunks.get(0).tokenizerId());
  }
}
