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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceReadException;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenUnit;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerException;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parser-free iterator over fake page sources: lifecycle, records, the
 * hold-back loss signal, error isolation, interruption, WARN budget and the adapter memory bound.
 */
class TestPdfChunkIterator {

  private static final Tokenizer WORDS = new WordCountTokenizer();
  private static final PdfFileMeta META = new PdfFileMeta("fid-1", "file:///docs/x.pdf");

  /** One page of a fake source: returns text or throws. */
  @FunctionalInterface
  interface FakePage {
    PageText extract() throws Exception;
  }

  static FakePage text(String text) {
    return () -> new PageText(text, false);
  }

  static FakePage throwing(Throwable failure) {
    return () -> {
      if (failure instanceof Exception) {
        throw (Exception) failure;
      }
      throw (Error) failure;
    };
  }

  /** A page source over a fixed or generated list of pages. */
  static class FakeSource implements PageSource {
    private final IntFunction<FakePage> pages;
    private final int pageCount;
    private final InputStreamSupplier supplier;
    private int next;
    boolean opened;
    boolean closed;

    FakeSource(InputStreamSupplier supplier, int pageCount, IntFunction<FakePage> pages) {
      this.supplier = supplier;
      this.pageCount = pageCount;
      this.pages = pages;
    }

    @Override
    public void open() throws java.io.IOException {
      supplier.open().close();
      opened = true;
    }

    @Override
    public boolean hasMorePages() {
      return next < pageCount;
    }

    @Override
    public PageText next() throws java.io.IOException {
      FakePage page = pages.apply(next++);
      try {
        return page.extract();
      } catch (java.io.IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public long stagedBytes() {
      return 0;
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  /** A source whose open() throws an unexpected (non-PdfFileFailure) exception. */
  static final class IllegalStateOnOpen extends FakeSource {
    IllegalStateOnOpen(InputStreamSupplier supplier) {
      super(supplier, 0, i -> null);
    }

    @Override
    public void open() {
      throw new IllegalStateException("unexpected state while opening");
    }
  }

  private final List<String> warnings = new ArrayList<>();
  private final List<FakeSource> sources = new ArrayList<>();

  private PdfChunkIterator open(PdfChunkerConfig config, List<FakePage> pages) {
    return open(config, pages.size(), pages::get, () -> new ByteArrayInputStream(new byte[1]));
  }

  private PdfChunkIterator open(PdfChunkerConfig config, int pageCount, IntFunction<FakePage> pages,
                                InputStreamSupplier supplier) {
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> {
      FakeSource source = new FakeSource(s, pageCount, pages);
      sources.add(source);
      return source;
    });
    return chunker.open(supplier, META, config, warnings::add);
  }

  static PdfChunkerConfig config(Tokenizer tokenizer, String... keyValues) {
    TypedProperties props = new TypedProperties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return PdfChunkerConfig.from(props, tokenizer);
  }

  private static PdfChunkerConfig small() {
    return config(WORDS, EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
  }

  static String sentences(String prefix, int count) {
    StringBuilder sb = new StringBuilder();
    for (int s = 0; s < count; s++) {
      sb.append("Sentence ").append(prefix).append(s).append(" has six words here. ");
    }
    return sb.toString().trim();
  }

  private static List<PdfChunk> drain(ClosableIterator<PdfChunk> iterator) {
    List<PdfChunk> chunks = new ArrayList<>();
    while (iterator.hasNext()) {
      chunks.add(iterator.next());
    }
    iterator.close();
    return chunks;
  }

  private static void drainInto(ClosableIterator<PdfChunk> iterator, List<PdfChunk> emitted) {
    while (iterator.hasNext()) {
      emitted.add(iterator.next());
    }
  }

  // ------------------------------------------------------------------ lifecycle

  @Test
  void openDoesNoIo() {
    AtomicBoolean called = new AtomicBoolean();
    PdfChunkIterator iterator = open(small(), 1, i -> text(sentences("a", 2)), () -> {
      called.set(true);
      return new ByteArrayInputStream(new byte[1]);
    });
    assertFalse(called.get(), "open() must not call the supplier");
    assertTrue(sources.isEmpty() || !sources.get(0).opened);
    assertTrue(iterator.hasNext());
    assertTrue(called.get());
    iterator.close();
  }

  @Test
  void openReturnsHudiClosableIterator() {
    PdfChunker chunker = new PdfBoxChunker((s, m, c) -> new FakeSource(s, 1, i -> text("One sentence.")));
    ClosableIterator<PdfChunk> iterator = chunker.open(() -> new ByteArrayInputStream(new byte[1]), META, small());
    assertEquals("One sentence.", drain(iterator).get(0).text());
  }

  @Test
  void hasNextIdempotent() {
    PdfChunkIterator iterator = open(small(), Collections.singletonList(text(sentences("a", 6))));
    for (int i = 0; i < 5; i++) {
      assertTrue(iterator.hasNext());
    }
    PdfChunk first = iterator.next();
    assertEquals(0, first.chunkId());
    assertTrue(iterator.hasNext() && iterator.hasNext());
    assertEquals(1, iterator.next().chunkId());
    iterator.close();
  }

  @Test
  void nextAfterEndThrows() {
    PdfChunkIterator iterator = open(small(), Collections.singletonList(text("Only one.")));
    drain(iterator);
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  void noThreadsStarted() {
    int before = Thread.activeCount();
    drain(open(small(), Arrays.asList(text(sentences("a", 20)), text(sentences("b", 20)))));
    assertEquals(before, Thread.activeCount());
  }

  @Test
  void everyRecordCarriesTheSourcePathSha() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(sentences("a", 9)), text(sentences("b", 9)))));
    assertTrue(chunks.size() > 3);
    assertTrue(chunks.stream().allMatch(c -> "fid-1".equals(c.sourcePathSha())));
  }

  @Test
  void chunkIdsAreConsecutiveFromZero() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(sentences("a", 9)), text(sentences("b", 9)))));
    for (int i = 0; i < chunks.size(); i++) {
      assertEquals(i, chunks.get(i).chunkId());
      assertEquals(PdfChunk.Status.OK, chunks.get(i).status());
    }
  }

  @Test
  void closeTwiceIsNoop() {
    PdfChunkIterator iterator = open(small(), Collections.singletonList(text(sentences("a", 9))));
    assertTrue(iterator.hasNext());
    iterator.close();
    iterator.close();
    assertTrue(sources.get(0).closed);
    assertFalse(iterator.hasNext(), "closing mid-file emits nothing further");
  }

  // ------------------------------------------------------------------ records

  @Test
  void lossSummaryOnLastChunk() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(sentences("a", 6)),
        throwing(new IllegalStateException("broken content stream")), text(sentences("c", 6)))));
    PdfChunk last = chunks.get(chunks.size() - 1);
    assertEquals(PdfChunk.Status.OK, last.status());
    assertEquals("PAGES_LOST failed=1 truncated=0 glyph_limited=0 decode_limited=0 graphics_limited=0 page_limit=false "
        + "file_error=none", last.statusDetail());
    for (PdfChunk chunk : chunks.subList(0, chunks.size() - 1)) {
      assertNull(chunk.statusDetail());
    }
    assertTrue(chunks.stream().noneMatch(c -> c.pageStart() == 2 && c.pageEnd() == 2));
  }

  /** Counts words but throws on any text containing FAIL_TOKEN. */
  static final class FailingTokenizer implements Tokenizer {
    @Override
    public int countTokens(String text) {
      if (text.contains("FAIL_TOKEN")) {
        throw new IllegalStateException("tokenizer failed");
      }
      return WORDS.countTokens(text);
    }

    @Override
    public String id() {
      return "failing";
    }

    @Override
    public TokenUnit unit() {
      return TokenUnit.WORDS;
    }
  }

  @Test
  void tokenizerErrorAfterChunksSignalled() {
    PdfChunkerConfig config = config(new FailingTokenizer(), EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20",
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    List<PdfChunk> chunks = drain(open(config, Arrays.asList(text(sentences("a", 12)), text("FAIL_TOKEN is here. " + sentences("c", 6)))));
    assertTrue(chunks.size() >= 2);
    PdfChunk last = chunks.get(chunks.size() - 1);
    assertEquals(PdfChunk.Status.OK, last.status());
    assertTrue(last.statusDetail().endsWith("file_error=TOKENIZER_ERROR"), last.statusDetail());
    assertTrue(chunks.stream().noneMatch(c -> c.text().contains("FAIL_TOKEN") || c.text().contains("c0")),
        "nothing is flushed after a tokenizer failure");
  }

  @Test
  void tokenizerThrowsOnSecondFileOnly() {
    PdfChunkerConfig config = config(new FailingTokenizer(), EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20",
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    List<PdfChunk> first = drain(open(config, Collections.singletonList(text(sentences("a", 6)))));
    List<PdfChunk> second = drain(open(config, Collections.singletonList(text("FAIL_TOKEN. " + sentences("b", 6)))));
    List<PdfChunk> third = drain(open(config, Collections.singletonList(text(sentences("c", 6)))));
    assertTrue(first.size() > 1 && third.size() > 1);
    assertEquals(1, second.size());
    assertEquals(PdfChunk.Status.FAILED, second.get(0).status());
    assertTrue(second.get(0).statusDetail().startsWith("TOKENIZER_ERROR: "), second.get(0).statusDetail());
    assertEquals(0, second.get(0).chunkId());
    assertNull(second.get(0).text());
  }

  @Test
  void corruptFileYieldsOneFailedMarker() {
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> new FakeSource(s, 0, i -> null) {
      @Override
      public void open() {
        throw new PdfFileFailure(PdfFailureCode.LOAD_ERROR, "not a pdf");
      }
    });
    List<PdfChunk> chunks = drain(chunker.open(() -> new ByteArrayInputStream(new byte[1]), META, small(), warnings::add));
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("LOAD_ERROR: "));
  }

  @Test
  void unexpectedFileLevelExceptionIsInternal() {
    // an exception outside a page extraction that is not a PdfFileFailure gives FAILED INTERNAL
    PageSourceFactory factory = (s, m, c) -> new IllegalStateOnOpen(s);
    PdfChunkIterator iterator = new PdfBoxChunker(factory).open(() -> new ByteArrayInputStream(new byte[1]), META, small(),
        warnings::add);
    List<PdfChunk> chunks = drain(iterator);
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("INTERNAL: java.lang.IllegalStateException: unexpected state"),
        chunks.get(0).statusDetail());
    List<PdfChunk> nextFile = drain(open(small(), Collections.singletonList(text("Next file."))));
    assertEquals(PdfChunk.Status.OK, nextFile.get(0).status(), "the next file is unaffected");
  }

  // ------------------------------------------------------------------ round failures: never a marker, never a truncated file

  /** Counts words but throws a TokenizerException of {@code kind} on any text containing FAIL_TOKEN. */
  static final class KindTokenizer implements Tokenizer {
    private final TokenizerException.Kind kind;

    KindTokenizer(TokenizerException.Kind kind) {
      this.kind = kind;
    }

    @Override
    public int countTokens(String text) {
      if (text.contains("FAIL_TOKEN")) {
        throw new TokenizerException(kind, "tokenizer runtime failed", null);
      }
      return WORDS.countTokens(text);
    }

    @Override
    public String id() {
      return "kind-" + kind;
    }

    @Override
    public TokenUnit unit() {
      return TokenUnit.WORDS;
    }
  }

  @Test
  void retryableTokenizerFailureAfterChunksFailsTheRound() {
    PdfChunkerConfig config = config(new KindTokenizer(TokenizerException.Kind.RETRYABLE), EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(),
        "20", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    PdfChunkIterator iterator = open(config, Arrays.asList(text(sentences("a", 12)), text("FAIL_TOKEN is here. " + sentences("c", 6))));
    List<PdfChunk> emitted = new ArrayList<>();
    RuntimeException e = assertThrows(RuntimeException.class, () -> drainInto(iterator, emitted));
    assertTrue(PrepFailures.roundFailure(e) instanceof TokenizerException, String.valueOf(e));
    assertTrue(emitted.stream().allMatch(c -> c.status() == PdfChunk.Status.OK && c.statusDetail() == null),
        "no chunk carries a loss summary: the file is not committed truncated");
    assertTrue(sources.get(0).closed, "the page source is closed before the failure leaves");
  }

  @Test
  void retryableTokenizerFailureBeforeAnyChunkFailsTheRound() {
    PdfChunkerConfig config = config(new KindTokenizer(TokenizerException.Kind.RETRYABLE), EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(),
        "20", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    PdfChunkIterator iterator = open(config, Collections.singletonList(text("FAIL_TOKEN. " + sentences("b", 6))));
    RuntimeException e = assertThrows(RuntimeException.class, () -> drain(iterator));
    assertTrue(PrepFailures.roundFailure(e) instanceof TokenizerException, String.valueOf(e));
  }

  @Test
  void permanentTokenizerFailureStaysTheFilesOwn() {
    PdfChunkerConfig config = config(new KindTokenizer(TokenizerException.Kind.PERMANENT), EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(),
        "20", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
    List<PdfChunk> chunks = drain(open(config, Collections.singletonList(text("FAIL_TOKEN. " + sentences("b", 6)))));
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("TOKENIZER_ERROR: "), chunks.get(0).statusDetail());
  }

  @Test
  void sourceReadFailureOpeningTheSourceFailsTheRound() {
    SourceReadException read = new SourceReadException("Failed to read the blob source", new java.io.IOException("throttled"));
    PdfChunkIterator iterator = open(small(), 1, i -> text("One."), () -> {
      throw read;
    });
    RuntimeException e = assertThrows(RuntimeException.class, () -> drain(iterator));
    assertSame(read, PrepFailures.roundFailure(e));
  }

  @Test
  void platformFailureOnAPageFailsTheRound() {
    PrepPlatformException scratch = new PrepPlatformException("local scratch disk full", new java.io.IOException("ENOSPC"));
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 6)), throwing(scratch), text(sentences("c", 6))));
    List<PdfChunk> emitted = new ArrayList<>();
    RuntimeException e = assertThrows(RuntimeException.class, () -> drainInto(iterator, emitted));
    assertSame(scratch, PrepFailures.roundFailure(e));
    assertEquals(0, iterator.stats().pagesFailed(), "not counted as a page error");
    assertTrue(emitted.stream().allMatch(c -> c.statusDetail() == null), "no loss summary");
    assertTrue(sources.get(0).closed);
  }

  @Test
  void platformFailureWrappedInAPageFailureFailsTheRound() {
    PrepPlatformException scratch = new PrepPlatformException("local scratch disk full", new java.io.IOException("ENOSPC"));
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 6)),
        throwing(new PageFailure(PdfFailureCode.PAGE_ERROR, "decode guard", scratch))));
    RuntimeException e = assertThrows(RuntimeException.class, () -> drain(iterator));
    assertSame(scratch, PrepFailures.roundFailure(e));
  }

  @Test
  void whitespaceOnlyPagesAreNoText() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(" \n\t "), text("\u200B\u00AD"), text(""))));
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.NO_TEXT, chunks.get(0).status());
    assertNull(chunks.get(0).statusDetail());
    assertEquals(3, open(small(), Collections.emptyList()).stats().pagesSeen() + 3);
  }

  @Test
  void partialFailureYieldsNoMarker() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(throwing(new RuntimeException("p1")), text(sentences("b", 4)))));
    assertTrue(chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK));
  }

  @Test
  void allPagesFailedIsFailedMarker() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(throwing(new RuntimeException("p1")), text(""))));
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("ALL_PAGES_FAILED"));
  }

  @Test
  void mixedPageFailuresKeepTheAggregateFailureCode() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(
        throwing(new PageFailure(PdfFailureCode.DECODE_ERROR, "content cannot decode")),
        throwing(new PageFailure(PdfFailureCode.PAGE_ERROR, "page cannot be read")))));
    assertEquals(1, chunks.size());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("ALL_PAGES_FAILED:"), chunks.get(0).statusDetail());
    assertTrue(chunks.get(0).statusDetail().contains("failed=1"), chunks.get(0).statusDetail());
    assertTrue(chunks.get(0).statusDetail().contains("decode_failed=1"), chunks.get(0).statusDetail());
  }

  // ------------------------------------------------------------------ errors

  @Test
  void stackOverflowOnPageIsolated() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(text(sentences("a", 4)), throwing(new StackOverflowError()),
        text(sentences("c", 4)))));
    assertTrue(chunks.stream().anyMatch(c -> c.text().contains("c3")));
    assertTrue(chunks.get(chunks.size() - 1).statusDetail().contains("failed=1"));
  }

  @Test
  void outOfMemoryErrorPropagates() {
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 4)), throwing(new OutOfMemoryError("fake"))));
    assertThrows(OutOfMemoryError.class, () -> drain(iterator));
    iterator.close();
    assertTrue(sources.get(0).closed);
  }

  @Test
  void interruptPropagatesNotPageError() {
    FakePage interrupting = () -> {
      Thread.currentThread().interrupt();
      throw new ClosedChannelException();
    };
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 2)), interrupting, text(sentences("c", 4))));
    try {
      PdfChunkerInterruptedException e = assertThrows(PdfChunkerInterruptedException.class, () -> drain(iterator));
      assertTrue(e.getCause() instanceof InterruptedIOException);
    } finally {
      Thread.interrupted();                            // clear the flag for the next test
    }
    assertEquals(0, iterator.stats().pagesFailed(), "the interrupt is not counted as a page error");
    assertTrue(sources.get(0).closed, "the source (staged file) is closed");
  }

  @Test
  void interruptWithClearedFlagStillPropagates() {
    // a library caught InterruptedException and cleared the flag, but its InterruptedIOException says it all
    FakePage page = () -> {
      throw new java.io.IOException("read failed", new InterruptedIOException("stream interrupted"));
    };
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 2)), page));
    assertThrows(PdfChunkerInterruptedException.class, () -> drain(iterator));
    assertEquals(0, iterator.stats().pagesFailed());
  }

  @Test
  void failingSourceCloseKeepsTheRecords() {
    // a throwing PageSource.close() must not replace the last OK chunk with a FAILED marker
    PdfBoxChunker chunker = new PdfBoxChunker((s, m, c) -> new FakeSource(s, 1, i -> text(sentences("a", 6))) {
      @Override
      public void close() {
        throw new IllegalStateException("close failed");
      }
    });
    List<PdfChunk> chunks = drain(chunker.open(() -> new ByteArrayInputStream(new byte[1]), META, small(), warnings::add));
    assertTrue(chunks.size() > 1);
    assertTrue(chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK));
    assertTrue(warnings.stream().anyMatch(w -> w.contains("closing the page source failed")));
  }

  @Test
  void cleanNoTextFileWarnsWithoutALossSummary() {
    drain(open(small(), Arrays.asList(text(""), text(""))));
    assertEquals(1, warnings.size());
    assertTrue(warnings.get(0).contains("status=NO_TEXT pages=2 (no text)"), warnings.get(0));
    assertFalse(warnings.get(0).contains("PAGES_LOST"));
  }

  @Test
  void warnLinesBoundedPerFile() {
    List<FakePage> pages = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      pages.add(throwing(new IllegalArgumentException("bad page " + i)));
    }
    List<PdfChunk> chunks = drain(open(small(), pages));
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertEquals(11, warnings.size(), String.join("\n", warnings));
    assertTrue(warnings.get(10).startsWith("PDF-FILE ") && warnings.get(10).contains("failed=50"));
  }

  @Test
  void suppressedWarningsOnALossFreeFileStillGetASummary() {
    // 12 pages each with an unpaired surrogate is an OK, loss-free file whose 11th and 12th WARNs are
    // suppressed; one summary line says so
    List<FakePage> pages = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      pages.add(text("Page " + i + " has a lone half " + (char) 0xD800 + " here."));
    }
    List<PdfChunk> chunks = drain(open(small(), pages));
    assertEquals(PdfChunk.Status.OK, chunks.get(chunks.size() - 1).status());
    assertEquals(11, warnings.size(), String.join("\n", warnings));
    assertTrue(warnings.get(10).startsWith("PDF-FILE ") && warnings.get(10).contains("status=OK")
        && warnings.get(10).contains("pageWarnings=12"), warnings.get(10));
    // neighbour: 10 such pages are all reported and need no summary
    warnings.clear();
    drain(open(small(), pages.subList(0, 10)));
    assertEquals(10, warnings.size(), String.join("\n", warnings));
    assertTrue(warnings.stream().noneMatch(w -> w.startsWith("PDF-FILE ")));
  }

  @Test
  void warnNeverContainsText() {
    StringBuilder huge = new StringBuilder();
    for (int i = 0; i < 300_000; i++) {
      huge.append(i % 7 == 0 ? ' ' : 'q');
    }
    drain(open(small(), Arrays.asList(text("SECRETWORD lives here. " + sentences("a", 3)),
        throwing(new RuntimeException("page broke")), () -> new PageText("SECRETWORD " + huge, true),
        text("SECRETWORD \uD800 again."))));
    assertFalse(warnings.isEmpty());
    assertTrue(warnings.stream().noneMatch(w -> w.contains("SECRETWORD") || w.contains("qqqq")), String.join("\n", warnings));
  }

  @Test
  public void truncatedPrefixesThatNormalizeEmptyProduceDurableFailures() {
    String[] prefixes = {"", " \t\n", "\u2060\uFEFF\u00AD"};
    for (String prefix : prefixes) {
      PdfChunkIterator iterator = open(small(), Collections.singletonList(() -> new PageText(prefix, true)));
      List<PdfChunk> chunks = drain(iterator);
      assertEquals(1, chunks.size());
      assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
      assertTrue(chunks.get(0).statusDetail().contains("PAGES_LOST failed=0 truncated=1"));
      assertEquals(1, iterator.stats().pagesTruncated());
      assertEquals(1, iterator.stats().pagesEmpty());
      assertEquals(0, iterator.stats().chunksEmitted());
    }
  }

  @Test
  public void emptyNormalizedTruncationCarriesLossOnOtherReadableText() {
    List<PdfChunk> chunks = drain(open(small(), Arrays.asList(() -> new PageText(" \n", true), text("Readable text."))));
    assertEquals(PdfChunk.Status.OK, chunks.get(chunks.size() - 1).status());
    assertEquals("Readable text.", chunks.get(0).text());
    assertTrue(chunks.get(chunks.size() - 1).statusDetail().contains("PAGES_LOST failed=0 truncated=1"));
  }

  @Test
  public void hugePageTruncatedCountsAndWarns() {
    List<PdfChunk> chunks = drain(open(small(), Collections.singletonList(() -> new PageText(sentences("a", 3), true))));
    assertTrue(chunks.get(chunks.size() - 1).statusDetail().contains("truncated=1"));
    assertTrue(warnings.stream().anyMatch(w -> w.contains("code=PAGE_TRUNCATED")));
  }

  @Test
  void statsCountPages() {
    PdfChunkIterator iterator = open(small(), Arrays.asList(text(sentences("a", 4)), text(""),
        throwing(new PageFailure(PdfFailureCode.DECODE_LIMIT, "oversized input")), throwing(new PageFailure(PdfFailureCode.PAGE_GLYPH_LIMIT, "g")),
        throwing(new PageFailure(PdfFailureCode.GRAPHICS_DEPTH_LIMIT, "q")), text(sentences("b", 4))));
    List<PdfChunk> chunks = drain(iterator);
    PdfChunkerStats stats = iterator.stats();
    assertEquals(6, stats.pagesSeen());
    assertEquals(1, stats.pagesEmpty());
    assertEquals(1, stats.decodeLimited());
    assertEquals(1, stats.glyphLimited());
    assertEquals(1, stats.graphicsLimited());
    assertEquals(0, stats.pagesFailed());
    assertEquals(chunks.size(), stats.chunksEmitted());
    assertTrue(stats.elapsedMillis() >= 0);
    assertEquals("PAGES_LOST failed=0 truncated=0 glyph_limited=1 decode_limited=1 graphics_limited=1 page_limit=false "
        + "file_error=none", chunks.get(chunks.size() - 1).statusDetail());
  }
}
