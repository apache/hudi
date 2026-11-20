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

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.ChunkPacker;
import org.apache.hudi.utilities.unstructured.embedding.text.PackedChunk;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenBudget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Locale;
import java.util.NoSuchElementException;

/**
 * The chunk iterator of ONE file. It pulls pages from a {@link PageSource} only as
 * far as the pull rule needs, normalizes them, lets the {@link ChunkPacker} cut chunks, and HOLDS BACK one cut
 * chunk so the last OK chunk can carry the loss summary. Everything runs on the caller's thread inside
 * {@code hasNext()}/{@code next()}; nothing runs between calls and no thread is started.
 *
 * <p>Isolation: a page failure skips that page with a WARN; a file-level failure ends the file — with a FAILED
 * marker when no OK chunk was cut yet, else by flushing the pending text (unless the tokenizer failed) and signalling the
 * loss on the last chunk. {@link VirtualMachineError}s other than {@link StackOverflowError},
 * {@link LinkageError}s and interruption propagate. At most {@value #MAX_PAGE_WARNINGS} page-level WARN lines
 * per file, then one summary WARN.
 *
 * <p>A failure that fails the prep round ({@link PrepFailures#roundFailure}) is never isolated, at the file or at the
 * page: a storage failure of the source a later run can still read, a platform failure (a local scratch disk that cannot be
 * written), a configuration error, or a RETRYABLE tokenizer failure after the tokenizer's own retries. The iterator closes its
 * page source and throws it as it was met, whatever wrapped it, so neither a FAILED marker (a quarantine entry) nor a file
 * truncated behind a loss summary is committed for it. Everything else a PDFBox call throws on an input is the input's own failure.
 */
public final class PdfChunkIterator implements ClosableIterator<PdfChunk> {
  private static final Logger LOG = LoggerFactory.getLogger(PdfChunkIterator.class);

  private static final String PATH_EQUALS = " path=";

  /** Receives the iterator's WARN lines; the production sink is the chunker's logger. */
  @FunctionalInterface
  interface WarnSink {
    void warn(String line);
  }

  static final int MAX_PAGE_WARNINGS = 10;

  private final InputStreamSupplier source;
  private final PdfFileMeta meta;
  private final PdfChunkerConfig config;
  private final PageSourceFactory factory;
  private final WarnSink warnings;
  private final TextNormalizer normalizer;
  private final ChunkPacker packer;
  private final String tokenizerId;
  private final PdfChunkerStats stats = new PdfChunkerStats();

  private PageSource pageSource;
  private boolean started;
  private boolean sourceExhausted;
  private boolean pageLimitHit;
  private boolean flushing;
  private boolean ended;
  private boolean closed;
  private PdfChunk held;
  private PdfChunk ready;
  private long ordinal;
  private PdfFailureCode fileError;
  private String fileErrorDetail;
  private int pageWarnings;
  private long startNanos;

  PdfChunkIterator(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config, PageSourceFactory factory,
                   WarnSink warnings) {
    this.source = source;
    this.meta = meta;
    this.config = config;
    this.factory = factory;
    this.warnings = warnings;
    this.normalizer = new TextNormalizer(config.maxPageChars());
    this.packer = new ChunkPacker(new TokenBudget(config.tokenizer(), config.maxTokens(), config.maxWords(),
        config.windowChars(), PdfFileFailure.PACK_FAILURES), config.overlapTokens(), config.locale());
    this.tokenizerId = config.tokenizer().id();
  }

  /** Per-file counters; final once {@link #hasNext()} returned false. */
  public PdfChunkerStats stats() {
    return stats;
  }

  @Override
  public boolean hasNext() {
    while (ready == null && !ended && !closed) {
      step();
    }
    return ready != null;
  }

  @Override
  public PdfChunk next() {
    if (!hasNext()) {
      throw new NoSuchElementException("no more chunks for source_path_sha " + meta.sourcePathSha());
    }
    PdfChunk chunk = ready;
    ready = null;
    return chunk;
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      ready = null;
      held = null;
      closeSource();
      LOG.debug("PDF source_path_sha={} chunked: {}", meta.sourcePathSha(), stats);
    }
  }

  // ------------------------------------------------------------------ the pull loop

  private void step() {
    checkInterrupted();
    try {
      if (!started) {
        start();
      } else if (!sourceExhausted && packer.needsMoreText()) {
        pullPage();
      } else {
        PackedChunk cut = packer.cut(sourceExhausted);
        if (cut == null) {
          finish();
        } else {
          accept(cut);
        }
      }
    } catch (PdfChunkerInterruptedException e) {
      throw e;
    } catch (PdfFileFailure e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      failFile(e.code(), e);
    } catch (Exception | StackOverflowError e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      failFile(PdfFailureCode.INTERNAL, e);
    }
  }

  private void start() throws IOException {
    started = true;
    startNanos = System.nanoTime();
    pageSource = factory.create(source, meta, config);
    pageSource.open();
    stats.stagedBytes = pageSource.stagedBytes();
  }

  private void pullPage() {
    if (!pageSource.hasMorePages()) {
      sourceExhausted = true;
      return;
    }
    if (stats.pagesSeen >= config.maxPages()) {
      pageLimitHit = true;
      sourceExhausted = true;
      pageWarning(PdfFailureCode.PAGE_LIMIT, stats.pagesSeen + 1, "stopped after " + config.maxPages() + " pages", null);
      return;
    }
    int page = (int) ++stats.pagesSeen;
    PageText raw;
    try {
      raw = pageSource.next();
    } catch (PdfFileFailure | PdfChunkerInterruptedException e) {
      throw e;
    } catch (PageFailure e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      pageFailed(page, e.code(), e);
      return;
    } catch (Exception | StackOverflowError e) {
      checkInterrupted(e);
      rethrowRoundFailure(e);
      pageFailed(page, PdfFailureCode.PAGE_ERROR, e);
      return;
    }
    if (raw.skippedContentStreams() > 0) {
      stats.streamsSkipped += raw.skippedContentStreams();
      pageWarning(PdfFailureCode.DECODE_ERROR, page, raw.skippedContentStreams() + " content streams skipped", null);
    }
    TextNormalizer.Result normalized = normalizer.normalize(raw.text());
    if (raw.truncated() || normalized.truncated()) {
      stats.pagesTruncated++;
      pageWarning(PdfFailureCode.PAGE_TRUNCATED, page, "text beyond " + config.maxPageChars() + " chars dropped", null);
    }
    if (normalized.unpairedSurrogates() > 0) {
      pageWarning(null, page, normalized.unpairedSurrogates() + " unpaired surrogates replaced by U+FFFD", null);
    }
    if (normalized.text().isEmpty()) {
      stats.pagesEmpty++;
    } else {
      packer.appendPage(normalized.text(), page);
    }
  }

  private void pageFailed(int page, PdfFailureCode code, Throwable cause) {
    switch (code) {
      case PAGE_GLYPH_LIMIT:
        stats.glyphLimited++;
        break;
      case DECODE_LIMIT:
        stats.decodeLimited++;
        break;
      case DECODE_ERROR:
        stats.decodeFailed++;
        break;
      case GRAPHICS_DEPTH_LIMIT:
        stats.graphicsLimited++;
        break;
      default:
        stats.pagesFailed++;
        code = PdfFailureCode.PAGE_ERROR;
    }
    pageWarning(code, page, null, cause);
  }

  private void accept(PackedChunk cut) {
    stats.chunksEmitted++;
    stats.forcedSplits = packer.forcedSplits();
    PdfChunk.Measures measures = new PdfChunk.Measures(cut.tokenCount(), tokenizerId, cut.wordCount(), cut.overlapChars(), cut.firstMark(),
        cut.lastMark());
    PdfChunk chunk = new PdfChunk(meta.sourcePathSha(), ordinal++, cut.text(), measures, PdfChunk.Status.OK, null);
    if (held != null) {
      ready = held;                                    // a chunk is returned once the next one is cut
    }
    held = chunk;
  }

  /** A file-level failure. */
  private void failFile(PdfFailureCode code, Throwable cause) {
    if (fileError == null) {
      fileError = code;
      fileErrorDetail = code.format(cause);
    }
    if (held == null) {
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.FAILED, fileErrorDetail));
    } else if (code == PdfFailureCode.TOKENIZER_ERROR || flushing) {
      end(held.withStatusDetail(lossSummary()));
    } else {
      flushing = true;                                 // cut the text of the pages already extracted, then end
      sourceExhausted = true;
    }
  }

  /** The end of the input: the last chunk with its loss summary, or the file's single marker row. */
  private void finish() {
    if (held != null) {
      end(lost() ? held.withStatusDetail(lossSummary()) : held);
      return;
    }
    boolean otherPageFailures = stats.pagesFailed + stats.pagesTruncated + stats.glyphLimited + stats.decodeLimited + stats.graphicsLimited > 0;
    boolean decodeLoss = stats.decodeFailed + stats.streamsSkipped > 0;
    if (pageLimitHit) {
      // the pages past max.pages were never read, so the file is not known to have no text
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.FAILED, PdfFailureCode.PAGE_LIMIT.name()
          + ": no text in the first " + config.maxPages() + " pages, " + lossSummary()));
    } else if (otherPageFailures || decodeLoss) {
      PdfFailureCode code = decodeLoss && !otherPageFailures ? PdfFailureCode.DECODE_ERROR : PdfFailureCode.ALL_PAGES_FAILED;
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.FAILED, code.name()
          + ": no page yielded text, " + lossSummary()));
    } else {
      end(PdfChunk.marker(meta.sourcePathSha(), tokenizerId, PdfChunk.Status.NO_TEXT, null));
    }
  }

  private void end(PdfChunk last) {
    ready = last;
    held = null;
    ended = true;
    stats.forcedSplits = packer.forcedSplits();
    stats.elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
    // the summary also follows suppressed page WARNs on an OK, loss-free file
    if (lost() || last.status() != PdfChunk.Status.OK || pageWarnings > MAX_PAGE_WARNINGS) {
      String pages = "pages=" + stats.pagesSeen + (last.status() == PdfChunk.Status.OK ? "" : " (no text)");
      warnings.warn("PDF-FILE source_path_sha=" + meta.sourcePathSha() + PATH_EQUALS + meta.sourcePath() + " status=" + last.status() + " "
          + (lost() ? lossSummary() : pages)
          + " pageWarnings=" + pageWarnings
          + (fileErrorDetail == null ? "" : " error=" + fileErrorDetail));
    }
    closeSource();
  }

  private boolean lost() {
    return stats.pagesFailed + stats.pagesTruncated + stats.glyphLimited + stats.decodeLimited + stats.decodeFailed
        + stats.streamsSkipped + stats.graphicsLimited > 0
        || pageLimitHit || fileError != null;
  }

  /** The durable loss signal the last OK chunk carries. */
  private String lossSummary() {
    return String.format(Locale.ROOT, "PAGES_LOST failed=%d truncated=%d glyph_limited=%d decode_limited=%d "
            + "graphics_limited=%d page_limit=%b file_error=%s", stats.pagesFailed, stats.pagesTruncated, stats.glyphLimited,
        stats.decodeLimited, stats.graphicsLimited, pageLimitHit, fileError == null ? "none" : fileError.name())
        + (stats.decodeFailed > 0 ? " decode_failed=" + stats.decodeFailed : "")
        + (stats.streamsSkipped > 0 ? " streams_skipped=" + stats.streamsSkipped : "");
  }

  private void pageWarning(PdfFailureCode code, long page, String detail, Throwable cause) {
    pageWarnings++;
    if (pageWarnings <= MAX_PAGE_WARNINGS) {
      warnings.warn("PDF-PAGE source_path_sha=" + meta.sourcePathSha() + PATH_EQUALS + meta.sourcePath() + " page=" + page
          + (code == null ? "" : " code=" + code) + (detail == null ? "" : " " + detail)
          + (cause == null ? "" : " " + cause.getClass().getName() + ": " + cause.getMessage()));
    }
  }

  // ------------------------------------------------------------------ interruption and cleanup

  private void checkInterrupted() {
    checkInterrupted(null);
  }

  /**
   * Throws when the thread or the Spark task is interrupted, or when {@code failure} was caused by an interrupt (an
   * {@link java.io.InterruptedIOException} or {@link java.nio.channels.ClosedByInterruptException} whose library cleared
   * the flag).
   */
  private void checkInterrupted(Throwable failure) {
    if (PrepFailures.isInterrupted(failure)) {
      closeSource();
      throw new PdfChunkerInterruptedException("interrupted while chunking source_path_sha " + meta.sourcePathSha());
    }
  }

  /**
   * Ends the file and throws the failure of {@code failure}'s cause or suppressed chain that fails the round, as it was met;
   * returns for a failure of the file's own input.
   */
  private void rethrowRoundFailure(Throwable failure) {
    RuntimeException round = PrepFailures.roundFailure(failure);
    if (round != null) {
      ended = true;
      ready = null;
      held = null;
      closeSource();
      throw round;
    }
  }

  /** Closes the page source; a failing close never changes the records already cut. */
  private void closeSource() {
    if (pageSource != null) {
      PageSource toClose = pageSource;
      pageSource = null;
      try {
        toClose.close();
      } catch (RuntimeException e) {
        warnings.warn("PDF-FILE source_path_sha=" + meta.sourcePathSha() + PATH_EQUALS + meta.sourcePath() + " closing the page source failed: "
            + e.getClass().getName() + ": " + e.getMessage());
      }
    }
  }
}
