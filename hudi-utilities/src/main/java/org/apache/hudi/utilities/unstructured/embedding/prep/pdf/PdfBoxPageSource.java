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

import org.apache.hudi.utilities.unstructured.embedding.prep.PrepFailures;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.apache.pdfbox.Loader;
import org.apache.pdfbox.io.IOUtils;
import org.apache.pdfbox.io.MemoryUsageSetting;
import org.apache.pdfbox.io.RandomAccessRead;
import org.apache.pdfbox.io.RandomAccessReadBufferedFile;
import org.apache.pdfbox.io.RandomAccessStreamCache.StreamCacheCreateFunction;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.spark.SparkEnv;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * The in-JVM PDFBox 3.0.8 page source, with the best-effort guard.
 * <ul>
 *   <li>a declared size above {@code max.input.bytes} (20 GiB unless the job sets it) fails {@code INPUT_LIMIT} without reading; otherwise the
 *       stream is copied through one 64 KiB buffer into a per-file scratch dir under the staging dir (the first Spark local
 *       dir inside a task, else {@code java.io.tmpdir}), aborting with {@code INPUT_LIMIT} past the cap;</li>
 *   <li>loaded from the staged FILE with PDFBox's scratch on disk, capped at {@code max.input.bytes} when the job sets it (else unrestricted)
 *       ({@code ENCRYPTED} for a non-empty user password, else {@code LOAD_ERROR}); PDFBox is never handed an
 *       InputStream or a byte array, which it would copy into heap;</li>
 *   <li>pages in document order, each through the {@link StreamDecodeGuard} and a fresh
 *       {@link GuardedPageStripper} writing into a {@link BoundedPageWriter};</li>
 *   <li>{@link #close()} closes the document and deletes the staged file and scratch dir.</li>
 * </ul>
 * A scratch dir or staged file that cannot be created or written is the executor's local disk, not the file
 * ({@link PrepPlatformException}, class P), and so is PDFBox's own scratch cache failing while a file is loaded or a page is
 * extracted ({@link PdfBoxScratchCache}): the failure is thrown after the load and after every page even where PDFBox caught
 * it on its way out; a source that cannot be read is the file's {@code SOURCE_IO}, unless the failure
 * fails the round (a {@code SourceReadException} of the framework's blob stream), which propagates.
 */
public final class PdfBoxPageSource implements PageSource {

  /**
   * The logic of extracting page text with PDFBox (loading, the guarded stripper, the budgets on PDFBox's tokens), recorded in
   * the PDF preprocessor's versioned name and so pinned on the table through the pdf extension's mapping
   * ({@code hoodie.table.embedding.prep.extension.pdf}) instead of the PDFBox jar version. Bump
   * only when a dependency upgrade or code change alters prep output; the golden test below proves it:
   * {@code TestPdfFilePreprocessor#goldenRecordsPinned} and {@code TestTextPackingPins} pin the records of fixed PDFs.
   */
  public static final String LOGIC_VERSION = "pdf-text-extraction-v1";

  private static final int COPY_BUFFER = 64 * 1024;

  /** Builds the stream cache PDFBox keeps one file's scratch data in. */
  interface ScratchCacheFactory {
    /**
     * @param scratchDir the file's own scratch dir, deleted with everything in it on {@link #close()}
     * @param maxBytes   the most scratch the file may use: {@code max.input.bytes}, or -1 (unrestricted) when it is unset
     */
    StreamCacheCreateFunction create(File scratchDir, long maxBytes);
  }

  /**
   * The default: PDFBox's temp-file cache in the file's scratch dir (capped at {@code max.input.bytes} when set), so a file is loaded from its
   * staged copy with its scratch on disk, never on the heap.
   */
  static final ScratchCacheFactory PDFBOX_SCRATCH_FILE =
      (scratchDir, maxBytes) -> MemoryUsageSetting.setupTempFileOnly(maxBytes).setTempDir(scratchDir).streamCache;

  private final InputStreamSupplier source;
  private final PdfFileMeta meta;
  private final PdfChunkerConfig config;
  private final File stagingRoot;
  private final boolean guardEnabled;
  private final ScratchCacheFactory scratchCacheFactory;

  private Path scratchDir;
  private long stagedBytes;
  private PDDocument document;
  private Iterator<PDPage> pages;
  private StreamDecodeGuard guard;
  /** 1-based number of the page {@link #next()} returned last. */
  private int pageNumber;
  private PdfBoxScratchCache scratchCache;

  public PdfBoxPageSource(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config) {
    this(source, meta, config, null, true);
  }

  /**
   * @param stagingRoot  the staging dir, or null for the Spark local dir / {@code java.io.tmpdir}
   * @param guardEnabled false only in tests that need the unguarded extraction
   */
  PdfBoxPageSource(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config, File stagingRoot,
                   boolean guardEnabled) {
    this(source, meta, config, stagingRoot, guardEnabled, PDFBOX_SCRATCH_FILE);
  }

  /** @param scratchCacheFactory PDFBox's scratch cache; tests inject a failing one */
  PdfBoxPageSource(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config, File stagingRoot,
                   boolean guardEnabled, ScratchCacheFactory scratchCacheFactory) {
    this.source = source;
    this.meta = meta;
    this.config = config;
    this.stagingRoot = stagingRoot;
    this.guardEnabled = guardEnabled;
    this.scratchCacheFactory = scratchCacheFactory;
  }

  @Override
  public void open() throws IOException {
    long max = config.maxInputBytes();
    if (meta.fileSize() > max) {
      throw new PdfFileFailure(PdfFailureCode.INPUT_LIMIT, "declared size " + meta.fileSize() + " bytes exceeds "
          + max);
    }
    File root = stagingRoot != null ? stagingRoot : defaultStagingRoot();
    try {
      scratchDir = Files.createTempDirectory(root.toPath(), "hudi-pdf-");
    } catch (IOException | RuntimeException e) {
      // the executor's local scratch disk, not the file: every PDF on it would fail the same way (a platform failure)
      throw new PrepPlatformException("cannot create a PDF staging dir under " + root, e);
    }
    File staged = scratchDir.resolve("input.pdf").toFile();
    stage(staged, max);
    // PDFBox's scratch storage is held to the input cap when the job sets one; -1 is PDFBox's "unrestricted"
    scratchCache = new PdfBoxScratchCache(scratchCacheFactory.create(scratchDir.toFile(), config.inputBytesConfigured() ? max : -1));
    RandomAccessRead input;
    try {
      input = new RandomAccessReadBufferedFile(staged);
    } catch (IOException | RuntimeException e) {
      throw new PrepPlatformException("cannot read the staged PDF " + staged, e);
    }
    try {
      document = Loader.loadPDF(input, "", null, null, scratchCache);
    } catch (InvalidPasswordException e) {
      throwScratchFailure();
      throw new PdfFileFailure(PdfFailureCode.ENCRYPTED, "encrypted with a user password", e);
    } catch (IOException | RuntimeException | StackOverflowError e) {
      throwScratchFailure();
      throw new PdfFileFailure(PdfFailureCode.LOAD_ERROR, "PDFBox could not load the file", e);
    } finally {
      if (document == null) {
        // a loaded document owns the input and closes it; a failed load leaves it open, whatever it threw
        IOUtils.closeQuietly(input);
      }
    }
    try {
      pages = document.getPages().iterator();
      pageNumber = 0;
    } catch (RuntimeException | StackOverflowError e) {
      throwScratchFailure();
      throw new PdfFileFailure(PdfFailureCode.LOAD_ERROR, "the page tree cannot be read", e);
    }
    throwScratchFailure();
    guard = new StreamDecodeGuard(config.maxDecodedStreamBytes(), config.maxPageFontBytes(), scratchDir.toFile());
  }

  /**
   * The staging copy: at most {@code max} bytes, through one buffer. A source that cannot be read ({@code IOException}: gone or
   * short) is the file's {@code SOURCE_IO}; a failure that fails the round, the framework's unchecked {@code SourceReadException}
   * for a throttled, reset or denied read among them, propagates as it is (a transient source failure); a failure to write the local staging file
   * is the platform's ({@link PrepPlatformException}).
   */
  private void stage(File staged, long max) {
    InputStream in;
    try {
      in = source.open();
    } catch (IOException e) {
      throw new PdfFileFailure(PdfFailureCode.SOURCE_IO, "opening the source failed", e);
    } catch (RuntimeException e) {
      RuntimeException round = PrepFailures.roundFailure(e);
      throw round != null ? round : new PdfFileFailure(PdfFailureCode.SOURCE_IO, "opening the source failed", e);
    }
    try (InputStream input = in; OutputStream out = ScratchFiles.newOutputStream(staged.toPath())) {
      byte[] buffer = new byte[COPY_BUFFER];
      int n;
      while ((n = input.read(buffer)) >= 0) {
        stagedBytes += n;
        if (stagedBytes > max) {
          throw new PdfFileFailure(PdfFailureCode.INPUT_LIMIT, "more than " + max + " bytes arrived (declared "
              + meta.fileSize() + ")");
        }
        out.write(buffer, 0, n);
      }
    } catch (IOException e) {
      throw new PdfFileFailure(PdfFailureCode.SOURCE_IO, "copying the source failed", e);
    }
  }

  /** The first Spark local dir inside a task (SPARK_LOCAL_DIRS, then spark.local.dir), else java.io.tmpdir. */
  static File defaultStagingRoot() {
    String dirs = null;
    if (SparkEnv.get() != null) {
      dirs = System.getenv("SPARK_LOCAL_DIRS");
      if (dirs == null || dirs.trim().isEmpty()) {
        dirs = SparkEnv.get().conf().get("spark.local.dir", null);
      }
    }
    String first = dirs == null ? null : dirs.split(",")[0].trim();
    File root = first == null || first.isEmpty() ? new File(System.getProperty("java.io.tmpdir")) : new File(first);
    if (!root.isDirectory() && !root.mkdirs() && !root.isDirectory()) {
      return new File(System.getProperty("java.io.tmpdir"));
    }
    return root;
  }

  @Override
  public boolean hasMorePages() {
    return pages != null && pages.hasNext();
  }

  /** PDFBox's scratch cache failed on the local disk, whatever PDFBox made of it: a platform failure, so the round fails. */
  private void throwScratchFailure() {
    PrepPlatformException failure = scratchCache.failure();
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public PageText next() throws IOException {
    PDPage page = pages.next();                        // moves past the page first, so a failure skips only this page
    pageNumber++;
    PageText text;
    try {
      text = extract(page, pageNumber);
    } catch (IOException | RuntimeException | StackOverflowError e) {
      throwScratchFailure();
      throw e;
    }
    throwScratchFailure();
    return text;
  }

  private PageText extract(PDPage page, int pageNumber) throws IOException {
    int skippedContentStreams = guardEnabled ? guard.checkPage(page, pageNumber) : 0;
    // the glyph-text cap is max.page.glyphs, not max.page.chars: text between the two is truncated,
    // text beyond it fails the page
    GuardedPageStripper stripper = new GuardedPageStripper(config.maxPageGlyphs(), config.maxPageGlyphs(),
        config.maxGraphicsDepth(), guardEnabled ? guard : null);
    BoundedPageWriter writer = new BoundedPageWriter(config.maxPageChars());
    if (guardEnabled) {
      guard.beginFontMaterializations();
    }
    try {
      stripper.extract(document, page, writer);
    } finally {
      if (guardEnabled) {
        guard.endFontMaterializations();
      }
    }
    return new PageText(writer.text(), writer.truncated(), skippedContentStreams + stripper.skippedContentStreams());
  }

  @Override
  public long stagedBytes() {
    return stagedBytes;
  }

  /** The per-file scratch dir (staged file, PDFBox scratch, guard temp files); null before {@link #open()}. */
  Path scratchDir() {
    return scratchDir;
  }

  @Override
  public void close() {
    PDDocument toClose = document;
    document = null;
    pages = null;
    if (toClose != null) {
      try {
        toClose.close();
      } catch (IOException | RuntimeException e) {
        // the scratch dir is deleted below either way
      }
    }
    Path dir = scratchDir;
    scratchDir = null;
    if (dir != null) {
      try (Stream<Path> paths = Files.walk(dir)) {
        paths.sorted(Comparator.reverseOrder()).forEach(PdfBoxPageSource::deleteQuietly);
      } catch (IOException | RuntimeException e) {
        dir.toFile().deleteOnExit();
      }
    }
  }

  /** Deletes a scratch file or emptied directory; one that cannot be deleted is left, as the walk goes on with the rest. */
  private static void deleteQuietly(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      // left behind: a scratch file the document no longer reads
    }
  }
}
