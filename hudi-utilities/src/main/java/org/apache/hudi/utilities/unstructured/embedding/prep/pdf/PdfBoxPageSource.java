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

/** Stages a PDF to disk and extracts its pages through PDFBox with disk-backed scratch storage. */
public final class PdfBoxPageSource implements PageSource {

  public static final String LOGIC_VERSION = "pdf-text-extraction-v1";

  private static final int COPY_BUFFER = 64 * 1024;

  private final InputStreamSupplier source;

  private Path scratchDir;
  private long stagedBytes;
  private PDDocument document;
  private Iterator<PDPage> pages;
  private PdfBoxScratchCache scratchCache;

  public PdfBoxPageSource(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config) {
    this.source = source;
  }

  @Override
  public void open() throws IOException {
    File root = defaultStagingRoot();
    try {
      scratchDir = Files.createTempDirectory(root.toPath(), "hudi-pdf-");
    } catch (IOException | RuntimeException e) {
      // the executor's local scratch disk, not the file: every PDF on it would fail the same way (a platform failure)
      throw new PrepPlatformException("cannot create a PDF staging dir under " + root, e);
    }
    File staged = scratchDir.resolve("input.pdf").toFile();
    stage(staged);
    // PDFBox's temp-file cache in the file's scratch dir, so a file is loaded from its staged copy with its scratch on disk,
    // never on the heap
    scratchCache = new PdfBoxScratchCache(MemoryUsageSetting.setupTempFileOnly().setTempDir(scratchDir.toFile()).streamCache);
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
    } catch (RuntimeException | StackOverflowError e) {
      throwScratchFailure();
      throw new PdfFileFailure(PdfFailureCode.LOAD_ERROR, "the page tree cannot be read", e);
    }
    throwScratchFailure();
  }

  /**
   * The staging copy, through one buffer. A source that cannot be read ({@code IOException}: gone or
   * short) is the file's {@code SOURCE_IO}; a failure that fails the round, the framework's unchecked {@code SourceReadException}
   * for a throttled, reset or denied read among them, propagates as it is (a transient source failure); a failure to write the local staging file
   * is the platform's ({@link PrepPlatformException}).
   */
  private void stage(File staged) {
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
    PageText text;
    try {
      text = extract(page);
    } catch (IOException | RuntimeException | StackOverflowError e) {
      throwScratchFailure();
      throw e;
    }
    throwScratchFailure();
    return text;
  }

  private PageText extract(PDPage page) throws IOException {
    GuardedPageStripper stripper = new GuardedPageStripper();
    BoundedPageWriter writer = new BoundedPageWriter();
    stripper.extract(document, page, writer);
    return new PageText(writer.text(), false, stripper.skippedContentStreams());
  }

  @Override
  public long stagedBytes() {
    return stagedBytes;
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
        paths.sorted(Comparator.reverseOrder()).forEach(path -> path.toFile().delete());
      } catch (IOException | RuntimeException e) {
        dir.toFile().deleteOnExit();
      }
    }
  }
}
