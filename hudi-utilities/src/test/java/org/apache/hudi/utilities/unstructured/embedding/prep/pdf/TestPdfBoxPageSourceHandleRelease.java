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
import org.apache.hudi.utilities.unstructured.embedding.prep.SourceReadException;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Handle release of one PDF file through {@link PdfChunkIterator} over the real {@link PdfBoxPageSource}: the source stream the
 * page source opens, the staged copy and PDFBox's scratch under the per-file scratch dir, and the file descriptors on them are
 * all released on every way a file ends (exhausted, abandoned by the consumer and closed, a failed load, a size cap met mid-copy,
 * a round failure mid-copy, an interrupt, an {@link Error} escaping a page).
 */
class TestPdfBoxPageSourceHandleRelease {

  @TempDir
  Path staging;
  private final List<String> warnings = new ArrayList<>();
  private final AtomicInteger opened = new AtomicInteger();
  private final AtomicInteger closed = new AtomicInteger();

  @BeforeAll
  static void fontCache(@TempDir Path cache) {
    System.setProperty("pdfbox.fontcache", cache.toString());
  }

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void exhaustedFileReleasesEverything() throws IOException {
    byte[] pdf = PdfFixtures.prose(7L, 4, 30);
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = open(pdf, config(new TypedProperties()))) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
      // released at the end of the input, before the consumer's close
      assertReleased("after the last chunk");
    }
    assertTrue(chunks.stream().allMatch(c -> c.status() == PdfChunk.Status.OK), chunks.toString());
    assertReleased("after close");
  }

  @Test
  void fileAbandonedMidWayIsReleasedByClose() throws IOException {
    byte[] pdf = PdfFixtures.prose(11L, 12, 60);
    PdfChunkIterator iterator = open(pdf, config(new TypedProperties()));
    assertTrue(iterator.hasNext());
    iterator.next();
    assertFalse(stagingContents().isEmpty(), "the file is still open mid-way (the staged copy is held)");
    iterator.close();
    assertReleased("after close of a file abandoned mid-way");
  }

  @Test
  void failedLoadReleasesEverything() {
    byte[] notAPdf = "%PDF-1.7 broken".getBytes(StandardCharsets.US_ASCII);
    List<PdfChunk> chunks = drain(notAPdf, config(new TypedProperties()));
    assertEquals(1, chunks.size(), chunks.toString());
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("LOAD_ERROR"), chunks.get(0).statusDetail());
    assertReleased("after a failed load");
  }

  @Test
  void inputLimitMetMidCopyReleasesEverything() throws IOException {
    byte[] pdf = PdfFixtures.prose(3L, 3, 30);
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), "1024");
    // the declared size passes the cap, the bytes that arrive do not: the copy aborts mid-way
    PdfChunkIterator iterator = new PdfBoxChunker((s, m, c) -> new PdfBoxPageSource(s, m, c, staging.toFile(), true))
        .open(countingSource(pdf, -1), new PdfFileMeta("s", "s.pdf", 10), config(props), warnings::add);
    List<PdfChunk> chunks = new ArrayList<>();
    try {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    } finally {
      iterator.close();
    }
    assertEquals(PdfChunk.Status.FAILED, chunks.get(0).status());
    assertTrue(chunks.get(0).statusDetail().startsWith("INPUT_LIMIT"), chunks.get(0).statusDetail());
    assertReleased("after INPUT_LIMIT mid-copy");
  }

  @Test
  void roundFailureMidCopyReleasesEverythingBeforeItPropagates() throws IOException {
    byte[] pdf = PdfFixtures.prose(5L, 3, 30);
    PdfChunkIterator iterator = new PdfBoxChunker((s, m, c) -> new PdfBoxPageSource(s, m, c, staging.toFile(), true))
        .open(countingSource(pdf, 2), new PdfFileMeta("s", "s.pdf", pdf.length), config(new TypedProperties()), warnings::add);
    assertThrows(SourceReadException.class, iterator::hasNext);
    // released by the iterator itself, before the consumer gets to close it
    assertReleased("after a round failure mid-copy, before close");
    iterator.close();
  }

  @Test
  void interruptMidFileReleasesEverythingBeforeItPropagates() throws IOException {
    byte[] pdf = PdfFixtures.prose(13L, 12, 60);
    PdfChunkIterator iterator = open(pdf, config(new TypedProperties()));
    assertTrue(iterator.hasNext());
    iterator.next();
    assertFalse(stagingContents().isEmpty(), "the file is still open mid-way");
    Thread.currentThread().interrupt();
    try {
      assertThrows(PdfChunkerInterruptedException.class, iterator::hasNext);
    } finally {
      Thread.interrupted();
    }
    assertReleased("after an interrupt, before close");
    iterator.close();
  }

  @Test
  void errorEscapingAPageIsReleasedByTheConsumersClose() throws IOException {
    byte[] pdf = PdfFixtures.prose(17L, 4, 30);
    PdfChunkIterator iterator = new PdfBoxChunker((s, m, c) -> new FailingNextPageSource(new PdfBoxPageSource(s, m, c,
        staging.toFile(), true))).open(countingSource(pdf, -1), new PdfFileMeta("s", "s.pdf", pdf.length),
        config(new TypedProperties()), warnings::add);
    assertThrows(NoClassDefFoundError.class, iterator::hasNext);
    iterator.close();
    assertReleased("after an Error escaped a page and the consumer closed");
  }

  // ------------------------------------------------------------------ helpers

  private PdfChunkIterator open(byte[] pdf, PdfChunkerConfig config) {
    return new PdfBoxChunker((s, m, c) -> new PdfBoxPageSource(s, m, c, staging.toFile(), true))
        .open(countingSource(pdf, -1), new PdfFileMeta("s", "s.pdf", pdf.length), config, warnings::add);
  }

  private List<PdfChunk> drain(byte[] pdf, PdfChunkerConfig config) {
    List<PdfChunk> chunks = new ArrayList<>();
    try (PdfChunkIterator iterator = open(pdf, config)) {
      while (iterator.hasNext()) {
        chunks.add(iterator.next());
      }
    }
    return chunks;
  }

  /** Each open counted; each close counted once; a read numbered {@code failingRead} (from 1) throws a round failure. */
  private InputStreamSupplier countingSource(byte[] bytes, int failingRead) {
    return () -> {
      opened.incrementAndGet();
      return new FilterInputStream(new ByteArrayInputStream(bytes)) {
        private int reads;
        private boolean done;

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          if (++reads == failingRead) {
            throw new SourceReadException("throttled read", new IOException("503"));
          }
          return super.read(b, off, Math.min(len, 512));
        }

        @Override
        public void close() throws IOException {
          if (!done) {
            done = true;
            closed.incrementAndGet();
          }
          super.close();
        }
      };
    };
  }

  private void assertReleased(String when) {
    assertTrue(opened.get() > 0, when + ": the source was opened");
    assertEquals(opened.get(), closed.get(), when + ": unclosed source stream(s): opened=" + opened + " closed=" + closed);
    List<Path> left;
    List<String> fds;
    try {
      left = stagingContents();
      fds = openStagedFiles();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    assertTrue(left.isEmpty(), when + ": scratch dir / staged file left on disk: " + left);
    assertTrue(fds.isEmpty(), when + ": open file descriptors under the staging dir: " + fds);
  }

  private static PdfChunkerConfig config(TypedProperties props) {
    return PdfChunkerConfig.from(props, new WordCountTokenizer());
  }

  private List<Path> stagingContents() throws IOException {
    try (Stream<Path> paths = Files.walk(staging)) {
      return paths.filter(p -> !p.equals(staging)).collect(Collectors.toList());
    }
  }

  /** This JVM's open file descriptors under the staging dir (Linux /proc; none elsewhere). */
  private List<String> openStagedFiles() throws IOException {
    Path fds = Paths.get("/proc/self/fd");
    List<String> open = new ArrayList<>();
    if (!Files.isDirectory(fds)) {
      return open;
    }
    // the /proc link targets are canonical paths: compare against the staging dir's real path, not a symlinked one
    String root = staging.toRealPath().toString();
    try (Stream<Path> links = Files.list(fds)) {
      for (Path link : links.collect(Collectors.toList())) {
        try {
          String target = Files.readSymbolicLink(link).toString();
          if (target.startsWith(root)) {
            open.add(target);
          }
        } catch (IOException e) {
          // the descriptor closed while listing
        }
      }
    }
    return open;
  }

  /** Delegates, except that the first page throws an {@link Error} the iterator does not isolate. */
  private static final class FailingNextPageSource implements PageSource {
    private final PageSource delegate;

    FailingNextPageSource(PageSource delegate) {
      this.delegate = delegate;
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
    public PageText next() {
      throw new NoClassDefFoundError("a font class that does not link");
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
}
