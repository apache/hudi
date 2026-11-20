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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** An open in progress keeps ownership until it can close a late result after task completion. */
class TestPreprocessingIteratorLifecycle {
  @Test
  void closeDuringOpenClosesTheLateChunkIterator() throws Exception {
    try (Harness h = new Harness(false)) {
      Future<Boolean> result = h.consumer.submit(h.iterator::hasNext);
      assertTrue(h.opened.await(5, TimeUnit.SECONDS));
      h.iterator.close();
      h.release.countDown();
      assertFalse(result.get(5, TimeUnit.SECONDS));
      assertEquals(1, h.chunkCloses.get(), "a late open cannot install an unowned chunk iterator");
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void registryCloseWaitsForTheOpenOwnerWithoutBlockingClose() throws Exception {
    try (Harness h = new Harness(false)) {
      Future<Boolean> result = h.consumer.submit(h.iterator::hasNext);
      assertTrue(h.opened.await(5, TimeUnit.SECONDS));
      h.iterator.close();
      assertEquals(0, h.registryCloses.get(), "an in-progress preprocessor open still owns the registry");
      h.release.countDown();
      assertFalse(result.get(5, TimeUnit.SECONDS));
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void sourceRowReturningAfterCloseDoesNotResolveAPreprocessor() throws Exception {
    try (Harness h = new Harness(true)) {
      Future<Boolean> result = h.consumer.submit(h.iterator::hasNext);
      assertTrue(h.rowTaken.await(5, TimeUnit.SECONDS));
      h.iterator.close();
      h.release.countDown();
      assertFalse(result.get(5, TimeUnit.SECONDS));
      assertEquals(0, h.resolutions.get(), "close prevents a new registry lookup and open");
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void normalExhaustionClosesChunksAndRegistryExactlyOnce() throws Exception {
    try (Harness h = new Harness(false)) {
      h.release.countDown();
      assertFalse(h.iterator.hasNext());
      h.iterator.close();
      assertEquals(1, h.chunkCloses.get());
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void registryCloseWaitsForAnAlreadyClosingChunkIterator() throws Exception {
    try (Harness h = new Harness(false, true)) {
      h.release.countDown();
      Future<Boolean> result = h.consumer.submit(h.iterator::hasNext);
      assertTrue(h.chunkClosing.await(5, TimeUnit.SECONDS));
      h.iterator.close();
      assertEquals(0, h.registryCloses.get(), "a chunk close retains the registry until its own cleanup finishes");
      h.releaseChunkClose.countDown();
      assertFalse(result.get(5, TimeUnit.SECONDS));
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void registryLookupErrorClosesOwnedResourcesBeforeItEscapes() throws Exception {
    try (Harness h = new Harness(false)) {
      LinkageError failure = new LinkageError("processor initialization failed");
      when(h.registry.forExtension(anyString())).thenThrow(failure);
      assertSame(failure, assertThrows(LinkageError.class, h.iterator::hasNext));
      assertEquals(1, h.registryCloses.get(), "an Error ends the standalone iterator without requiring a caller close");
    }
  }

  private static void await(CountDownLatch latch) throws IOException {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new IOException("test open was not released");
      }
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new IOException(failure);
    }
  }

  private static final class Harness implements AutoCloseable {
    private final CountDownLatch opened = new CountDownLatch(1);
    private final CountDownLatch rowTaken = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final CountDownLatch chunkClosing = new CountDownLatch(1);
    private final CountDownLatch releaseChunkClose = new CountDownLatch(1);
    private final PreprocessorRegistry registry = mock(PreprocessorRegistry.class);
    private final AtomicInteger chunkCloses = new AtomicInteger();
    private final AtomicInteger registryCloses = new AtomicInteger();
    private final AtomicInteger resolutions = new AtomicInteger();
    private final ExecutorService consumer = Executors.newSingleThreadExecutor();
    private final PreprocessingIterator iterator;

    private Harness(boolean blockSource) throws Exception {
      this(blockSource, false);
    }

    @SuppressWarnings("unchecked")
    private Harness(boolean blockSource, boolean blockChunkClose) throws Exception {
      FilePreprocessor processor = mock(FilePreprocessor.class);
      ClosableIterator<Chunk> chunks = mock(ClosableIterator.class);
      when(processor.name()).thenReturn("tracked-v1");
      when(processor.sourceKind()).thenReturn(SourceKind.DOCUMENT);
      when(registry.forExtension(anyString())).thenAnswer(call -> {
        resolutions.incrementAndGet();
        return Option.of(processor);
      });
      doAnswer(call -> {
        registryCloses.incrementAndGet();
        return null;
      }).when(registry).close();
      doAnswer(call -> {
        chunkClosing.countDown();
        if (blockChunkClose) {
          await(releaseChunkClose);
        }
        chunkCloses.incrementAndGet();
        return null;
      }).when(chunks).close();
      when(processor.open(any(), any(), any(), any())).thenAnswer(call -> {
        opened.countDown();
        await(release);
        return chunks;
      });
      Row row = mock(Row.class);
      when(row.getAs(BlobColumns.SOURCE_PATH_SHA)).thenReturn("sha");
      when(row.getAs(BlobColumns.SOURCE_FILE_EXTENSION)).thenReturn("txt");
      when(row.getAs(BlobColumns.SOURCE_FILE_SIZE)).thenReturn(1L);
      when(row.getAs(BlobColumns.SOURCE_MODIFICATION_TIME)).thenReturn(1L);
      when(row.getAs(BlobColumns.SOURCE_FILE_PATH)).thenReturn("file:///source.txt");
      when(row.getAs(BlobColumns.SOURCE_FILE_NAME)).thenReturn("source.txt");
      when(row.getAs(BlobColumns.BLOB)).thenReturn(mock(Row.class));
      Iterator<Row> rows = new Iterator<Row>() {
        private boolean taken;

        @Override
        public boolean hasNext() {
          return !taken;
        }

        @Override
        public Row next() {
          taken = true;
          rowTaken.countDown();
          if (blockSource) {
            try {
              await(release);
            } catch (IOException failure) {
              throw new UncheckedIOException(failure);
            }
          }
          return row;
        }
      };
      TypedProperties props = new TypedProperties();
      props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
      props.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), "0");
      iterator = PreprocessingIterator.builder(rows, EmbeddingTableConfig.from(props), "001")
          .bytesSource(mock(BlobBytesSource.class)).registry(registry).tokenizer(new WordCountTokenizer())
          .counters(PrepCounters.local()).build();
    }

    @Override
    public void close() throws Exception {
      release.countDown();
      releaseChunkClose.countDown();
      iterator.close();
      consumer.shutdown();
      assertTrue(consumer.awaitTermination(5, TimeUnit.SECONDS));
    }
  }
}
