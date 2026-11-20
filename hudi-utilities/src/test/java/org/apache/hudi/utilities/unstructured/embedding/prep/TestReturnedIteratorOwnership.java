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
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** A returned iterator stays owned if the post-open source check rejects a decoder's swallowed read failure. */
class TestReturnedIteratorOwnership {
  @Test
  void successfulOpenClosesTheReturnedIteratorOnExhaustion() throws Exception {
    try (Harness h = new Harness(false, false)) {
      assertFalse(h.iterator.hasNext());
      assertEquals(1, h.chunkCloses.get());
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void sourceFailureClosesTheIteratorReturnedAfterASwallowedRead() throws Exception {
    try (Harness h = new Harness(true, false)) {
      assertThrows(SourceReadException.class, h.iterator::hasNext);
      assertEquals(1, h.chunkCloses.get());
      assertEquals(1, h.registryCloses.get());
    }
  }

  @Test
  void sourceFailureKeepsACleanupFailureSuppressed() throws Exception {
    try (Harness h = new Harness(true, true)) {
      SourceReadException failure = assertThrows(SourceReadException.class, h.iterator::hasNext);
      assertEquals(1, h.chunkCloses.get());
      assertEquals(1, failure.getSuppressed().length);
      assertSame(h.cleanupFailure, failure.getSuppressed()[0]);
    }
  }

  private static final class Harness implements AutoCloseable {
    private final AtomicInteger chunkCloses = new AtomicInteger();
    private final AtomicInteger registryCloses = new AtomicInteger();
    private final IllegalStateException cleanupFailure = new IllegalStateException("returned iterator cleanup failed");
    private final PreprocessingIterator iterator;

    @SuppressWarnings("unchecked")
    private Harness(boolean failsRead, boolean failsClose) throws Exception {
      BlobBytesSource source = mock(BlobBytesSource.class);
      if (failsRead) {
        when(source.open(any(Row.class), anyLong())).thenThrow(new IOException("source unavailable"));
      } else {
        when(source.open(any(Row.class), anyLong())).thenAnswer(call -> new ByteArrayInputStream(new byte[] {1}));
      }
      ClosableIterator<Chunk> chunks = mock(ClosableIterator.class);
      doAnswer(call -> {
        chunkCloses.incrementAndGet();
        if (failsClose) {
          throw cleanupFailure;
        }
        return null;
      }).when(chunks).close();
      FilePreprocessor processor = mock(FilePreprocessor.class);
      when(processor.name()).thenReturn("swallowed-read-v1");
      when(processor.sourceKind()).thenReturn(SourceKind.DOCUMENT);
      when(processor.open(any(), any(), any(), any())).thenAnswer(call -> {
        BlobBytesSupplier bytes = call.getArgument(0);
        try (InputStream input = bytes.open()) {
          input.read();
        } catch (IOException | RuntimeException ignored) {
          // A decoder may isolate its read failure; the framework must still retain and close its returned iterator.
        }
        return chunks;
      });
      PreprocessorRegistry registry = mock(PreprocessorRegistry.class);
      when(registry.forExtension(anyString())).thenReturn(Option.of(processor));
      doAnswer(call -> {
        registryCloses.incrementAndGet();
        return null;
      }).when(registry).close();
      Row row = mock(Row.class);
      when(row.getAs(BlobColumns.SOURCE_PATH_SHA)).thenReturn("source-version");
      when(row.getAs(BlobColumns.SOURCE_FILE_EXTENSION)).thenReturn("txt");
      when(row.getAs(BlobColumns.SOURCE_FILE_SIZE)).thenReturn(1L);
      when(row.getAs(BlobColumns.SOURCE_MODIFICATION_TIME)).thenReturn(1L);
      when(row.getAs(BlobColumns.SOURCE_FILE_PATH)).thenReturn("file:///source.txt");
      when(row.getAs(BlobColumns.SOURCE_FILE_NAME)).thenReturn("source.txt");
      when(row.getAs(BlobColumns.BLOB)).thenReturn(mock(Row.class));
      TypedProperties props = new TypedProperties();
      props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
      props.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), "0");
      iterator = new PreprocessingIterator(Collections.singletonList(row).iterator(), EmbeddingTableConfig.from(props), "001",
          source, registry, new WordCountTokenizer(), PrepCounters.local(), null);
    }

    @Override
    public void close() {
      iterator.close();
    }
  }
}
