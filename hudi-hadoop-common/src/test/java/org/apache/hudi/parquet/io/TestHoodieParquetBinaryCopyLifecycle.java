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

package org.apache.hudi.parquet.io;

import org.apache.hudi.core.io.storage.HoodieFileMetadataMerger;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.CompressionConverter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TestHoodieParquetBinaryCopyLifecycle {
  @Test
  void testCloseClearsWriterWhenEndFails() throws Exception {
    TestCopy copy = new TestCopy();
    ParquetFileWriter writer = mock(ParquetFileWriter.class);
    copy.setWriter(writer);
    IOException failure = new IOException("end failed");
    doThrow(failure).when(writer).end(any());
    assertSame(failure, assertThrows(IOException.class, copy::close));
    assertNull(copy.getWriter());
    assertDoesNotThrow(copy::close);
    verify(writer).end(any());
  }

  @Test
  void testClosePreservesMetadataFailure() throws Exception {
    TestCopy copy = new TestCopy();
    ParquetFileWriter writer = mock(ParquetFileWriter.class);
    copy.setWriter(writer);
    copy.metadataFailure = new IllegalStateException("metadata failed");
    IOException closeFailure = new IOException("close failed");
    if (writer instanceof AutoCloseable) {
      doThrow(closeFailure).when((AutoCloseable) writer).close();
    }
    assertSame(copy.metadataFailure, assertThrows(IllegalStateException.class, copy::close));
    assertArrayEquals(writer instanceof AutoCloseable ? new Throwable[] {closeFailure} : new Throwable[0],
        copy.metadataFailure.getSuppressed());
    assertNull(copy.getWriter());
  }

  @Test
  void testFailedFinalizationClosesReaderAndExecutor() throws Exception {
    HoodieParquetFileBinaryCopier copy = new HoodieParquetFileBinaryCopier(
        new Configuration(), CompressionCodecName.UNCOMPRESSED, new HoodieFileMetadataMerger());
    ExecutorService executor = copy.getPrefetchExecutor();
    executor.submit(() -> { }).get();
    CompressionConverter.TransParquetFileReader reader = mock(CompressionConverter.TransParquetFileReader.class);
    IOException closeFailure = new IOException("reader close failed");
    doThrow(closeFailure).when(reader).close();
    copy.setReader(reader);
    ParquetFileWriter writer = mock(ParquetFileWriter.class);
    IOException failure = new IOException("end failed");
    doThrow(failure).when(writer).end(any());
    copy.setWriter(writer);
    try {
      assertSame(failure, assertThrows(IOException.class, copy::close));
      assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
      assertTrue(executor.isShutdown());
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertDoesNotThrow(copy::close);
      verify(reader).close();
      verify(writer).end(any());
    } finally {
      executor.shutdownNow();
    }
  }

  private static class TestCopy extends HoodieParquetBinaryCopyBase {
    private RuntimeException metadataFailure;

    private TestCopy() {
      super(new Configuration());
    }

    @Override
    protected Map<String, String> finalizeMetadata() {
      if (metadataFailure != null) {
        throw metadataFailure;
      }
      return Collections.emptyMap();
    }
  }
}
