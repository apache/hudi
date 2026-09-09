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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieLogFormatWriter {

  private static final String WRITE_FAIL = "write-fail";
  private static final String CLOSE_FAIL = "close-fail";
  private static final String SYNC_FAIL = "sync-fail";

  @TempDir
  java.nio.file.Path tempDir;

  @Test
  void testCloseOutputOnAppendWriteException() throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.toString());
    HoodieLogFormatWriter writer = newWriter(storage);
    try {
      CloseTrackingOutputStream outputStream = new CloseTrackingOutputStream(true, false);
      writer.withOutputStream(newFSDataOutputStream(outputStream, storage));

      IOException exception = assertThrows(IOException.class, () -> writer.appendBlock(commandBlock()));

      assertEquals(WRITE_FAIL, exception.getMessage());
      assertTrue(outputStream.isClosed());
      assertThrows(IllegalStateException.class, writer::getCurrentSize);
    } finally {
      writer.close();
    }
  }

  @Test
  void testPreserveAppendExceptionWhenCloseFails() throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.toString());
    HoodieLogFormatWriter writer = newWriter(storage);
    try {
      CloseTrackingOutputStream outputStream = new CloseTrackingOutputStream(true, true);
      writer.withOutputStream(newFSDataOutputStream(outputStream, storage));

      IOException exception = assertThrows(IOException.class, () -> writer.appendBlock(commandBlock()));

      assertEquals(WRITE_FAIL, exception.getMessage());
      assertTrue(outputStream.isClosed());
      assertEquals(1, exception.getSuppressed().length);
      assertEquals(CLOSE_FAIL, exception.getSuppressed()[0].getMessage());
      assertThrows(IllegalStateException.class, writer::getCurrentSize);
    } finally {
      writer.close();
    }
  }

  @Test
  void testCloseOutputWhenSyncFailsOnClose() throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.toString());
    HoodieLogFormatWriter writer = newWriter(storage);
    try {
      CloseTrackingOutputStream outputStream = new CloseTrackingOutputStream(false, false);
      writer.withOutputStream(new SyncFailingFSDataOutputStream(outputStream, storage));

      IOException exception = assertThrows(IOException.class, writer::close);

      assertEquals(SYNC_FAIL, exception.getMessage());
      assertTrue(outputStream.isClosed());
      assertThrows(IllegalStateException.class, writer::getCurrentSize);
    } finally {
      writer.close();
    }
  }

  private HoodieLogFormatWriter newWriter(HoodieStorage storage) throws IOException {
    return HoodieLogFormatWriter.builder()
        .withParentPath(new StoragePath(tempDir.toString()))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId("test-fileid")
        .withInstantTime("100")
        .withLogVersion(1)
        .withStorage(storage)
        .build();
  }

  private HoodieCommandBlock commandBlock() {
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>();
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return new HoodieCommandBlock(header);
  }

  private FSDataOutputStream newFSDataOutputStream(CloseTrackingOutputStream outputStream, HoodieStorage storage)
      throws IOException {
    return new FSDataOutputStream(outputStream, new FileSystem.Statistics(storage.getScheme()));
  }

  private static class CloseTrackingOutputStream extends OutputStream {

    private final boolean failOnWrite;
    private final boolean failOnClose;
    private boolean closed;

    private CloseTrackingOutputStream(boolean failOnWrite, boolean failOnClose) {
      this.failOnWrite = failOnWrite;
      this.failOnClose = failOnClose;
    }

    @Override
    public void write(int b) throws IOException {
      if (failOnWrite) {
        throw new IOException(WRITE_FAIL);
      }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      if (failOnWrite) {
        throw new IOException(WRITE_FAIL);
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (failOnClose) {
        throw new IOException(CLOSE_FAIL);
      }
    }

    private boolean isClosed() {
      return closed;
    }
  }

  private static class SyncFailingFSDataOutputStream extends FSDataOutputStream {

    private SyncFailingFSDataOutputStream(CloseTrackingOutputStream outputStream, HoodieStorage storage)
        throws IOException {
      super(outputStream, new FileSystem.Statistics(storage.getScheme()));
    }

    @Override
    public void hsync() throws IOException {
      throw new IOException(SYNC_FAIL);
    }
  }
}
