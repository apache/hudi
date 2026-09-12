/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.hfile;

import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.compress.CompressionCodec;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression tests for compressed HFile block boundaries. */
class TestHFileBlockDecompression {

  // A valid gzip member header without a body. If checksum bytes are passed to the gzip decoder,
  // the decoder treats these bytes as a second member and fails with "invalid block type".
  private static final byte[] GZIP_MEMBER_HEADER = {
      0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00
  };

  @Test
  void compressedBlockDoesNotDecodeTrailingChecksums() throws IOException {
    HFileContext context = HFileContext.builder()
        .compressionCodec(CompressionCodec.GZIP)
        .blockSize(1024 * 1024)
        .build();
    byte[] value = new byte[128 * 1024];
    new Random(19929L).nextBytes(value);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (HFileWriter writer = new HFileWriterImpl(context, output)) {
      writer.append("row", value);
    }

    byte[] hfile = output.toByteArray();
    int dataBlockOffset;
    try (HFileReaderImpl reader = openReader(hfile)) {
      reader.initializeMetadata();
      dataBlockOffset = (int) reader.getDataBlockIndexMap().values().iterator().next().getOffset();
    }

    int onDiskSizeWithoutHeader = readInt(
        hfile, dataBlockOffset + HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    int onDiskDataSizeWithHeader = readInt(
        hfile, dataBlockOffset + HFileBlock.Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
    int compressedDataSize = onDiskDataSizeWithHeader - HFILEBLOCK_HEADER_SIZE;
    int checksumStart = dataBlockOffset + onDiskDataSizeWithHeader;
    int checksumLength = onDiskSizeWithoutHeader - compressedDataSize;
    assertTrue(checksumLength >= GZIP_MEMBER_HEADER.length,
        "the block must have enough checksum bytes for the gzip-boundary regression");
    System.arraycopy(GZIP_MEMBER_HEADER, 0, hfile, checksumStart, GZIP_MEMBER_HEADER.length);

    try (HFileReaderImpl reader = openReader(hfile)) {
      reader.initializeMetadata();
      assertEquals(1, reader.getNumKeyValueEntries());
      assertTrue(reader.seekTo());
      KeyValue keyValue = reader.getKeyValue().get();
      assertEquals("row", keyValue.getKey().getContentInString());
      assertArrayEquals(value, Arrays.copyOfRange(
          keyValue.getBytes(), keyValue.getValueOffset(),
          keyValue.getValueOffset() + keyValue.getValueLength()));
    }
  }

  private static HFileReaderImpl openReader(byte[] hfile) {
    return new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(hfile)), hfile.length);
  }
}
