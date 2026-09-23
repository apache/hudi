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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.util.FileIOUtils.readAsByteArray;
import static org.apache.hudi.io.util.IOUtils.readInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression tests for compressed HFile block boundaries. */
class TestHFileBlockDecompression {

  private static final int CHECKSUM_LENGTH = 12;
  private static final byte[] GZIP_MEMBER_HEADER = {
      0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };

  private static Stream<Arguments> checksumTails() {
    Stream.Builder<Arguments> tails = Stream.builder();
    tails.add(Arguments.of("real CRC32C", (byte[]) null));
    tails.add(Arguments.of("zeros", new byte[CHECKSUM_LENGTH]));

    // A second gzip member with a reserved DEFLATE block type triggers ZipException.
    byte[] invalidBlock = Arrays.copyOf(GZIP_MEMBER_HEADER, CHECKSUM_LENGTH);
    invalidBlock[GZIP_MEMBER_HEADER.length] = 0x07;
    tails.add(Arguments.of("gzip header with invalid block type", invalidBlock));
    // An incomplete uncompressed DEFLATE block triggers EOFException.
    tails.add(Arguments.of("gzip header with truncated data",
        Arrays.copyOf(GZIP_MEMBER_HEADER, CHECKSUM_LENGTH)));

    Random random = new Random(19929L);
    for (int i = 0; i < 20; i++) {
      byte[] tail = new byte[CHECKSUM_LENGTH];
      random.nextBytes(tail);
      System.arraycopy(GZIP_MEMBER_HEADER, 0, tail, 0, 3);
      tails.add(Arguments.of("seeded random tail " + i, tail));
    }
    return tails.build();
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("checksumTails")
  void compressedBlockDoesNotDecodeTrailingChecksums(String description, byte[] checksumTail) throws IOException {
    byte[] original;
    try (InputStream input = getClass().getResourceAsStream(
        "/hfile/hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile")) {
      assertNotNull(input);
      original = readAsByteArray(input);
    }
    byte[] modified = original.clone();
    try (HFileReaderImpl expected = openReader(original)) {
      expected.initializeMetadata();
      int dataBlockOffset = (int) expected.getDataBlockIndexMap().values().iterator().next().getOffset();
      int onDiskSizeWithoutHeader = readInt(
          original, dataBlockOffset + HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
      int onDiskDataSizeWithHeader = readInt(
          original, dataBlockOffset + HFileBlock.Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
      int checksumLength = onDiskSizeWithoutHeader + HFILEBLOCK_HEADER_SIZE - onDiskDataSizeWithHeader;
      assertEquals(ChecksumType.CRC32C.getCode(), original[dataBlockOffset + HFileBlock.Header.CHECKSUM_TYPE_INDEX]);
      assertEquals(CHECKSUM_LENGTH, checksumLength);
      // The native reader does not validate CRCs. Changing only these bytes must not affect decoded rows.
      if (checksumTail != null) {
        assertEquals(checksumLength, checksumTail.length);
        System.arraycopy(checksumTail, 0, modified, dataBlockOffset + onDiskDataSizeWithHeader, checksumLength);
      }

      try (HFileReaderImpl actual = openReader(modified)) {
        actual.initializeMetadata();
        assertEquals(20000, expected.getNumKeyValueEntries());
        assertEquals(expected.getNumKeyValueEntries(), actual.getNumKeyValueEntries());
        assertTrue(expected.seekTo());
        assertTrue(actual.seekTo());
        int rows = 0;
        boolean hasNext;
        do {
          KeyValue expectedRow = expected.getKeyValue().get();
          KeyValue actualRow = actual.getKeyValue().get();
          assertArrayEquals(Arrays.copyOfRange(expectedRow.getBytes(), expectedRow.getKeyOffset(),
                  expectedRow.getValueOffset() + expectedRow.getValueLength()),
              Arrays.copyOfRange(actualRow.getBytes(), actualRow.getKeyOffset(),
                  actualRow.getValueOffset() + actualRow.getValueLength()), "row " + rows);
          assertEquals(expectedRow.getKeyLength(), actualRow.getKeyLength());
          rows++;
          hasNext = expected.next();
          assertEquals(hasNext, actual.next(), "next after row " + rows);
        } while (hasNext);
        assertEquals(expected.getNumKeyValueEntries(), rows);
      }
    }
  }

  private static HFileReaderImpl openReader(byte[] hfile) {
    return new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(hfile)), hfile.length);
  }
}
