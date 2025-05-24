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

package org.apache.hudi.io.compress;

import org.apache.hudi.io.compress.airlift.HoodieAirliftGzipCompressor;
import org.apache.hudi.io.compress.builtin.HoodieNoneCompressor;
import org.apache.hudi.io.util.IOUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests all implementation of {@link HoodieCompressor}.
 */
public class TestHoodieCompressor {
  private static final int INPUT_LENGTH = 394850;
  private static final int[] READ_PART_SIZE_LIST =
      new int[] {1200, 30956, 204958, INPUT_LENGTH + 50};
  private static final byte[] INPUT_BYTES = generateRandomBytes(INPUT_LENGTH);

  @ParameterizedTest
  @EnumSource(CompressionCodec.class)
  public void testDefaultDecompressors(CompressionCodec codec) throws IOException {
    switch (codec) {
      case NONE:
      case GZIP:
        HoodieCompressor decompressor = HoodieCompressorFactory.getCompressor(codec);
        byte[] actualOutput = new byte[INPUT_LENGTH + 100];
        try (InputStream stream = prepareInputStream(codec)) {
          for (int sizeToRead : READ_PART_SIZE_LIST) {
            stream.mark(INPUT_LENGTH);
            int actualSizeRead =
                decompressor.decompress(stream, actualOutput, 4, sizeToRead);
            assertEquals(actualSizeRead, Math.min(INPUT_LENGTH, sizeToRead));
            assertEquals(0, IOUtils.compareTo(
                actualOutput, 4, actualSizeRead, INPUT_BYTES, 0, actualSizeRead));
            stream.reset();
          }
        }
        break;
      default:
        assertThrows(
            IllegalArgumentException.class, () -> HoodieCompressorFactory.getCompressor(codec));
    }
  }

  private static InputStream prepareInputStream(CompressionCodec codec) throws IOException {
    switch (codec) {
      case NONE:
        return new ByteArrayInputStream(
            new HoodieNoneCompressor().compress(INPUT_BYTES));
      case GZIP:
        return new ByteArrayInputStream(
            new HoodieAirliftGzipCompressor().compress(INPUT_BYTES));
      default:
        throw new IllegalArgumentException("Not supported in tests.");
    }
  }

  private static byte[] generateRandomBytes(int length) {
    Random random = new Random(0x8e96);
    byte[] result = new byte[length];
    int chunkSize = 16384;
    int numChunks = length / chunkSize;
    // Fill in the same bytes in all chunks
    if (numChunks > 0) {
      byte[] chunk = new byte[chunkSize];
      random.nextBytes(chunk);
      for (int i = 0; i < numChunks; i++) {
        System.arraycopy(chunk, 0, result, chunkSize * i, chunkSize);
      }
    }
    // Fill in random bytes in the remaining
    for (int i = numChunks * chunkSize; i < length; i++) {
      result[i] = (byte) (random.nextInt() & 0xff);
    }
    return result;
  }
}
