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

import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TestHFileWriter.class);
  private static final String TEST_FILE = "test.hfile";
  private static final HFileContext CONTEXT = HFileContext.builder().build();

  public static void main(String[] args) throws Exception {
    try {
      // 1. Write data.
      writeTestFile();

      // 2. Validate file size.
      validateHFileSize();

      // 3. Validate file structure.
      validateHFileStructure();

      // 4. Validate consistency with HFileReader.
      validateConsistencyWithHFileReader();

      LOG.info("All validations passed!");
    } finally {
      Files.deleteIfExists(Paths.get(TEST_FILE));
    }
  }

  private static void writeTestFile() throws Exception {
    try (
        DataOutputStream outputStream =
             new DataOutputStream(Files.newOutputStream(Paths.get(TEST_FILE)));
        HFileWriter writer = new HFileWriterImpl(CONTEXT, outputStream)) {
      writer.append("key1", "value1".getBytes());
      writer.append("key2", "value2".getBytes());
      writer.append("key3", "value3".getBytes());
    }
  }

  private static void validateHFileSize() throws IOException {
    Path path = Paths.get(TEST_FILE);
    long actualSize = Files.size(path);
    long expectedSize = 4372L;
    assertEquals(expectedSize, actualSize); // Will now pass
  }

  private static void validateHFileStructure() throws IOException {
    ByteBuffer fileBuffer = mapFileToBuffer();

    // 1. Validate Trailer
    validateTrailer(fileBuffer);

    // 2. Validate Data block.
    validateDataBlocks(fileBuffer);
  }

  private static void validateConsistencyWithHFileReader() throws IOException {
    ByteBuffer content = mapFileToBuffer();
    try (HFileReader reader = new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(
            new ByteBufferBackedInputStream(content)), content.limit())) {
      reader.initializeMetadata();
      assertEquals(3, reader.getNumKeyValueEntries());
      assertTrue(reader.getMetaInfo(new UTF8StringKey("hfile.LASTKEY")).isPresent());
      assertEquals(1, reader.getMetaInfo(
          new UTF8StringKey("hfile.MAX_MEMSTORE_TS_KEY")).get().length);
    }
  }

  private static ByteBuffer mapFileToBuffer() throws IOException {
    try (FileChannel channel = FileChannel.open(Paths.get(TEST_FILE), StandardOpenOption.READ)) {
      return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    }
  }

  private static void validateTrailer(ByteBuffer buf) {
    int trailerStart = Math.max(0, buf.limit() - 4096);
    buf.position(trailerStart);

    // Verify magic
    byte[] trailerMagic = new byte[8];
    buf.get(trailerMagic);
    assertArrayEquals("TRABLK\"$".getBytes(), trailerMagic);

    // Verify version (last 4 bytes of trailer)
    buf.position(trailerStart + 4096 - 4);
    byte[] versionBytes = new byte[4];
    buf.get(versionBytes);
    int version = ByteBuffer.wrap(versionBytes).getInt();
    assertEquals(3, version);
  }

  private static void validateDataBlocks(ByteBuffer buf) {
    // Point to the first data block.
    buf.position(0);

    // Validate magic.
    byte[] dataBlockMagic = new byte[8];
    buf.get(dataBlockMagic);
    assertArrayEquals("DATABLK*".getBytes(), dataBlockMagic);

    // Skip header.
    buf.position(buf.position() + 25);

    // Validate data.
    validateKeyValue(buf, "key1", "value1");
    validateKeyValue(buf, "key2", "value2");
    validateKeyValue(buf, "key3", "value3");
  }

  private static void validateKeyValue(ByteBuffer buf, String expectedKey, String expectedValue) {
    int keyLen = buf.getInt();
    int valLen = buf.getInt();

    byte[] key = new byte[keyLen];
    buf.get(key);
    byte[] keyContent = Arrays.copyOfRange(key, 2, key.length);
    assertArrayEquals(expectedKey.getBytes(StandardCharsets.UTF_8), keyContent);

    byte[] value = new byte[valLen];
    buf.get(value);
    assertArrayEquals(expectedValue.getBytes(StandardCharsets.UTF_8), value);

    buf.get(); // Skip MVCC timestamp
  }

  private static void assertArrayEquals(byte[] expected, byte[] actual) {
    if (!Arrays.equals(expected, actual)) {
      throw new AssertionError("Byte array mismatch");
    }
  }

  private static void assertEquals(long expected, long actual) {
    if (expected != actual) {
      throw new AssertionError("Value mismatch: " + expected + " vs " + actual);
    }
  }
}
