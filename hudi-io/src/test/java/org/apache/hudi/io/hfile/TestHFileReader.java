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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.FileIOUtils.readAsByteArray;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_BACKWARDS;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_FOUND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HFileReader}
 */
public class TestHFileReader {
  public static final String SIMPLE_SCHEMA_HFILE_SUFFIX = "_simple.hfile";
  public static final String COMPLEX_SCHEMA_HFILE_SUFFIX = "_complex.hfile";
  public static final String BOOTSTRAP_INDEX_HFILE_SUFFIX = "_bootstrap_index_partitions.hfile";
  // Custom information added to file info block
  public static final String CUSTOM_META_KEY = "hudi_hfile_testing.custom_key";
  public static final String CUSTOM_META_VALUE = "hudi_custom_value";
  // Dummy Bloom filter bytes
  public static final String DUMMY_BLOOM_FILTER =
      "/////wAAABQBAAABID797Rg6cC9QEnS/mT3C01cdQGaLYH2jbOCLtMA0RWppEH1HQg==";
  public static final Function<Integer, String> KEY_CREATOR = i -> String.format("hudi-key-%09d", i);
  public static final Function<Integer, String> VALUE_CREATOR = i -> String.format("hudi-value-%09d", i);

  static Stream<Arguments> testArgsReadHFilePointAndPrefixLookup() {
    return Stream.of(
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile",
            20000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", SEEK_TO_BACKWARDS, "", ""),
                new KeyLookUpInfo("a", SEEK_TO_BACKWARDS, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", SEEK_TO_BACKWARDS, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", SEEK_TO_FOUND, "hudi-key-000000000", "hudi-value-000000000"),
                // key in the block 0
                new KeyLookUpInfo("hudi-key-000000100", SEEK_TO_FOUND, "hudi-key-000000100", "hudi-value-000000100"),
                // backward seek not supported
                new KeyLookUpInfo("hudi-key-000000099", -1, "", ""),
                // prefix lookup, the pointer should not move
                new KeyLookUpInfo("hudi-key-000000100a", 1, "hudi-key-000000100", "hudi-value-000000100"),
                new KeyLookUpInfo("hudi-key-000000100b", 1, "hudi-key-000000100", "hudi-value-000000100"),
                // prefix lookup with a jump, the pointer should not go beyond the lookup key
                new KeyLookUpInfo("hudi-key-000000200a", 1, "hudi-key-000000200", "hudi-value-000000200"),
                new KeyLookUpInfo("hudi-key-000000200b", 1, "hudi-key-000000200", "hudi-value-000000200"),
                // last key of the block 0
                new KeyLookUpInfo("hudi-key-000000277", 0, "hudi-key-000000277", "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277a", 1, "hudi-key-000000277", "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277b", 1, "hudi-key-000000277", "hudi-value-000000277"),
                // first key of the block 1
                new KeyLookUpInfo("hudi-key-000000278", 0, "hudi-key-000000278", "hudi-value-000000278"),
                // prefix before the first key of the block 9
                new KeyLookUpInfo("hudi-key-000002501a", 1, "hudi-key-000002501", "hudi-value-000002501"),
                new KeyLookUpInfo("hudi-key-000002501b", 1, "hudi-key-000002501", "hudi-value-000002501"),
                // first key of the block 30
                new KeyLookUpInfo("hudi-key-000008340", 0, "hudi-key-000008340", "hudi-value-000008340"),
                // last key of the block 49
                new KeyLookUpInfo("hudi-key-000013899", 0, "hudi-key-000013899", "hudi-value-000013899"),
                // seeking again should not move the pointer
                new KeyLookUpInfo("hudi-key-000013899", 0, "hudi-key-000013899", "hudi-value-000013899"),
                // key in the block 70
                new KeyLookUpInfo("hudi-key-000019500", 0, "hudi-key-000019500", "hudi-value-000019500"),
                // prefix lookups
                new KeyLookUpInfo("hudi-key-0000196", 1, "hudi-key-000019599", "hudi-value-000019599"),
                new KeyLookUpInfo("hudi-key-00001960", 1, "hudi-key-000019599", "hudi-value-000019599"),
                new KeyLookUpInfo("hudi-key-000019600a", 1, "hudi-key-000019600", "hudi-value-000019600"),
                // second to last key
                new KeyLookUpInfo("hudi-key-000019998", 0, "hudi-key-000019998", "hudi-value-000019998"),
                // last key
                new KeyLookUpInfo("hudi-key-000019999", 0, "hudi-key-000019999", "hudi-value-000019999"),
                // after last key
                new KeyLookUpInfo("hudi-key-000019999a", 2, "", ""),
                new KeyLookUpInfo("hudi-key-000019999b", 2, "", "")
            )
        ),
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile",
            20000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", -1, "", ""),
                new KeyLookUpInfo("a", -1, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", -1, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", 0, "hudi-key-000000000", "hudi-value-000000000"),
                // last key of block 0
                new KeyLookUpInfo("hudi-key-000008886", 0, "hudi-key-000008886", "hudi-value-000008886"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-000008886a", 1, "hudi-key-000008886", "hudi-value-000008886"),
                new KeyLookUpInfo("hudi-key-000008886b", 1, "hudi-key-000008886", "hudi-value-000008886"),
                // key in block 1
                new KeyLookUpInfo("hudi-key-000008888", 0, "hudi-key-000008888", "hudi-value-000008888"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-0000090", 1, "hudi-key-000008999", "hudi-value-000008999"),
                new KeyLookUpInfo("hudi-key-00000900", 1, "hudi-key-000008999", "hudi-value-000008999"),
                new KeyLookUpInfo("hudi-key-000009000a", 1, "hudi-key-000009000", "hudi-value-000009000"),
                // last key in block 1
                new KeyLookUpInfo("hudi-key-000017773", 0, "hudi-key-000017773", "hudi-value-000017773"),
                // after last key
                new KeyLookUpInfo("hudi-key-000020000", 2, "", ""),
                new KeyLookUpInfo("hudi-key-000020001", 2, "", "")
            )
        ),
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile",
            5000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", -1, "", ""),
                new KeyLookUpInfo("a", -1, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", -1, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", 0, "hudi-key-000000000", "hudi-value-000000000"),
                // key in the block 0
                new KeyLookUpInfo("hudi-key-000000100", 0, "hudi-key-000000100", "hudi-value-000000100"),
                // backward seek not supported
                new KeyLookUpInfo("hudi-key-000000099", -1, "", ""),
                // prefix lookup, the pointer should not move
                new KeyLookUpInfo("hudi-key-000000100a", 1, "hudi-key-000000100", "hudi-value-000000100"),
                new KeyLookUpInfo("hudi-key-000000100b", 1, "hudi-key-000000100", "hudi-value-000000100"),
                // prefix lookup with a jump, the pointer should not go beyond the lookup key
                new KeyLookUpInfo("hudi-key-000000200a", 1, "hudi-key-000000200", "hudi-value-000000200"),
                new KeyLookUpInfo("hudi-key-000000200b", 1, "hudi-key-000000200", "hudi-value-000000200"),
                // last key of the block 0
                new KeyLookUpInfo("hudi-key-000000277", 0, "hudi-key-000000277", "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277a", 1, "hudi-key-000000277", "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277b", 1, "hudi-key-000000277", "hudi-value-000000277"),
                // first key of the block 1
                new KeyLookUpInfo("hudi-key-000000278", 0, "hudi-key-000000278", "hudi-value-000000278"),
                // prefix before the first key of the block 9
                new KeyLookUpInfo("hudi-key-000002501a", 1, "hudi-key-000002501", "hudi-value-000002501"),
                new KeyLookUpInfo("hudi-key-000002501b", 1, "hudi-key-000002501", "hudi-value-000002501"),
                // first key of the block 12
                new KeyLookUpInfo("hudi-key-000003336", 0, "hudi-key-000003336", "hudi-value-000003336"),
                // last key of the block 14
                new KeyLookUpInfo("hudi-key-000004169", 0, "hudi-key-000004169", "hudi-value-000004169"),
                // seeking again should not move the pointer
                new KeyLookUpInfo("hudi-key-000004169", 0, "hudi-key-000004169", "hudi-value-000004169"),
                // key in the block 16
                new KeyLookUpInfo("hudi-key-000004600", 0, "hudi-key-000004600", "hudi-value-000004600"),
                // prefix lookups
                new KeyLookUpInfo("hudi-key-0000047", 1, "hudi-key-000004699", "hudi-value-000004699"),
                new KeyLookUpInfo("hudi-key-00000470", 1, "hudi-key-000004699", "hudi-value-000004699"),
                new KeyLookUpInfo("hudi-key-000004700a", 1, "hudi-key-000004700", "hudi-value-000004700"),
                // second to last key
                new KeyLookUpInfo("hudi-key-000004998", 0, "hudi-key-000004998", "hudi-value-000004998"),
                // last key
                new KeyLookUpInfo("hudi-key-000004999", 0, "hudi-key-000004999", "hudi-value-000004999"),
                // after last key
                new KeyLookUpInfo("hudi-key-000004999a", 2, "", ""),
                new KeyLookUpInfo("hudi-key-000004999b", 2, "", "")
            )
        ),
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_64KB_NONE_5000.hfile",
            5000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", -1, "", ""),
                new KeyLookUpInfo("a", -1, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", -1, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", 0, "hudi-key-000000000", "hudi-value-000000000"),
                // last key of block 0
                new KeyLookUpInfo("hudi-key-000001110", 0, "hudi-key-000001110", "hudi-value-000001110"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-000001110a", 1, "hudi-key-000001110", "hudi-value-000001110"),
                new KeyLookUpInfo("hudi-key-000001110b", 1, "hudi-key-000001110", "hudi-value-000001110"),
                // key in block 1
                new KeyLookUpInfo("hudi-key-000001688", 0, "hudi-key-000001688", "hudi-value-000001688"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-0000023", 1, "hudi-key-000002299", "hudi-value-000002299"),
                new KeyLookUpInfo("hudi-key-00000230", 1, "hudi-key-000002299", "hudi-value-000002299"),
                new KeyLookUpInfo("hudi-key-000002300a", 1, "hudi-key-000002300", "hudi-value-000002300"),
                // last key in block 2
                new KeyLookUpInfo("hudi-key-000003332", 0, "hudi-key-000003332", "hudi-value-000003332"),
                // after last key
                new KeyLookUpInfo("hudi-key-000020000", 2, "", ""),
                new KeyLookUpInfo("hudi-key-000020001", 2, "", "")
            )
        )
    );
  }

  @ParameterizedTest
  @MethodSource("testArgsReadHFilePointAndPrefixLookup")
  public void testReadHFilePointAndPrefixLookup(String filename,
                                                int numEntries,
                                                List<KeyLookUpInfo> keyLookUpInfoList) throws IOException {
    verifyHFileRead(filename, numEntries, KEY_CREATOR, VALUE_CREATOR, keyLookUpInfoList);
  }

  @Test
  public void testReadHFileWithoutKeyValueEntries() throws IOException {
    try (HFileReader reader = getHFileReader("/hfile/hudi_1_0_hbase_2_4_9_no_entry.hfile")) {
      reader.initializeMetadata();
      verifyHFileMetadataCompatibility(reader, 0);
      assertFalse(reader.isSeeked());
      assertFalse(reader.next());
      assertFalse(reader.seekTo());
      assertFalse(reader.next());
      assertEquals(2, reader.seekTo(new UTF8StringKey("random")));
      assertFalse(reader.next());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "/hfile/hudi_0_9_hbase_1_2_3", "/hfile/hudi_0_10_hbase_1_2_3", "/hfile/hudi_0_11_hbase_2_4_9"})
  public void testReadHFileCompatibility(String hfilePrefix) throws IOException {
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadPrimitiveRecord()
    // using different Hudi releases
    String simpleHFile = hfilePrefix + SIMPLE_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestHoodieReaderWriterBase#testWriteReadComplexRecord()
    // using different Hudi releases
    String complexHFile = hfilePrefix + COMPLEX_SCHEMA_HFILE_SUFFIX;
    // This fixture is generated from TestBootstrapIndex#testBootstrapIndex()
    // using different Hudi releases.  The file is copied from .hoodie/.aux/.bootstrap/.partitions/
    String bootstrapIndexFile = hfilePrefix + BOOTSTRAP_INDEX_HFILE_SUFFIX;

    Option<Function<Integer, String>> keyCreator = Option.of(i -> "key" + String.format("%02d", i));
    verifyHFileReadCompatibility(simpleHFile, 50, keyCreator);
    verifyHFileReadCompatibility(complexHFile, 50, keyCreator);
    verifyHFileReadCompatibility(bootstrapIndexFile, 4, Option.empty());
  }

  public static byte[] readHFileFromResources(String filename) throws IOException {
    long size = TestHFileReader.class
        .getResource(filename).openConnection().getContentLength();
    return readAsByteArray(
        TestHFileReader.class.getResourceAsStream(filename), (int) size);
  }

  public static HFileReader getHFileReader(String filename) throws IOException {
    byte[] content = readHFileFromResources(filename);
    return new HFileReaderImpl(
        new FSDataInputStream(new SeekableByteArrayInputStream(content)), content.length);
  }

  private static void verifyHFileRead(String filename,
                                      int numEntries,
                                      Function<Integer, String> keyCreator,
                                      Function<Integer, String> valueCreator,
                                      List<KeyLookUpInfo> keyLookUpInfoList) throws IOException {
    try (HFileReader reader = getHFileReader(filename)) {
      reader.initializeMetadata();
      verifyHFileMetadata(reader, numEntries);
      verifyHFileValuesInSequentialReads(reader, numEntries, Option.of(keyCreator), Option.of(valueCreator));
      verifyHFileSeekToReads(reader, keyLookUpInfoList);
    }
  }

  private static void verifyHFileMetadata(HFileReader reader, int numEntries) throws IOException {
    assertEquals(numEntries, reader.getNumKeyValueEntries());

    Option<byte[]> customValue = reader.getMetaInfo(new UTF8StringKey(CUSTOM_META_KEY));
    assertTrue(customValue.isPresent());
    assertEquals(CUSTOM_META_VALUE, new String(customValue.get(), StandardCharsets.UTF_8));

    Option<ByteBuffer> bloomFilter = reader.getMetaBlock("bloomFilter");
    assertTrue(bloomFilter.isPresent());
    assertEquals(DUMMY_BLOOM_FILTER, new String(
        bloomFilter.get().array(), bloomFilter.get().position(), bloomFilter.get().remaining(),
        StandardCharsets.UTF_8));
  }

  private static void verifyHFileReadCompatibility(String filename,
                                                   int numEntries,
                                                   Option<Function<Integer, String>> keyCreator) throws IOException {
    try (HFileReader reader = getHFileReader(filename)) {
      reader.initializeMetadata();
      verifyHFileMetadataCompatibility(reader, numEntries);
      verifyHFileValuesInSequentialReads(reader, numEntries, keyCreator);
    }
  }

  private static void verifyHFileMetadataCompatibility(HFileReader reader, int numEntries) {
    assertEquals(numEntries, reader.getNumKeyValueEntries());
  }

  private static void verifyHFileValuesInSequentialReads(HFileReader reader,
                                                         int numEntries,
                                                         Option<Function<Integer, String>> keyCreator)
      throws IOException {
    verifyHFileValuesInSequentialReads(reader, numEntries, keyCreator, Option.empty());
  }

  private static void verifyHFileValuesInSequentialReads(HFileReader reader,
                                                         int numEntries,
                                                         Option<Function<Integer, String>> keyCreator,
                                                         Option<Function<Integer, String>> valueCreator)
      throws IOException {
    assertFalse(reader.isSeeked());
    assertFalse(reader.next());
    boolean result = reader.seekTo();
    assertEquals(numEntries > 0, result);

    // Calling reader.next()
    for (int i = 0; i < numEntries; i++) {
      Option<KeyValue> keyValue = reader.getKeyValue();
      assertTrue(keyValue.isPresent());
      if (keyCreator.isPresent()) {
        assertEquals(new UTF8StringKey(keyCreator.get().apply(i)), keyValue.get().getKey());
      }
      if (valueCreator.isPresent()) {
        assertEquals(valueCreator.get().apply(i), getValue(keyValue.get()));
      }
      if (i < numEntries - 1) {
        assertTrue(reader.next());
      } else {
        assertFalse(reader.next());
      }
    }

    if (keyCreator.isPresent()) {
      result = reader.seekTo();
      assertEquals(numEntries > 0, result);
      // Calling reader.seekTo(key) on each key
      for (int i = 0; i < numEntries; i++) {
        Key expecedKey = new UTF8StringKey(keyCreator.get().apply(i));
        assertEquals(0, reader.seekTo(expecedKey));
        Option<KeyValue> keyValue = reader.getKeyValue();
        assertTrue(keyValue.isPresent());
        assertEquals(expecedKey, keyValue.get().getKey());
        if (valueCreator.isPresent()) {
          assertEquals(valueCreator.get().apply(i), getValue(keyValue.get()));
        }
      }
    }
  }

  private static void verifyHFileSeekToReads(HFileReader reader,
                                             List<KeyLookUpInfo> keyLookUpInfoList) throws IOException {
    assertTrue(reader.seekTo());

    for (KeyLookUpInfo keyLookUpInfo : keyLookUpInfoList) {
      assertEquals(keyLookUpInfo.getExpectedSeekToResult(),
          reader.seekTo(new UTF8StringKey(keyLookUpInfo.getLookUpKey())),
          String.format("Unexpected seekTo result for lookup key %s", keyLookUpInfo.getLookUpKey()));
      switch (keyLookUpInfo.getExpectedSeekToResult()) {
        case -1:
          break;
        case 0:
        case 1:
          assertTrue(reader.getKeyValue().isPresent());
          assertEquals(new UTF8StringKey(keyLookUpInfo.getExpectedKey()),
              reader.getKeyValue().get().getKey());
          assertEquals(keyLookUpInfo.getExpectedValue(), getValue(reader.getKeyValue().get()));
          break;
        case 2:
          assertFalse(reader.getKeyValue().isPresent());
          assertFalse(reader.next());
          break;
        default:
          throw new IllegalArgumentException(
              "SeekTo result not allowed: " + keyLookUpInfo.expectedSeekToResult);
      }
    }
  }

  private static String getValue(KeyValue kv) {
    return new String(kv.getBytes(), kv.getValueOffset(), kv.getValueLength());
  }

  static class KeyLookUpInfo {
    private final String lookUpKey;
    private final int expectedSeekToResult;
    private final String expectedKey;
    private final String expectedValue;

    public KeyLookUpInfo(String lookUpKey,
                         int expectedSeekToResult,
                         String expectedKey,
                         String expectedValue) {
      this.lookUpKey = lookUpKey;
      this.expectedSeekToResult = expectedSeekToResult;
      this.expectedKey = expectedKey;
      this.expectedValue = expectedValue;
    }

    public String getLookUpKey() {
      return lookUpKey;
    }

    public int getExpectedSeekToResult() {
      return expectedSeekToResult;
    }

    public String getExpectedKey() {
      return expectedKey;
    }

    public String getExpectedValue() {
      return expectedValue;
    }
  }

  static class SeekableByteArrayInputStream extends ByteBufferBackedInputStream implements Seekable,
      PositionedReadable {
    public SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public long getPos() throws IOException {
      return getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
      return copyFrom(position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      read(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      read(position, buffer, offset, length);
    }
  }
}
