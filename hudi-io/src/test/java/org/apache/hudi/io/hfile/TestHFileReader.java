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
import org.apache.hudi.io.ByteArraySeekableDataInputStream;

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
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_BEFORE_FIRST_KEY;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_EOF;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_FOUND;
import static org.apache.hudi.io.hfile.HFileReader.SEEK_TO_IN_RANGE;
import static org.apache.hudi.io.hfile.HFileUtils.getValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
  private static final int SEEK_TO_THROW_EXCEPTION = -2;

  static Stream<Arguments> testArgsReadHFilePointAndPrefixLookup() {
    return Stream.of(
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_16KB_GZ_20000.hfile",
            20000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("a", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", SEEK_TO_FOUND, "hudi-key-000000000", "hudi-value-000000000"),
                // key in the block 0
                new KeyLookUpInfo("hudi-key-000000100", SEEK_TO_FOUND, "hudi-key-000000100", "hudi-value-000000100"),
                // backward seek not supported
                new KeyLookUpInfo("hudi-key-000000099", SEEK_TO_THROW_EXCEPTION, "", ""),
                // prefix lookup, the pointer should not move
                new KeyLookUpInfo("hudi-key-000000100a", SEEK_TO_IN_RANGE, "hudi-key-000000100",
                    "hudi-value-000000100"),
                new KeyLookUpInfo("hudi-key-000000100b", SEEK_TO_IN_RANGE, "hudi-key-000000100",
                    "hudi-value-000000100"),
                // prefix lookup with a jump, the pointer should not go beyond the lookup key
                new KeyLookUpInfo("hudi-key-000000200a", SEEK_TO_IN_RANGE, "hudi-key-000000200",
                    "hudi-value-000000200"),
                new KeyLookUpInfo("hudi-key-000000200b", SEEK_TO_IN_RANGE, "hudi-key-000000200",
                    "hudi-value-000000200"),
                // last key of the block 0
                new KeyLookUpInfo("hudi-key-000000277", SEEK_TO_FOUND, "hudi-key-000000277", "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277a", SEEK_TO_IN_RANGE, "hudi-key-000000277",
                    "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277b", SEEK_TO_IN_RANGE, "hudi-key-000000277",
                    "hudi-value-000000277"),
                // first key of the block 1
                new KeyLookUpInfo("hudi-key-000000278", SEEK_TO_FOUND, "hudi-key-000000278", "hudi-value-000000278"),
                // prefix before the first key of the block 9
                new KeyLookUpInfo("hudi-key-000002501a", SEEK_TO_IN_RANGE, "hudi-key-000002501",
                    "hudi-value-000002501"),
                new KeyLookUpInfo("hudi-key-000002501b", SEEK_TO_IN_RANGE, "hudi-key-000002501",
                    "hudi-value-000002501"),
                // first key of the block 30
                new KeyLookUpInfo("hudi-key-000008340", SEEK_TO_FOUND, "hudi-key-000008340", "hudi-value-000008340"),
                // last key of the block 49
                new KeyLookUpInfo("hudi-key-000013899", SEEK_TO_FOUND, "hudi-key-000013899", "hudi-value-000013899"),
                // seeking again should not move the pointer
                new KeyLookUpInfo("hudi-key-000013899", SEEK_TO_FOUND, "hudi-key-000013899", "hudi-value-000013899"),
                // adjacent keys
                new KeyLookUpInfo("hudi-key-000013900", SEEK_TO_FOUND, "hudi-key-000013900", "hudi-value-000013900"),
                new KeyLookUpInfo("hudi-key-000013901", SEEK_TO_FOUND, "hudi-key-000013901", "hudi-value-000013901"),
                new KeyLookUpInfo("hudi-key-000013902", SEEK_TO_FOUND, "hudi-key-000013902", "hudi-value-000013902"),
                // key in the block 70
                new KeyLookUpInfo("hudi-key-000019500", SEEK_TO_FOUND, "hudi-key-000019500", "hudi-value-000019500"),
                // prefix lookups
                new KeyLookUpInfo("hudi-key-0000196", SEEK_TO_IN_RANGE, "hudi-key-000019599", "hudi-value-000019599"),
                new KeyLookUpInfo("hudi-key-00001960", SEEK_TO_IN_RANGE, "hudi-key-000019599", "hudi-value-000019599"),
                new KeyLookUpInfo("hudi-key-000019600a", SEEK_TO_IN_RANGE, "hudi-key-000019600",
                    "hudi-value-000019600"),
                // second to last key
                new KeyLookUpInfo("hudi-key-000019998", SEEK_TO_FOUND, "hudi-key-000019998", "hudi-value-000019998"),
                // last key
                new KeyLookUpInfo("hudi-key-000019999", SEEK_TO_FOUND, "hudi-key-000019999", "hudi-value-000019999"),
                // after last key
                new KeyLookUpInfo("hudi-key-000019999a", SEEK_TO_EOF, "", ""),
                new KeyLookUpInfo("hudi-key-000019999b", SEEK_TO_EOF, "", "")
            )
        ),
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_512KB_GZ_20000.hfile",
            20000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("a", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", SEEK_TO_FOUND, "hudi-key-000000000", "hudi-value-000000000"),
                // last key of block 0
                new KeyLookUpInfo("hudi-key-000008886", SEEK_TO_FOUND, "hudi-key-000008886", "hudi-value-000008886"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-000008886a", SEEK_TO_IN_RANGE, "hudi-key-000008886",
                    "hudi-value-000008886"),
                new KeyLookUpInfo("hudi-key-000008886b", SEEK_TO_IN_RANGE, "hudi-key-000008886",
                    "hudi-value-000008886"),
                // keys in block 1
                new KeyLookUpInfo("hudi-key-000008888", SEEK_TO_FOUND, "hudi-key-000008888", "hudi-value-000008888"),
                new KeyLookUpInfo("hudi-key-000008889", SEEK_TO_FOUND, "hudi-key-000008889", "hudi-value-000008889"),
                new KeyLookUpInfo("hudi-key-000008890", SEEK_TO_FOUND, "hudi-key-000008890", "hudi-value-000008890"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-0000090", SEEK_TO_IN_RANGE, "hudi-key-000008999", "hudi-value-000008999"),
                new KeyLookUpInfo("hudi-key-00000900", SEEK_TO_IN_RANGE, "hudi-key-000008999", "hudi-value-000008999"),
                new KeyLookUpInfo("hudi-key-000009000a", SEEK_TO_IN_RANGE, "hudi-key-000009000",
                    "hudi-value-000009000"),
                // last key in block 1
                new KeyLookUpInfo("hudi-key-000017773", SEEK_TO_FOUND, "hudi-key-000017773", "hudi-value-000017773"),
                // after last key
                new KeyLookUpInfo("hudi-key-000020000", SEEK_TO_EOF, "", ""),
                new KeyLookUpInfo("hudi-key-000020001", SEEK_TO_EOF, "", "")
            )
        ),
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_16KB_NONE_5000.hfile",
            5000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("a", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", SEEK_TO_FOUND, "hudi-key-000000000", "hudi-value-000000000"),
                // key in the block 0
                new KeyLookUpInfo("hudi-key-000000100", SEEK_TO_FOUND, "hudi-key-000000100", "hudi-value-000000100"),
                // backward seek not supported
                new KeyLookUpInfo("hudi-key-000000099", SEEK_TO_THROW_EXCEPTION, "", ""),
                // prefix lookup, the pointer should not move
                new KeyLookUpInfo("hudi-key-000000100a", SEEK_TO_IN_RANGE, "hudi-key-000000100",
                    "hudi-value-000000100"),
                new KeyLookUpInfo("hudi-key-000000100b", SEEK_TO_IN_RANGE, "hudi-key-000000100",
                    "hudi-value-000000100"),
                // prefix lookup with a jump, the pointer should not go beyond the lookup key
                new KeyLookUpInfo("hudi-key-000000200a", SEEK_TO_IN_RANGE, "hudi-key-000000200",
                    "hudi-value-000000200"),
                new KeyLookUpInfo("hudi-key-000000200b", SEEK_TO_IN_RANGE, "hudi-key-000000200",
                    "hudi-value-000000200"),
                // last key of the block 0
                new KeyLookUpInfo("hudi-key-000000277", SEEK_TO_FOUND, "hudi-key-000000277", "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277a", SEEK_TO_IN_RANGE, "hudi-key-000000277",
                    "hudi-value-000000277"),
                new KeyLookUpInfo("hudi-key-000000277b", SEEK_TO_IN_RANGE, "hudi-key-000000277",
                    "hudi-value-000000277"),
                // first key of the block 1
                new KeyLookUpInfo("hudi-key-000000278", SEEK_TO_FOUND, "hudi-key-000000278", "hudi-value-000000278"),
                // prefix before the first key of the block 9
                new KeyLookUpInfo("hudi-key-000002501a", SEEK_TO_IN_RANGE, "hudi-key-000002501",
                    "hudi-value-000002501"),
                new KeyLookUpInfo("hudi-key-000002501b", SEEK_TO_IN_RANGE, "hudi-key-000002501",
                    "hudi-value-000002501"),
                // first key of the block 12
                new KeyLookUpInfo("hudi-key-000003336", SEEK_TO_FOUND, "hudi-key-000003336", "hudi-value-000003336"),
                // last key of the block 14
                new KeyLookUpInfo("hudi-key-000004169", SEEK_TO_FOUND, "hudi-key-000004169", "hudi-value-000004169"),
                // seeking again should not move the pointer
                new KeyLookUpInfo("hudi-key-000004169", SEEK_TO_FOUND, "hudi-key-000004169", "hudi-value-000004169"),
                // keys in the block 16
                new KeyLookUpInfo("hudi-key-000004600", SEEK_TO_FOUND, "hudi-key-000004600", "hudi-value-000004600"),
                new KeyLookUpInfo("hudi-key-000004601", SEEK_TO_FOUND, "hudi-key-000004601", "hudi-value-000004601"),
                new KeyLookUpInfo("hudi-key-000004602", SEEK_TO_FOUND, "hudi-key-000004602", "hudi-value-000004602"),
                // prefix lookups
                new KeyLookUpInfo("hudi-key-0000047", SEEK_TO_IN_RANGE, "hudi-key-000004699", "hudi-value-000004699"),
                new KeyLookUpInfo("hudi-key-00000470", SEEK_TO_IN_RANGE, "hudi-key-000004699", "hudi-value-000004699"),
                new KeyLookUpInfo("hudi-key-000004700a", SEEK_TO_IN_RANGE, "hudi-key-000004700",
                    "hudi-value-000004700"),
                // second to last key
                new KeyLookUpInfo("hudi-key-000004998", SEEK_TO_FOUND, "hudi-key-000004998", "hudi-value-000004998"),
                // last key
                new KeyLookUpInfo("hudi-key-000004999", SEEK_TO_FOUND, "hudi-key-000004999", "hudi-value-000004999"),
                // after last key
                new KeyLookUpInfo("hudi-key-000004999a", SEEK_TO_EOF, "", ""),
                new KeyLookUpInfo("hudi-key-000004999b", SEEK_TO_EOF, "", "")
            )
        ),
        Arguments.of(
            "/hfile/hudi_1_0_hbase_2_4_9_64KB_NONE_5000.hfile",
            5000,
            Arrays.asList(
                // before first key
                new KeyLookUpInfo("", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("a", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                new KeyLookUpInfo("hudi-key-0000000", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
                // first key
                new KeyLookUpInfo("hudi-key-000000000", SEEK_TO_FOUND, "hudi-key-000000000", "hudi-value-000000000"),
                // last key of block 0
                new KeyLookUpInfo("hudi-key-000001110", SEEK_TO_FOUND, "hudi-key-000001110", "hudi-value-000001110"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-000001110a", SEEK_TO_IN_RANGE, "hudi-key-000001110",
                    "hudi-value-000001110"),
                new KeyLookUpInfo("hudi-key-000001110b", SEEK_TO_IN_RANGE, "hudi-key-000001110",
                    "hudi-value-000001110"),
                // keys in block 1
                new KeyLookUpInfo("hudi-key-000001688", SEEK_TO_FOUND, "hudi-key-000001688", "hudi-value-000001688"),
                new KeyLookUpInfo("hudi-key-000001689", SEEK_TO_FOUND, "hudi-key-000001689", "hudi-value-000001689"),
                new KeyLookUpInfo("hudi-key-000001690", SEEK_TO_FOUND, "hudi-key-000001690", "hudi-value-000001690"),
                // prefix lookup
                new KeyLookUpInfo("hudi-key-0000023", SEEK_TO_IN_RANGE, "hudi-key-000002299", "hudi-value-000002299"),
                new KeyLookUpInfo("hudi-key-00000230", SEEK_TO_IN_RANGE, "hudi-key-000002299", "hudi-value-000002299"),
                new KeyLookUpInfo("hudi-key-000002300a", SEEK_TO_IN_RANGE, "hudi-key-000002300",
                    "hudi-value-000002300"),
                // last key in block 2
                new KeyLookUpInfo("hudi-key-000003332", SEEK_TO_FOUND, "hudi-key-000003332", "hudi-value-000003332"),
                // after last key
                new KeyLookUpInfo("hudi-key-000020000", SEEK_TO_EOF, "", ""),
                new KeyLookUpInfo("hudi-key-000020001", SEEK_TO_EOF, "", "")
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
  public void testReadHFileWithNonUniqueKeys() throws IOException {
    try (HFileReader reader = getHFileReader("/hfile/hudi_1_0_hbase_2_4_9_16KB_GZ_200_20_non_unique.hfile")) {
      reader.initializeMetadata();
      verifyHFileMetadata(reader, 4200);

      assertFalse(reader.isSeeked());
      assertFalse(reader.next());
      assertTrue(reader.seekTo());

      int numKeys = 200;
      // Calling reader.next()
      for (int i = 0; i < numKeys; i++) {
        Option<KeyValue> keyValue = reader.getKeyValue();
        assertTrue(keyValue.isPresent());
        Key expectedKey = new UTF8StringKey(KEY_CREATOR.apply(i));
        String value = VALUE_CREATOR.apply(i);
        assertEquals(expectedKey, keyValue.get().getKey());
        assertEquals(value, getValue(keyValue.get()));
        assertTrue(reader.next());

        for (int j = 0; j < 20; j++) {
          keyValue = reader.getKeyValue();
          assertTrue(keyValue.isPresent());
          assertEquals(expectedKey, keyValue.get().getKey());
          assertEquals(value + "_" + j, getValue(keyValue.get()));
          if (i == numKeys - 1 && j == 19) {
            assertFalse(reader.next());
          } else {
            assertTrue(reader.next());
          }
        }
      }

      assertTrue(reader.seekTo());
      // Calling reader.seekTo(key) on each key
      for (int i = 0; i < numKeys; i++) {
        Key expectedKey = new UTF8StringKey(KEY_CREATOR.apply(i));

        for (int j = 0; j < 1; j++) {
          // seekTo twice and the results should be the same
          assertEquals(SEEK_TO_FOUND, reader.seekTo(expectedKey));
          Option<KeyValue> keyValue = reader.getKeyValue();
          assertTrue(keyValue.isPresent());
          String value = VALUE_CREATOR.apply(i);
          assertEquals(expectedKey, keyValue.get().getKey());
          assertEquals(value, getValue(keyValue.get()));
        }

        assertTrue(reader.next());
        for (int j = 0; j < 1; j++) {
          // seekTo twice and the results should be the same
          assertEquals(SEEK_TO_FOUND, reader.seekTo(expectedKey));
          Option<KeyValue> keyValue = reader.getKeyValue();
          assertTrue(keyValue.isPresent());
          String value = VALUE_CREATOR.apply(i);
          assertEquals(expectedKey, keyValue.get().getKey());
          assertEquals(value + "_0", getValue(keyValue.get()));
        }
      }

      verifyHFileSeekToReads(
          reader,
          // point and prefix lookups
          Arrays.asList(
              // before first key
              new KeyLookUpInfo("", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
              new KeyLookUpInfo("a", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
              new KeyLookUpInfo("hudi-key-0000000", SEEK_TO_BEFORE_FIRST_KEY, "", ""),
              // first key
              new KeyLookUpInfo("hudi-key-000000000", SEEK_TO_FOUND, "hudi-key-000000000", "hudi-value-000000000"),
              // key in the block 0
              new KeyLookUpInfo("hudi-key-000000005", SEEK_TO_FOUND, "hudi-key-000000005", "hudi-value-000000005"),
              // backward seek not supported
              new KeyLookUpInfo("hudi-key-000000004", SEEK_TO_THROW_EXCEPTION, "", ""),
              // prefix lookup, the pointer should move to the entry before
              new KeyLookUpInfo("hudi-key-000000006a", SEEK_TO_IN_RANGE, "hudi-key-000000006",
                  "hudi-value-000000006_19"),
              new KeyLookUpInfo("hudi-key-000000006b", SEEK_TO_IN_RANGE, "hudi-key-000000006",
                  "hudi-value-000000006_19"),
              // prefix lookup with a jump, the pointer should not go beyond the lookup key
              new KeyLookUpInfo("hudi-key-000000008a", SEEK_TO_IN_RANGE, "hudi-key-000000008",
                  "hudi-value-000000008_19"),
              new KeyLookUpInfo("hudi-key-000000008b", SEEK_TO_IN_RANGE, "hudi-key-000000008",
                  "hudi-value-000000008_19"),
              // last key of the block 0
              new KeyLookUpInfo("hudi-key-000000012", SEEK_TO_FOUND, "hudi-key-000000012", "hudi-value-000000012"),
              new KeyLookUpInfo("hudi-key-000000012a", SEEK_TO_IN_RANGE, "hudi-key-000000012",
                  "hudi-value-000000012_19"),
              new KeyLookUpInfo("hudi-key-000000012b", SEEK_TO_IN_RANGE, "hudi-key-000000012",
                  "hudi-value-000000012_19"),
              // first key of the block 1
              new KeyLookUpInfo("hudi-key-000000013", SEEK_TO_FOUND, "hudi-key-000000013", "hudi-value-000000013"),
              // prefix before the first key of the block 5
              new KeyLookUpInfo("hudi-key-000000064a", SEEK_TO_IN_RANGE, "hudi-key-000000064",
                  "hudi-value-000000064_19"),
              new KeyLookUpInfo("hudi-key-000000064b", SEEK_TO_IN_RANGE, "hudi-key-000000064",
                  "hudi-value-000000064_19"),
              // first key of the block 8
              new KeyLookUpInfo("hudi-key-000000104", SEEK_TO_FOUND, "hudi-key-000000104", "hudi-value-000000104"),
              // last key of the block 11
              new KeyLookUpInfo("hudi-key-000000155", SEEK_TO_FOUND, "hudi-key-000000155", "hudi-value-000000155"),
              // seeking again should not move the pointer
              new KeyLookUpInfo("hudi-key-000000155", SEEK_TO_FOUND, "hudi-key-000000155", "hudi-value-000000155"),
              // adjacent keys
              new KeyLookUpInfo("hudi-key-000000156", SEEK_TO_FOUND, "hudi-key-000000156", "hudi-value-000000156"),
              new KeyLookUpInfo("hudi-key-000000157", SEEK_TO_FOUND, "hudi-key-000000157", "hudi-value-000000157"),
              new KeyLookUpInfo("hudi-key-000000158", SEEK_TO_FOUND, "hudi-key-000000158", "hudi-value-000000158"),
              // prefix lookups in the block 14
              new KeyLookUpInfo("hudi-key-00000019", SEEK_TO_IN_RANGE, "hudi-key-000000189",
                  "hudi-value-000000189_19"),
              new KeyLookUpInfo("hudi-key-000000190a", SEEK_TO_IN_RANGE, "hudi-key-000000190",
                  "hudi-value-000000190_19"),
              // second to last key
              new KeyLookUpInfo("hudi-key-000000198", SEEK_TO_FOUND, "hudi-key-000000198", "hudi-value-000000198"),
              // last key
              new KeyLookUpInfo("hudi-key-000000199", SEEK_TO_FOUND, "hudi-key-000000199", "hudi-value-000000199"),
              // after last key
              new KeyLookUpInfo("hudi-key-000000199a", SEEK_TO_EOF, "", ""),
              new KeyLookUpInfo("hudi-key-000000199b", SEEK_TO_EOF, "", "")
          )
      );
    }
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
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(content)), content.length);
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
        assertEquals(SEEK_TO_FOUND, reader.seekTo(expecedKey));
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
      int expectedSeekToResult = keyLookUpInfo.getExpectedSeekToResult();
      if (expectedSeekToResult == SEEK_TO_THROW_EXCEPTION) {
        assertThrows(
            IllegalStateException.class,
            () -> reader.seekTo(new UTF8StringKey(keyLookUpInfo.getLookUpKey())));
      } else {
        assertEquals(
            expectedSeekToResult,
            reader.seekTo(new UTF8StringKey(keyLookUpInfo.getLookUpKey())),
            String.format("Unexpected seekTo result for lookup key %s", keyLookUpInfo.getLookUpKey()));
      }
      switch (expectedSeekToResult) {
        case SEEK_TO_THROW_EXCEPTION:
        case SEEK_TO_BEFORE_FIRST_KEY:
          break;
        case SEEK_TO_FOUND:
        case SEEK_TO_IN_RANGE:
          assertTrue(reader.getKeyValue().isPresent());
          assertEquals(new UTF8StringKey(keyLookUpInfo.getExpectedKey()),
              reader.getKeyValue().get().getKey());
          assertEquals(keyLookUpInfo.getExpectedValue(), getValue(reader.getKeyValue().get()));
          break;
        case SEEK_TO_EOF:
          assertFalse(reader.getKeyValue().isPresent());
          assertFalse(reader.next());
          break;
        default:
          throw new IllegalArgumentException(
              "SeekTo result not allowed: " + keyLookUpInfo.expectedSeekToResult);
      }
    }
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
}
