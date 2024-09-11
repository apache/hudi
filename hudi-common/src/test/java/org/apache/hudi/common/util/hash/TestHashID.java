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

package org.apache.hudi.common.util.hash;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.xml.bind.DatatypeConverter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static java.util.Arrays.asList;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HashID}.
 */
public class TestHashID {

  public static List<Arguments> hashIdSizes() {
    return asList(
        Arguments.of(HashID.Size.BITS_32),
        Arguments.of(HashID.Size.BITS_64),
        Arguments.of(HashID.Size.BITS_128)
    );
  }

  /**
   * Test HashID of all sizes for ByteArray type input message.
   */
  @ParameterizedTest
  @EnumSource(HashID.Size.class)
  public void testHashForByteInput(HashID.Size size) {
    final int count = 8;
    Random random = new Random();
    for (int i = 0; i < count; i++) {
      final String message = random.ints(50, 120)
          .filter(j -> (j <= 57 || j >= 65) && (j <= 90 || j >= 97))
          .limit((32 + (i * 4)))
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
      final byte[] originalData = getUTF8Bytes(message);
      final byte[] hashBytes = HashID.hash(originalData, size);
      assertEquals(hashBytes.length, size.byteSize());
    }
  }

  /**
   * Test HashID of all sizes for String type input message.
   */
  @ParameterizedTest
  @EnumSource(HashID.Size.class)
  public void testHashForStringInput(HashID.Size size) {
    final int count = 8;
    Random random = new Random();
    for (int i = 0; i < count; i++) {
      final String message = random.ints(50, 120)
          .filter(j -> (j <= 57 || j >= 65) && (j <= 90 || j >= 97))
          .limit((32 + (i * 4)))
          .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
          .toString();
      final byte[] hashBytes = HashID.hash(message, size);
      assertEquals(hashBytes.length, size.byteSize());
    }
  }

  /**
   * Test expected hash values for all bit sizes.
   */
  @Test
  public void testHashValues() {
    Map<HashID.Size, Map<String, String>> expectedValuesMap = new HashMap<HashID.Size, Map<String, String>>();
    Map<String, String> hash32ExpectedValues = new HashMap<String, String>() {
      {
        put("Hudi", "FB6A3F92");
        put("Data lake", "99913A4D");
        put("Data Lake", "6F7DAD6A");
        put("Col1", "B4393B9A");
        put("A", "CDD946CE");
        put("2021/10/28/", "BBD4FDB2");
      }
    };
    expectedValuesMap.put(HashID.Size.BITS_32, hash32ExpectedValues);

    Map<String, String> hash64ExpectedValues = new HashMap<String, String>() {
      {
        put("Hudi", "F7727B9A28379071");
        put("Data lake", "52BC72D592EBCAE5");
        put("Data Lake", "5ED19AF9FD746E3E");
        put("Col1", "22FB1DD2F4784D31");
        put("A", "EBF88350484B5AA7");
        put("2021/10/28/", "2A9399AF6E7C8B12");
      }
    };
    expectedValuesMap.put(HashID.Size.BITS_128, hash64ExpectedValues);

    Map<String, String> hash128ExpectedValues = new HashMap<String, String>() {
      {
        put("Hudi", "09DAB749F255311C1C9EF6DD7B790170");
        put("Data lake", "7F2FC1EA445FC81F67CAA25EC9089C08");
        put("Data Lake", "9D2CEF0D61B02848C528A070ED75C570");
        put("Col1", "EC0FFE21E704DE2A580661C59A81D453");
        put("A", "7FC56270E7A70FA81A5935B72EACBE29");
        put("2021/10/28/", "1BAE8F04F44CB7ACF2458EF5219742DC");
      }
    };
    expectedValuesMap.put(HashID.Size.BITS_128, hash128ExpectedValues);

    for (Map.Entry<HashID.Size, Map<String, String>> allSizeEntries : expectedValuesMap.entrySet()) {
      for (Map.Entry<String, String> sizeEntry : allSizeEntries.getValue().entrySet()) {
        final byte[] actualHashBytes = HashID.hash(sizeEntry.getKey(), allSizeEntries.getKey());
        final byte[] expectedHashBytes = DatatypeConverter.parseHexBinary(sizeEntry.getValue());
        assertArrayEquals(expectedHashBytes, actualHashBytes);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("hashIdSizes")
  void testGenerateXXHashMagicNumber(HashID.Size size) throws IOException, URISyntaxException {
    // We need to make sure we always generate the same hash value for the same input. This test
    // guards against unexpected change of hash value due to accidents like library version upgrade.
    // Load inputs and expected hash values from files.
    List<String> inputs = Files.readAllLines(Paths.get(Objects.requireNonNull(
        getClass().getClassLoader().getResource("hash/magic_input.txt")).toURI()));
    List<String> expectedHash = Files.readAllLines(Paths.get(Objects.requireNonNull(
        getClass().getClassLoader().getResource(String.format("hash/xxhash_%s_for_magic_input.txt", size.name()))).toURI()));

    for (int i = 0; i < expectedHash.size(); ++i) {
      String hash = HashID.generateXXHashAsString(inputs.get(i), size);
      // Magic number test to guard against accidental upgrade that changes the hash value
      assertEquals(expectedHash.get(i), hash);
    }
  }
}
