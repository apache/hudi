/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link CodedRecordGlobalLocationSerializer}.
 */
public class TestCodedRecordGlobalLocationSerializer {

  private CodedRecordGlobalLocationSerializer serializer;

  @BeforeEach
  public void setUp() {
    serializer = new CodedRecordGlobalLocationSerializer();
  }

  @Test
  public void testHistoricalBytesRemainReadableAfterDictionaryGrowth() throws IOException {
    String instantTime = "20240315120000";
    String firstFileId = UUID.randomUUID().toString();
    String secondFileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation first = new HoodieRecordGlobalLocation("partition/a", instantTime, firstFileId);
    HoodieRecordGlobalLocation second = new HoodieRecordGlobalLocation("partition/b", instantTime, secondFileId);

    byte[] firstSerialized = serializer.serialize(first);
    byte[] secondSerialized = serializer.serialize(second);

    assertEquals(first, serializer.deserialize(firstSerialized));
    assertEquals(second, serializer.deserialize(secondSerialized));
  }

  @Test
  public void testSerializedSizeEfficiency() throws IOException {
    String partitionPath = "partition/path/test/with/repeated/value";
    String instantTime = "20240315120000";
    String fileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);
    HoodieRecordGlobalLocation samePartitionLocation = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] firstSerialized = serializer.serialize(location);
    byte[] secondSerialized = serializer.serialize(samePartitionLocation);

    int expectedSize = 4 + 4 + instantTime.length() + 8 + 8 + 4;
    int legacySize = 4 + partitionPath.getBytes(StandardCharsets.UTF_8).length + 4 + instantTime.length() + 8 + 8 + 4;

    assertEquals(expectedSize, firstSerialized.length);
    assertEquals(expectedSize, secondSerialized.length);
    assertEquals(firstSerialized.length, secondSerialized.length);
    assertTrue(firstSerialized.length < legacySize);
  }

  @Test
  public void testRepeatedPartitionPathProducesSmallerPayloadThanLegacyFormat() throws IOException {
    String partitionPath = "year=2024/month=03/day=15/hour=12";
    String instantTime = "20240315123045";
    String fileId = UUID.randomUUID().toString();

    byte[] serialized = serializer.serialize(new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId));

    int dictionaryEncodedSize = 4 + 4 + instantTime.length() + 8 + 8 + 4;
    int legacySize = 4 + partitionPath.getBytes(StandardCharsets.UTF_8).length + 4 + instantTime.length() + 8 + 8 + 4;

    assertEquals(dictionaryEncodedSize, serialized.length);
    assertEquals(legacySize - partitionPath.getBytes(StandardCharsets.UTF_8).length, serialized.length);
  }

  @Test
  public void testDeserializeFailsForUnknownPartitionPathId() {
    byte[] invalidBytes = new byte[] {
        0, 0, 0, 7,
        0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
        -1, -1, -1, -1
    };

    IllegalStateException exception = assertThrows(IllegalStateException.class, () -> serializer.deserialize(invalidBytes));
    assertEquals("Unknown partition path dictionary id 7, dictionary size is 0", exception.getMessage());
  }

  @Test
  public void testFirstPartitionPathUsesFirstDictionaryId() throws IOException {
    String instantTime = "20240315120000";
    String fileId = UUID.randomUUID().toString();

    byte[] serialized = serializer.serialize(new HoodieRecordGlobalLocation("partition/a", instantTime, fileId));

    assertEquals(0, readInt(serialized, 0));
  }

  private int readInt(byte[] bytes, int offset) {
    return ((bytes[offset] & 0xFF) << 24)
        | ((bytes[offset + 1] & 0xFF) << 16)
        | ((bytes[offset + 2] & 0xFF) << 8)
        | (bytes[offset + 3] & 0xFF);
  }
}
