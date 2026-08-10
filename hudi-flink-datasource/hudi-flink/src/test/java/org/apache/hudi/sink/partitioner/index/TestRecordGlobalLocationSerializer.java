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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test cases for {@link RecordGlobalLocationSerializer}.
 */
public class TestRecordGlobalLocationSerializer {

  private RecordGlobalLocationSerializer serializer;

  @BeforeEach
  public void setUp() {
    serializer = new RecordGlobalLocationSerializer();
  }

  @Test
  public void testSerializeDeserializeWithStandardUUID() throws IOException {
    // Test with a standard 36-character UUID (without file index)
    String partitionPath = "2024/03/15";
    String instantTime = "20240315120000";
    String fileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation original = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized = serializer.serialize(original);
    assertNotNull(serialized);

    HoodieRecordGlobalLocation deserialized = serializer.deserialize(serialized);

    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getInstantTime(), deserialized.getInstantTime());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testSerializeDeserializeWithFileIndex() throws IOException {
    // Test with UUID that includes a file index suffix
    String partitionPath = "partition/path";
    String instantTime = "20240315120000";
    UUID uuid = UUID.randomUUID();
    int fileIndex = 42;
    String fileId = uuid.toString() + "-" + fileIndex;

    HoodieRecordGlobalLocation original = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized = serializer.serialize(original);
    assertNotNull(serialized);

    HoodieRecordGlobalLocation deserialized = serializer.deserialize(serialized);

    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getInstantTime(), deserialized.getInstantTime());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testSerializeDeserializeWithEmptyPartition() throws IOException {
    // Test with empty partition path
    String partitionPath = "";
    String instantTime = "20240315120000";
    String fileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation original = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized = serializer.serialize(original);
    HoodieRecordGlobalLocation deserialized = serializer.deserialize(serialized);

    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getInstantTime(), deserialized.getInstantTime());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testSerializeDeserializeWithLongPartitionPath() throws IOException {
    // Test with a long partition path
    String partitionPath = "year=2024/month=03/day=15/hour=12/minute=30/second=45";
    String instantTime = "20240315123045";
    String fileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation original = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized = serializer.serialize(original);
    HoodieRecordGlobalLocation deserialized = serializer.deserialize(serialized);

    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getInstantTime(), deserialized.getInstantTime());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testSerializeDeserializeWithUnicodeCharacters() throws IOException {
    // Test with Unicode characters in partition path
    String partitionPath = "分区/路径/测试";
    String instantTime = "20240315120000";
    String fileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation original = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized = serializer.serialize(original);
    HoodieRecordGlobalLocation deserialized = serializer.deserialize(serialized);

    assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
    assertEquals(original.getInstantTime(), deserialized.getInstantTime());
    assertEquals(original.getFileId(), deserialized.getFileId());
  }

  @Test
  public void testMultipleSerializationCalls() throws IOException {
    // Test that the serializer can be reused for multiple serialization calls
    RecordGlobalLocationSerializer reusableSerializer = new RecordGlobalLocationSerializer();

    for (int i = 0; i < 10; i++) {
      String partitionPath = "partition" + i;
      String instantTime = "2024031512000" + i;
      String fileId = UUID.randomUUID().toString() + "-" + i;

      HoodieRecordGlobalLocation original = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

      byte[] serialized = reusableSerializer.serialize(original);
      HoodieRecordGlobalLocation deserialized = reusableSerializer.deserialize(serialized);

      assertEquals(original.getPartitionPath(), deserialized.getPartitionPath());
      assertEquals(original.getInstantTime(), deserialized.getInstantTime());
      assertEquals(original.getFileId(), deserialized.getFileId());
    }
  }

  @Test
  public void testSerializedDataConsistency() throws IOException {
    // Test that serializing the same object twice produces the same bytes
    String partitionPath = "partition/path";
    String instantTime = "20240315120000";
    String fileId = "550e8400-e29b-41d4-a716-446655440000";

    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized1 = serializer.serialize(location);
    byte[] serialized2 = serializer.serialize(location);

    assertArrayEquals(serialized1, serialized2);
  }

  @Test
  public void testSerializedSizeEfficiency() throws IOException {
    // Verify that serialized size is compact as expected
    String partitionPath = "partition/path/test";
    String instantTime = "20240315120000";
    String fileId = UUID.randomUUID().toString();

    HoodieRecordGlobalLocation location = new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);

    byte[] serialized = serializer.serialize(location);

    // Expected size calculation:
    // 4 bytes (partition length) + partition bytes
    // 4 bytes (instant length) + instant bytes
    // 8 bytes (UUID high bits) + 8 bytes (UUID low bits) + 4 bytes (file index)
    int expectedMinSize = 4 + partitionPath.length() + 4 + instantTime.length() + 8 + 8 + 4;

    assertEquals(expectedMinSize, serialized.length);
  }
}
