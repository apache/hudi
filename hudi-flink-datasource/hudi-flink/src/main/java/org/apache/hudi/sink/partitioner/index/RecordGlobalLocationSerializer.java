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
import org.apache.hudi.common.serialization.CustomSerializer;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.apache.hudi.metadata.HoodieMetadataPayload.RECORD_INDEX_MISSING_FILEINDEX_FALLBACK;

/**
 * Custom serializer for {@link HoodieRecordGlobalLocation} that provides compact binary serialization.
 *
 * <p>This serializer is not thread-safe, and mainly designed for the single-threaded model in Flink's operator.
 * It reuses stream objects across serialization calls to reduce object allocation and GC pressure.
 */
public class RecordGlobalLocationSerializer implements CustomSerializer<HoodieRecordGlobalLocation> {

  // Initial buffer size: 128 bytes is sufficient for most serialized data
  // Typical size: 4(partitionLen) + ~30(partition) + 4(instantLen) + ~20(instant) + 8+8+4(UUID+index) ≈ 78 bytes
  private static final int INITIAL_BUFFER_SIZE = 128;
  // Reusable stream objects (single-threaded use only)
  private final DataOutputSerializer outputSerializer = new DataOutputSerializer(INITIAL_BUFFER_SIZE);
  private final DataInputDeserializer inputDeserializer = new DataInputDeserializer();

  @Override
  public byte[] serialize(HoodieRecordGlobalLocation location) throws IOException {
    // Reset buffer, retaining allocated memory
    outputSerializer.clear();

    // Write partitionPath
    byte[] partitionBytes = location.getPartitionPath().getBytes(StandardCharsets.UTF_8);
    outputSerializer.writeInt(partitionBytes.length);
    outputSerializer.write(partitionBytes);

    // Write instantTime
    byte[] instantBytes = location.getInstantTime().getBytes(StandardCharsets.UTF_8);
    outputSerializer.writeInt(instantBytes.length);
    outputSerializer.write(instantBytes);

    // Convert UUID string to two longs and write
    String fileId = location.getFileId();
    final int fileIndex;
    final UUID uuid;
    if (fileId.length() == 36) {
      uuid = UUID.fromString(fileId);
      fileIndex = RECORD_INDEX_MISSING_FILEINDEX_FALLBACK;
    } else {
      final int index = fileId.lastIndexOf("-");
      uuid = UUID.fromString(fileId.substring(0, index));
      fileIndex = Integer.parseInt(fileId.substring(index + 1));
    }

    outputSerializer.writeLong(uuid.getMostSignificantBits());
    outputSerializer.writeLong(uuid.getLeastSignificantBits());
    outputSerializer.writeInt(fileIndex);

    return outputSerializer.getCopyOfBuffer();
  }

  @Override
  public HoodieRecordGlobalLocation deserialize(byte[] bytes) {
    try {
      // Reset input buffer with new data
      inputDeserializer.setBuffer(bytes);

      // Read partitionPath
      int partitionLen = inputDeserializer.readInt();
      byte[] partitionBytes = new byte[partitionLen];
      inputDeserializer.readFully(partitionBytes);
      String partitionPath = new String(partitionBytes, StandardCharsets.UTF_8);

      // Read instantTime
      int instantLen = inputDeserializer.readInt();
      byte[] instantBytes = new byte[instantLen];
      inputDeserializer.readFully(instantBytes);
      String instantTime = new String(instantBytes, StandardCharsets.UTF_8);

      // Read UUID as two longs and convert back to string
      long fileIdHighBits = inputDeserializer.readLong();
      long fileIdLowBits = inputDeserializer.readLong();
      int fileIndex = inputDeserializer.readInt();
      UUID uuid = new UUID(fileIdHighBits, fileIdLowBits);
      String fileId = uuid.toString();
      if (fileIndex != RECORD_INDEX_MISSING_FILEINDEX_FALLBACK) {
        fileId += "-" + fileIndex;
      }

      return new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize HoodieRecordGlobalLocation", e);
    }
  }
}
