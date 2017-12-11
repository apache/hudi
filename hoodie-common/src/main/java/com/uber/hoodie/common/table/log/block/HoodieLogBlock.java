/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log.block;

import com.google.common.collect.Maps;
import com.uber.hoodie.exception.HoodieException;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;

/**
 * Abstract class defining a block in HoodieLogFile
 */
public abstract class HoodieLogBlock {

  public byte[] getBytes() throws IOException {
    throw new HoodieException("No implementation was provided");
  }

  public HoodieLogBlockType getBlockType() {
    throw new HoodieException("No implementation was provided");
  }

  //log metadata for each log block
  private Map<LogMetadataType, String> logMetadata;

  /**
   * Type of the log block WARNING: This enum is serialized as the ordinal. Only add new enums at
   * the end.
   */
  public enum HoodieLogBlockType {
    COMMAND_BLOCK,
    DELETE_BLOCK,
    CORRUPT_BLOCK,
    AVRO_DATA_BLOCK
  }

  /**
   * Metadata abstraction for a HoodieLogBlock WARNING : This enum is serialized as the ordinal.
   * Only add new enums at the end.
   */
  public enum LogMetadataType {
    INSTANT_TIME,
    TARGET_INSTANT_TIME
  }

  public HoodieLogBlock(Map<LogMetadataType, String> logMetadata) {
    this.logMetadata = logMetadata;
  }

  public Map<LogMetadataType, String> getLogMetadata() {
    return logMetadata;
  }

  /**
   * Convert log metadata to bytes 1. Write size of metadata 2. Write enum ordinal 3. Write actual
   * bytes
   */
  public static byte[] getLogMetadataBytes(Map<LogMetadataType, String> metadata)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    output.writeInt(metadata.size());
    for (Map.Entry<LogMetadataType, String> entry : metadata.entrySet()) {
      output.writeInt(entry.getKey().ordinal());
      byte[] bytes = entry.getValue().getBytes();
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    return baos.toByteArray();
  }

  /**
   * Convert bytes to LogMetadata, follow the same order as {@link HoodieLogBlock#getLogMetadataBytes}
   */
  public static Map<LogMetadataType, String> getLogMetadata(DataInputStream dis)
      throws IOException {

    Map<LogMetadataType, String> metadata = Maps.newHashMap();
    // 1. Read the metadata written out
    int metadataCount = dis.readInt();
    try {
      while (metadataCount > 0) {
        int metadataEntryIndex = dis.readInt();
        int metadataEntrySize = dis.readInt();
        byte[] metadataEntry = new byte[metadataEntrySize];
        dis.readFully(metadataEntry, 0, metadataEntrySize);
        metadata.put(LogMetadataType.values()[metadataEntryIndex], new String(metadataEntry));
        metadataCount--;
      }
      return metadata;
    } catch (EOFException eof) {
      throw new IOException("Could not read metadata fields ", eof);
    }
  }
}
