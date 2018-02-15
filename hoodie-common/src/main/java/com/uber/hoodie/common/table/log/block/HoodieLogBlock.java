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
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.exception.HoodieException;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract class defining a block in HoodieLogFile
 */
public abstract class HoodieLogBlock {

  // Header for each log block
  private final Map<HeaderMetadataType, String> logBlockHeader;

  // Footer for each log block
  private final Map<HeaderMetadataType, String> logBlockFooter;

  // Location of a log block on disk
  private final Optional<HoodieLogBlockContentLocation> blockLocation;

  public HoodieLogBlock(@Nonnull  Map<HeaderMetadataType, String> logBlockHeader) {
    this(logBlockHeader, Maps.newHashMap(), Optional.empty());
  }

  public HoodieLogBlock(@Nonnull  Map<HeaderMetadataType, String> logBlockHeader,
                        @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
                        @Nonnull Optional<HoodieLogBlockContentLocation> blockLocation) {
    this.logBlockHeader = logBlockHeader;
    this.logBlockFooter = logBlockFooter;
    this.blockLocation = blockLocation;
  }

  // Return the bytes representation of the data belonging to a LogBlock
  public byte[] getContentBytes() throws IOException {
    throw new HoodieException("No implementation was provided");
  }

  public byte [] getMagic() {
    throw new HoodieException("No implementation was provided");
  }

  public HoodieLogBlockType getBlockType() {
    throw new HoodieException("No implementation was provided");
  }

  public long getLogBlockLength() {
    throw new HoodieException("No implementation was provided");
  }

  public Optional<HoodieLogBlockContentLocation> getBlockContentLocation() {
    return this.blockLocation;
  }

  public Map<HeaderMetadataType, String> getLogBlockHeader() {
    return logBlockHeader;
  }

  public Map<HeaderMetadataType, String> getLogBlockFooter() {
    return logBlockFooter;
  }

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
   * Log Metadata headers abstraction for a HoodieLogBlock WARNING : This enum is serialized as the ordinal.
   * Only add new enums at the end.
   */
  public enum HeaderMetadataType {
    INSTANT_TIME,
    TARGET_INSTANT_TIME,
    SCHEMA,
    COMMAND_BLOCK_TYPE
  }

  /**
   * Log Metadata footers abstraction for a HoodieLogBlock WARNING : This enum is serialized as the ordinal.
   * Only add new enums at the end.
   */
  public enum FooterMetadataType {
  }

  /**
   * This class is used to store the Location of the Content of a Log Block. It's used when a client chooses for a
   * IO intensive CompactedScanner, the location helps to lazily read contents from the log file
   */
  public static final class HoodieLogBlockContentLocation {
    // The logFile that contains this block
    private final HoodieLogFile logFile;
    // The filePosition in the logFile for the contents of this block
    private final long contentPositionInLogFile;
    // The number of bytes / size of the contents of this block
    private final long blockSize;

    HoodieLogBlockContentLocation(HoodieLogFile logFile, long contentPositionInLogFile, long blockSize) {
      this.logFile = logFile;
      this.contentPositionInLogFile = contentPositionInLogFile;
      this.blockSize = blockSize;
    }

    public HoodieLogFile getLogFile() {
      return logFile;
    }

    public long getContentPositionInLogFile() {
      return contentPositionInLogFile;
    }

    public long getBlockSize() {
      return blockSize;
    }
  }

  /**
   * Convert log metadata to bytes 1. Write size of metadata 2. Write enum ordinal 3. Write actual
   * bytes
   */
  public static byte[] getLogMetadataBytes(Map<HeaderMetadataType, String> metadata)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    output.writeInt(metadata.size());
    for (Map.Entry<HeaderMetadataType, String> entry : metadata.entrySet()) {
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
  public static Map<HeaderMetadataType, String> getLogMetadata(DataInputStream dis)
      throws IOException {

    Map<HeaderMetadataType, String> metadata = Maps.newHashMap();
    // 1. Read the metadata written out
    int metadataCount = dis.readInt();
    try {
      while (metadataCount > 0) {
        int metadataEntryIndex = dis.readInt();
        int metadataEntrySize = dis.readInt();
        byte[] metadataEntry = new byte[metadataEntrySize];
        dis.readFully(metadataEntry, 0, metadataEntrySize);
        metadata.put(HeaderMetadataType.values()[metadataEntryIndex], new String(metadataEntry));
        metadataCount--;
      }
      return metadata;
    } catch (EOFException eof) {
      throw new IOException("Could not read metadata fields ", eof);
    }
  }
}
