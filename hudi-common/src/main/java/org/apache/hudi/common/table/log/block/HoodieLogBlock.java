/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.log.block;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypeUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataInputStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Abstract class defining a block in HoodieLogFile.
 */
public abstract class HoodieLogBlock {

  /**
   * The current version of the log block. Anytime the logBlock format changes this version needs to be bumped and
   * corresponding changes need to be made to {@link HoodieLogBlockVersion} TODO : Change this to a class, something
   * like HoodieLogBlockVersionV1/V2 and implement/override operations there
   */
  public static int version = 2;
  // Header for each log block
  private final Map<HeaderMetadataType, String> logBlockHeader;
  // Footer for each log block
  private final Map<HeaderMetadataType, String> logBlockFooter;
  // Location of a log block on disk
  private final Option<HoodieLogBlockContentLocation> blockContentLocation;
  // data for a specific block
  private Option<byte[]> content;
  // TODO : change this to just InputStream so this works for any FileSystem
  // create handlers to return specific type of inputstream based on FS
  // input stream corresponding to the log file where this logBlock belongs
  private final FSDataInputStream inputStream;
  // Toggle flag, whether to read blocks lazily (I/O intensive) or not (Memory intensive)
  protected boolean readBlockLazily;

  public HoodieLogBlock(
      @Nonnull Map<HeaderMetadataType, String> logBlockHeader,
      @Nonnull Map<HeaderMetadataType, String> logBlockFooter,
      @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation,
      @Nonnull Option<byte[]> content,
      @Nullable FSDataInputStream inputStream,
      boolean readBlockLazily) {
    this.logBlockHeader = logBlockHeader;
    this.logBlockFooter = logBlockFooter;
    this.blockContentLocation = blockContentLocation;
    this.content = content;
    this.inputStream = inputStream;
    this.readBlockLazily = readBlockLazily;
  }

  // Return the bytes representation of the data belonging to a LogBlock
  public byte[] getContentBytes() throws IOException {
    throw new HoodieException("No implementation was provided");
  }

  public byte[] getMagic() {
    throw new HoodieException("No implementation was provided");
  }

  public abstract HoodieLogBlockType getBlockType();

  public long getLogBlockLength() {
    throw new HoodieException("No implementation was provided");
  }

  public Option<HoodieLogBlockContentLocation> getBlockContentLocation() {
    return this.blockContentLocation;
  }

  public Map<HeaderMetadataType, String> getLogBlockHeader() {
    return logBlockHeader;
  }

  public Map<HeaderMetadataType, String> getLogBlockFooter() {
    return logBlockFooter;
  }

  public Option<byte[]> getContent() {
    return content;
  }

  /**
   * Type of the log block WARNING: This enum is serialized as the ordinal. Only add new enums at the end.
   */
  public enum HoodieLogBlockType {
    COMMAND_BLOCK(":command"),
    DELETE_BLOCK(":delete"),
    CORRUPT_BLOCK(":corrupted"),
    AVRO_DATA_BLOCK("avro"),
    HFILE_DATA_BLOCK("hfile"),
    PARQUET_DATA_BLOCK("parquet"),
    CDC_DATA_BLOCK("cdc");

    private static final Map<String, HoodieLogBlockType> ID_TO_ENUM_MAP =
        TypeUtils.getValueToEnumMap(HoodieLogBlockType.class, e -> e.id);

    private final String id;

    HoodieLogBlockType(String id) {
      this.id = id;
    }

    public static HoodieLogBlockType fromId(String id) {
      return ID_TO_ENUM_MAP.get(id);
    }
  }

  /**
   * Log Metadata headers abstraction for a HoodieLogBlock WARNING : This enum is serialized as the ordinal. Only add
   * new enums at the end.
   */
  public enum HeaderMetadataType {
    INSTANT_TIME, TARGET_INSTANT_TIME, SCHEMA, COMMAND_BLOCK_TYPE
  }

  /**
   * Log Metadata footers abstraction for a HoodieLogBlock WARNING : This enum is serialized as the ordinal. Only add
   * new enums at the end.
   */
  public enum FooterMetadataType {
  }

  /**
   * This class is used to store the Location of the Content of a Log Block. It's used when a client chooses for a IO
   * intensive CompactedScanner, the location helps to lazily read contents from the log file
   */
  public static final class HoodieLogBlockContentLocation {
    // Hadoop Config required to access the file
    private final Configuration hadoopConf;
    // The logFile that contains this block
    private final HoodieLogFile logFile;
    // The filePosition in the logFile for the contents of this block
    private final long contentPositionInLogFile;
    // The number of bytes / size of the contents of this block
    private final long blockSize;
    // The final position where the complete block ends
    private final long blockEndPos;

    public HoodieLogBlockContentLocation(Configuration hadoopConf,
                                         HoodieLogFile logFile,
                                         long contentPositionInLogFile,
                                         long blockSize,
                                         long blockEndPos) {
      this.hadoopConf = hadoopConf;
      this.logFile = logFile;
      this.contentPositionInLogFile = contentPositionInLogFile;
      this.blockSize = blockSize;
      this.blockEndPos = blockEndPos;
    }

    public Configuration getHadoopConf() {
      return hadoopConf;
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

    public long getBlockEndPos() {
      return blockEndPos;
    }
  }

  /**
   * Convert log metadata to bytes 1. Write size of metadata 2. Write enum ordinal 3. Write actual bytes
   */
  public static byte[] getLogMetadataBytes(Map<HeaderMetadataType, String> metadata) throws IOException {
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
   * Convert bytes to LogMetadata, follow the same order as {@link HoodieLogBlock#getLogMetadataBytes}.
   */
  public static Map<HeaderMetadataType, String> getLogMetadata(DataInputStream dis) throws IOException {

    Map<HeaderMetadataType, String> metadata = new HashMap<>();
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

  /**
   * Read or Skip block content of a log block in the log file. Depends on lazy reading enabled in
   * {@link HoodieMergedLogRecordScanner}
   */
  public static Option<byte[]> tryReadContent(FSDataInputStream inputStream, Integer contentLength, boolean readLazily)
      throws IOException {
    if (readLazily) {
      // Seek to the end of the content block
      inputStream.seek(inputStream.getPos() + contentLength);
      return Option.empty();
    }

    // TODO re-use buffer if stream is backed by buffer
    // Read the contents in memory
    byte[] content = new byte[contentLength];
    inputStream.readFully(content, 0, contentLength);
    return Option.of(content);
  }

  /**
   * When lazyReading of blocks is turned on, inflate the content of a log block from disk.
   */
  protected void inflate() throws HoodieIOException {
    checkState(!content.isPresent(), "Block has already been inflated");
    checkState(inputStream != null, "Block should have input-stream provided");

    try {
      content = Option.of(new byte[(int) this.getBlockContentLocation().get().getBlockSize()]);
      inputStream.seek(this.getBlockContentLocation().get().getContentPositionInLogFile());
      inputStream.readFully(content.get(), 0, content.get().length);
      inputStream.seek(this.getBlockContentLocation().get().getBlockEndPos());
    } catch (IOException e) {
      // TODO : fs.open() and return inputstream again, need to pass FS configuration
      // because the inputstream might close/timeout for large number of log blocks to be merged
      inflate();
    }
  }

  /**
   * After the content bytes is converted into the required DataStructure by a logBlock, deflate the content to release
   * byte [] and relieve memory pressure when GC kicks in. NOTE: This still leaves the heap fragmented
   */
  protected void deflate() {
    content = Option.empty();
  }
}
