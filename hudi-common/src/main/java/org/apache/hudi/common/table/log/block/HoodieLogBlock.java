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

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.LogReaderUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypeUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;

import lombok.Getter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecordLocation.isPositionValid;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Abstract class defining a block in HoodieLogFile.
 */
public abstract class HoodieLogBlock {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogBlock.class);
  /**
   * The current version of the log block. Anytime the logBlock format changes this version needs to be bumped and
   * corresponding changes need to be made to {@link HoodieLogBlockVersion} TODO : Change this to a class, something
   * like HoodieLogBlockVersionV1/V2 and implement/override operations there
   * Current log block version is V3.
   */
  public static int version = 3;
  // Header for each log block
  @Getter
  private final Map<HeaderMetadataType, String> logBlockHeader;
  // Footer for each log block
  @Getter
  private final Map<FooterMetadataType, String> logBlockFooter;
  // Location of a log block on disk
  @Getter
  private final Option<HoodieLogBlockContentLocation> blockContentLocation;
  // data for a specific block
  @Getter
  private Option<byte[]> content;
  private final Supplier<SeekableDataInputStream> inputStreamSupplier;
  // Toggle flag, whether to read blocks lazily (I/O intensive) or not (Memory intensive)
  protected boolean readBlockLazily;

  public HoodieLogBlock(
      @Nonnull Map<HeaderMetadataType, String> logBlockHeader,
      @Nonnull Map<FooterMetadataType, String> logBlockFooter,
      @Nonnull Option<HoodieLogBlockContentLocation> blockContentLocation,
      @Nonnull Option<byte[]> content,
      @Nullable Supplier<SeekableDataInputStream> inputStreamSupplier,
      boolean readBlockLazily) {
    this.logBlockHeader = logBlockHeader;
    this.logBlockFooter = logBlockFooter;
    this.blockContentLocation = blockContentLocation;
    this.content = content;
    this.inputStreamSupplier = inputStreamSupplier;
    this.readBlockLazily = readBlockLazily;
  }

  // Return the bytes representation of the data belonging to a LogBlock
  public ByteArrayOutputStream getContentBytes(HoodieStorage storage) throws IOException {
    throw new HoodieException("No implementation was provided");
  }

  public byte[] getMagic() {
    throw new HoodieException("No implementation was provided");
  }

  public abstract HoodieLogBlockType getBlockType();

  public boolean isDataOrDeleteBlock() {
    return getBlockType().isDataOrDeleteBlock();
  }

  public long getLogBlockLength() {
    throw new HoodieException("No implementation was provided");
  }

  /**
   * Compacted blocks are created using log compaction which basically merges the consecutive blocks together and create
   * huge block with all the changes.
   */
  public boolean isCompactedLogBlock() {
    return logBlockHeader.containsKey(HeaderMetadataType.COMPACTED_BLOCK_TIMES);
  }

  /**
   * @return A {@link Roaring64NavigableMap} bitmap containing the record positions in long type
   * if the {@link HeaderMetadataType#RECORD_POSITIONS} block header exists; otherwise, an empty
   * {@link Roaring64NavigableMap} bitmap.
   * @throws IOException upon I/O error.
   */
  public Roaring64NavigableMap getRecordPositions() throws IOException {
    if (!logBlockHeader.containsKey(HeaderMetadataType.RECORD_POSITIONS)) {
      return new Roaring64NavigableMap();
    }
    return LogReaderUtils.decodeRecordPositionsHeader(logBlockHeader.get(HeaderMetadataType.RECORD_POSITIONS));
  }

  /**
   * @return base file instant time of the record positions if the record positions are enabled
   * in the log block; {@code null} otherwise.
   */
  public String getBaseFileInstantTimeOfPositions() {
    return logBlockHeader.get(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
  }

  protected void addRecordPositionsToHeader(Set<Long> positionSet,
                                            int numRecords) {
    if (positionSet.size() == numRecords) {
      try {
        logBlockHeader.put(HeaderMetadataType.RECORD_POSITIONS, LogReaderUtils.encodePositions(positionSet));
      } catch (IOException e) {
        LOG.error("Cannot write record positions to the log block header.", e);
      }
    } else {
      LOG.warn("There are duplicate keys in the records (number of unique positions: {}, "
              + "number of records: {}). Skip writing record positions to the log block header.",
          positionSet.size(), numRecords);
    }
  }

  protected boolean containsBaseFileInstantTimeOfPositions() {
    return logBlockHeader.containsKey(
        HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
  }

  protected void removeBaseFileInstantTimeOfPositions() {
    LOG.info("There are records without valid positions. "
        + "Skip writing record positions to the block header.");
    logBlockHeader.remove(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS);
  }

  /**
   * Type of the log block WARNING: This enum is serialized as the ordinal. Only add new enums at the end.
   */
  public enum HoodieLogBlockType {
    COMMAND_BLOCK(":command", HoodieTableVersion.ONE),
    DELETE_BLOCK(":delete", HoodieTableVersion.ONE),
    CORRUPT_BLOCK(":corrupted", HoodieTableVersion.ONE),
    AVRO_DATA_BLOCK("avro", HoodieTableVersion.ONE),
    HFILE_DATA_BLOCK("hfile", HoodieTableVersion.ONE),
    PARQUET_DATA_BLOCK("parquet", HoodieTableVersion.FOUR),
    CDC_DATA_BLOCK("cdc", HoodieTableVersion.SIX);

    private static final Map<String, HoodieLogBlockType> ID_TO_ENUM_MAP =
        TypeUtils.getValueToEnumMap(HoodieLogBlockType.class, e -> e.id);

    private final String id;

    @SuppressWarnings("unused")
    private final HoodieTableVersion earliestTableVersion;

    HoodieLogBlockType(String id, HoodieTableVersion earliestTableVersion) {
      this.id = id;
      this.earliestTableVersion = earliestTableVersion;
    }

    public static HoodieLogBlockType fromId(String id) {
      return ID_TO_ENUM_MAP.get(id);
    }

    /**
     * @returns true if the log block type refers to data or delete block. false otherwise.
     */
    public boolean isDataOrDeleteBlock() {
      return this != HoodieLogBlockType.COMMAND_BLOCK && this != HoodieLogBlockType.CORRUPT_BLOCK;
    }
  }

  /**
   * Log Metadata headers abstraction for a HoodieLogBlock WARNING : This enum is serialized as the ordinal. Only add
   * new enums at the end.
   */
  public enum HeaderMetadataType {
    INSTANT_TIME(HoodieTableVersion.ONE),
    TARGET_INSTANT_TIME(HoodieTableVersion.ONE),
    SCHEMA(HoodieTableVersion.ONE),
    COMMAND_BLOCK_TYPE(HoodieTableVersion.ONE),
    COMPACTED_BLOCK_TIMES(HoodieTableVersion.FIVE),
    RECORD_POSITIONS(HoodieTableVersion.SIX),
    BLOCK_IDENTIFIER(HoodieTableVersion.SIX),
    IS_PARTIAL(HoodieTableVersion.EIGHT),
    BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS(HoodieTableVersion.EIGHT);

    @SuppressWarnings("unused")
    private final HoodieTableVersion earliestTableVersion;

    HeaderMetadataType(HoodieTableVersion version) {
      this.earliestTableVersion = version;
    }
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
  @Getter
  public static final class HoodieLogBlockContentLocation {
    // Storage Config required to access the file
    private final HoodieStorage storage;
    // The logFile that contains this block
    private final HoodieLogFile logFile;
    // The filePosition in the logFile for the contents of this block
    private final long contentPositionInLogFile;
    // The number of bytes / size of the contents of this block
    private final long blockSize;
    // The final position where the complete block ends
    private final long blockEndPos;

    public HoodieLogBlockContentLocation(HoodieStorage storage,
                                         HoodieLogFile logFile,
                                         long contentPositionInLogFile,
                                         long blockSize,
                                         long blockEndPos) {
      this.storage = storage;
      this.logFile = logFile;
      this.contentPositionInLogFile = contentPositionInLogFile;
      this.blockSize = blockSize;
      this.blockEndPos = blockEndPos;
    }

  }

  /**
   * Convert header metadata to bytes 1. Write size of metadata 2. Write enum ordinal 3. Write actual bytes
   */
  public static byte[] getHeaderMetadataBytes(Map<HeaderMetadataType, String> metadata) throws IOException {
    return getLogMetadataBytes(metadata);
  }

  /**
   * Convert bytes to Header Metadata, follow the same order as {@link HoodieLogBlock#getHeaderMetadataBytes}.
   */
  public static Map<HeaderMetadataType, String> getHeaderMetadata(SeekableDataInputStream dis) throws IOException {
    return getLogMetadata(dis, index -> HeaderMetadataType.values()[index]);
  }

  /**
   * Convert footer metadata to bytes 1. Write size of metadata 2. Write enum ordinal 3. Write actual bytes
   */
  public static byte[] getFooterMetadataBytes(Map<FooterMetadataType, String> metadata) throws IOException {
    return getLogMetadataBytes(metadata);
  }

  /**
   * Convert bytes to Footer Metadata, follow the same order as {@link HoodieLogBlock#getFooterMetadataBytes}.
   */
  public static Map<FooterMetadataType, String> getFooterMetadata(SeekableDataInputStream dis) throws IOException {
    return getLogMetadata(dis, index -> FooterMetadataType.values()[index]);
  }

  /**
   * Read or Skip block content of a log block in the log file. Depends on lazy reading enabled in
   * {@link org.apache.hudi.common.table.log.HoodieMergedLogRecordReader}
   */
  public static Option<byte[]> tryReadContent(SeekableDataInputStream inputStream, Integer contentLength, boolean readLazily)
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
   * Return bytes content as a {@link ByteArrayOutputStream}.
   *
   * @return a {@link ByteArrayOutputStream} contains the block content bytes
   */
  protected Option<ByteArrayOutputStream> getContentAsByteStream() throws IOException {
    if (content.isEmpty()) {
      return Option.empty();
    }
    byte[] contentBytes = content.get();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(contentBytes.length);
    baos.write(contentBytes);
    return Option.of(baos);
  }

  protected Supplier<SeekableDataInputStream> getInputStreamSupplier() {
    return inputStreamSupplier;
  }

  /**
   * Adds the record positions if the base file instant time of the positions exists
   * in the log header and the record positions are all valid.
   *
   * @param records         records with valid or invalid positions
   * @param getPositionFunc function to get the position from the record
   * @param <T>             type of record
   */
  protected <T> void addRecordPositionsIfRequired(List<T> records,
                                                  Function<T, Long> getPositionFunc) {
    if (containsBaseFileInstantTimeOfPositions()) {
      if (!isPositionValid(getPositionFunc.apply(records.get(0)))) {
        // Short circuit in case all records do not have valid positions,
        // e.g., BUCKET index cannot identify the record position with low overhead
        removeBaseFileInstantTimeOfPositions();
        return;
      }
      records.sort((o1, o2) -> {
        long v1 = getPositionFunc.apply(o1);
        long v2 = getPositionFunc.apply(o2);
        return Long.compare(v1, v2);
      });
      if (isPositionValid(getPositionFunc.apply(records.get(0)))) {
        addRecordPositionsToHeader(
            records.stream().map(getPositionFunc).collect(Collectors.toSet()),
            records.size());
      } else {
        removeBaseFileInstantTimeOfPositions();
      }
    }
  }

  /**
   * When lazyReading of blocks is turned on, inflate the content of a log block from disk.
   */
  protected void inflate() throws HoodieIOException {
    checkState(!content.isPresent(), "Block has already been inflated");
    checkState(inputStreamSupplier != null, "Block should have input-stream provided");

    try (SeekableDataInputStream inputStream = inputStreamSupplier.get()) {
      content = Option.of(new byte[(int) this.getBlockContentLocation().get().getBlockSize()]);
      inputStream.seek(this.getBlockContentLocation().get().getContentPositionInLogFile());
      inputStream.readFully(content.get(), 0, content.get().length);
      inputStream.seek(this.getBlockContentLocation().get().getBlockEndPos());
    } catch (InterruptedIOException e) {
      // Stop retry inflate if encounters InterruptedIOException
      Thread.currentThread().interrupt();
      throw new HoodieIOException("Thread is interrupted while inflating.", e);
    } catch (IOException e) {
      // TODO : fs.open() and return inputstream again, need to pass FS configuration
      // because the inputstream might close/timeout for large number of log blocks to be merged
      deflate();
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

  /**
   * Converts a given map of log metadata into a byte array representation.
   *
   * The conversion process involves the following steps:
   * 1. Write the size of the metadata map (number of entries).
   * 2. For each entry in the map:
   *    - Write the ordinal of the enum key (to identify the type of metadata).
   *    - Write the length of the value string.
   *    - Write the actual bytes of the value string in UTF-8 encoding.
   *
   * @param metadata A map containing metadata entries, where the key is an enum type representing
   *                 the metadata type and the value is the corresponding string representation.
   * @return A byte array containing the serialized metadata.
   * @throws IOException If an I/O error occurs during the writing process, such as failure to write
   *                     to the underlying output stream.
   */
  private static <T extends Enum<T>> byte[] getLogMetadataBytes(Map<T, String> metadata) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream output = new DataOutputStream(baos);
    output.writeInt(metadata.size());
    for (Map.Entry<T, String> entry : metadata.entrySet()) {
      output.writeInt(entry.getKey().ordinal());
      byte[] bytes = getUTF8Bytes(entry.getValue());
      output.writeInt(bytes.length);
      output.write(bytes);
    }
    return baos.toByteArray();
  }

  /**
   * Convert bytes to Log Metadata, following the same order as {@link HoodieLogBlock#getHeaderMetadataBytes}
   * and {@link HoodieLogBlock#getFooterMetadataBytes}.
   *
   * @param dis The SeekableDataInputStream to read the metadata from.
   * @param typeMapper A function to map the ordinal index to the corresponding metadata type enum.
   * @param <T> The type of the metadata enum (either HeaderMetadataType or FooterMetadataType).
   * @return A Map containing the metadata type as the key and the metadata value as the value.
   * @throws IOException If an I/O error occurs while reading the metadata.
   */
  private static <T> Map<T, String> getLogMetadata(SeekableDataInputStream dis, Function<Integer, T> typeMapper) throws IOException {
    Map<T, String> metadata = new HashMap<>();
    // 1. Read the metadata written out
    int metadataCount = dis.readInt();
    try {
      while (metadataCount > 0) {
        int metadataEntryIndex = dis.readInt();
        int metadataEntrySize = dis.readInt();
        byte[] metadataEntry = new byte[metadataEntrySize];
        dis.readFully(metadataEntry, 0, metadataEntrySize);
        metadata.put(typeMapper.apply(metadataEntryIndex), new String(metadataEntry));
        metadataCount--;
      }
      return metadata;
    } catch (EOFException eof) {
      throw new IOException("Could not read metadata fields ", eof);
    }
  }
}
