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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.io.hfile.protobuf.generated.HFileProtos;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT16;
import static org.apache.hudi.io.hfile.HFileBlockType.TRAILER;
import static org.apache.hudi.io.hfile.HFileFileInfoBlock.getProtobufVarIntBytes;
import static org.apache.hudi.io.hfile.HFileInfo.KEY_VALUE_VERSION_WITH_MVCC_TS;
import static org.apache.hudi.io.hfile.HFileInfo.LAST_KEY;
import static org.apache.hudi.io.hfile.HFileInfo.MAX_MVCC_TS_KEY;
import static org.apache.hudi.io.hfile.HFileTrailer.TRAILER_SIZE;
import static org.apache.hudi.io.util.IOUtils.toBytes;

/**
 * Pure Java implementation of HFile writer (HFile v3 format) for Hudi.
 */
public class HFileWriterImpl implements HFileWriter {
  private static final String COMPARATOR_CLASS_NAME
      = "org.apache.hudi.io.storage.HoodieHBaseKVComparator";
  private static final byte HFILE_VERSION = (byte) 3;
  // Here 21 = 4 bytes (key length) + 4 bytes (value length) + 1 byte (MVCC)
  //           + 2 bytes (length of key length)
  //           + 10 bytes (check sum, timestamp, key type).
  private static final int EXTRA_BYTES_PER_DATA_ENTRY = 21;
  // Block sizes are 4-byte ints. The 2 MiB headroom covers the header, checksums (< 512 KiB) and
  // deflate growth of incompressible data (< 660 KiB); HFileBlock#serialize checks the exact bound.
  static final long MAX_UNCOMPRESSED_DATA_BLOCK_BYTES = Integer.MAX_VALUE - (2L << 20);
  private final OutputStream outputStream;
  private final HFileContext context;
  // Meta Info map.
  private final Map<String, byte[]> metaInfo = new HashMap<>();
  // Data block under construction.
  private HFileDataBlock currentDataBlock;
  // Meta block under construction.
  private final HFileRootIndexBlock rootIndexBlock;
  private final HFileMetaIndexBlock metaIndexBlock;
  private final HFileFileInfoBlock fileInfoBlock;
  private long uncompressedDataBlockBytes;
  private long totalUncompressedDataBlockBytes;
  private long currentOffset;
  private long loadOnOpenSectionOffset;
  private final int blockSize;
  private final long maxUncompressedDataBlockBytes;

  // Variables used to record necessary information to reduce
  // the memory usage.
  private byte[] lastKey = new byte[0];
  private long firstDataBlockOffset = -1;
  private long lastDataBlockOffset;
  private long totalNumberOfRecords = 0;
  private long totalKeyLength = 0;
  private long totalValueLength = 0;

  public HFileWriterImpl(HFileContext context, OutputStream outputStream) {
    this(context, outputStream, MAX_UNCOMPRESSED_DATA_BLOCK_BYTES);
  }

  /**
   * @param maxUncompressedDataBlockBytes max uncompressed bytes per data block, lowered in tests.
   */
  HFileWriterImpl(HFileContext context, OutputStream outputStream, long maxUncompressedDataBlockBytes) {
    this.outputStream = outputStream;
    this.context = context;
    this.maxUncompressedDataBlockBytes = maxUncompressedDataBlockBytes;
    this.blockSize = (int) Math.min(this.context.getBlockSize(), maxUncompressedDataBlockBytes);
    this.uncompressedDataBlockBytes = 0L;
    this.totalUncompressedDataBlockBytes = 0L;
    this.currentOffset = 0L;
    this.currentDataBlock = HFileDataBlock.createDataBlockToWrite(context, -1L);
    this.rootIndexBlock = HFileRootIndexBlock.createRootIndexBlockToWrite(context);
    this.metaIndexBlock = HFileMetaIndexBlock.createMetaIndexBlockToWrite(context);
    this.fileInfoBlock = HFileFileInfoBlock.createFileInfoBlockToWrite(context);
    initFileInfo();
  }

  // Append a data kv pair.
  public void append(String key, byte[] value) throws IOException {
    byte[] keyBytes = StringUtils.getUTF8Bytes(key);
    long entryBytes = (long) keyBytes.length + value.length + EXTRA_BYTES_PER_DATA_ENTRY;
    // Records with the same key must be put into the same block.
    boolean flushBeforeAppend = !Arrays.equals(currentDataBlock.getLastKeyContent(), keyBytes)
        && uncompressedDataBlockBytes + entryBytes > blockSize;
    // Check before mutating state so a rejected record leaves no trace.
    checkDataBlockCapacity((flushBeforeAppend ? 0 : uncompressedDataBlockBytes) + entryBytes, key);
    lastKey = keyBytes;
    totalKeyLength += keyBytes.length;
    totalValueLength += value.length;
    if (flushBeforeAppend) {
      flushCurrentDataBlock();
      uncompressedDataBlockBytes = 0;
    }
    currentDataBlock.add(keyBytes, value);
    uncompressedDataBlockBytes += entryBytes;
    totalUncompressedDataBlockBytes += entryBytes;
  }

  /**
   * Rejects an append that would grow the block past its capacity, which only an oversized record
   * or a run of same-key records can do.
   */
  private void checkDataBlockCapacity(long uncompressedBytes, String key) {
    if (uncompressedBytes > maxUncompressedDataBlockBytes) {
      throw new IllegalStateException(String.format(
          "Data block would hold %d uncompressed bytes after appending key %s, exceeding the "
              + "maximum of %d bytes: a single record, or records sharing the same key, cannot "
              + "exceed the maximum size of one data block",
          uncompressedBytes, key, maxUncompressedDataBlockBytes));
    }
  }

  // Append a metadata kv pair.
  public void appendMetaInfo(String name, byte[] value) {
    metaInfo.put(name, value);
  }

  // Append a file info kv pair.
  public void appendFileInfo(String name, byte[] value) {
    fileInfoBlock.add(name, value);
  }

  @Override
  public void close() throws IOException {
    flushCurrentDataBlock();
    flushMetaBlocks();
    writeLoadOnOpenSection();
    writeTrailer();
    outputStream.flush();
    outputStream.close();
  }

  private void flushCurrentDataBlock() throws IOException {
    // 0. Skip flush if no data.
    if (currentDataBlock.isEmpty()) {
      return;
    }
    // 1. Update metrics.
    if (firstDataBlockOffset < 0) {
      firstDataBlockOffset = currentOffset;
    }
    lastDataBlockOffset = currentOffset;
    totalNumberOfRecords += currentDataBlock.getNumOfEntries();
    // 2. Flush data block.
    ByteBuffer blockBuffer = currentDataBlock.serialize();
    writeBuffer(blockBuffer);
    // 3. Create an index entry.
    rootIndexBlock.add(
        currentDataBlock.getFirstKey(), lastDataBlockOffset, blockBuffer.limit());
    // 4. Create a new data block whose previous-block offset points at the block just flushed.
    currentDataBlock = HFileDataBlock.createDataBlockToWrite(context, lastDataBlockOffset);
  }

  // NOTE that: reader assumes that every meta info piece
  // should be a separate meta block.
  private void flushMetaBlocks() throws IOException {
    for (Map.Entry<String, byte[]> e : metaInfo.entrySet()) {
      HFileMetaBlock currentMetaBlock =
          HFileMetaBlock.createMetaBlockToWrite(
              context, new KeyValueEntry(StringUtils.getUTF8Bytes(e.getKey()), e.getValue()));
      ByteBuffer blockBuffer = currentMetaBlock.serialize();
      long blockOffset = currentOffset;
      currentMetaBlock.setStartOffsetInBuffForWrite(currentOffset);
      writeBuffer(blockBuffer);
      metaIndexBlock.add(
          currentMetaBlock.getFirstKey(), blockOffset, blockBuffer.limit());
    }
  }

  private void writeLoadOnOpenSection() throws IOException {
    loadOnOpenSectionOffset = currentOffset;
    // Write Root Data Index
    ByteBuffer dataIndexBuffer = rootIndexBlock.serialize();
    rootIndexBlock.setStartOffsetInBuffForWrite(currentOffset);
    writeBuffer(dataIndexBuffer);
    // Write Meta Data Index.
    // Note: Even this block is empty, it has to be there
    //  due to the behavior of the reader.
    ByteBuffer metaIndexBuffer = metaIndexBlock.serialize();
    metaIndexBlock.setStartOffsetInBuffForWrite(currentOffset);
    writeBuffer(metaIndexBuffer);
    // Write File Info.
    finishFileInfo();
    writeBuffer(fileInfoBlock.serialize());
  }

  private void writeTrailer() throws IOException {
    HFileProtos.TrailerProto.Builder builder = HFileProtos.TrailerProto.newBuilder();
    builder.setFileInfoOffset(fileInfoBlock.getStartOffsetInBuffForWrite());
    builder.setLoadOnOpenDataOffset(loadOnOpenSectionOffset);
    builder.setUncompressedDataIndexSize(totalUncompressedDataBlockBytes);
    builder.setDataIndexCount(rootIndexBlock.getNumOfEntries());
    builder.setMetaIndexCount(metaIndexBlock.getNumOfEntries());
    builder.setEntryCount(totalNumberOfRecords);
    // TODO(HUDI-9464): support multiple levels.
    builder.setNumDataIndexLevels(1);
    builder.setFirstDataBlockOffset(firstDataBlockOffset);
    builder.setLastDataBlockOffset(lastDataBlockOffset);
    builder.setComparatorClassName(COMPARATOR_CLASS_NAME);
    builder.setCompressionCodec(context.getCompressionCodec().getId());
    HFileProtos.TrailerProto trailerProto = builder.build();

    ByteBuffer trailer = ByteBuffer.allocate(TRAILER_SIZE);
    trailer.limit(TRAILER_SIZE);
    trailer.put(TRAILER.getMagic());
    trailer.put(getProtobufVarIntBytes(trailerProto.getSerializedSize()));
    trailer.put(trailerProto.toByteArray());
    // Force trailer to have fixed length.
    trailer.position(TRAILER_SIZE - 1);
    trailer.put(HFILE_VERSION);

    trailer.flip();
    writeBuffer(trailer);
  }

  private void writeBuffer(ByteBuffer buffer) throws IOException {
    // Note that: Use `write(byte[], off, len)`, instead of `write(byte[])`.
    outputStream.write(buffer.array(), 0, buffer.limit());
    currentOffset += buffer.limit();
  }

  private void initFileInfo() {
    fileInfoBlock.add(
        new String(MAX_MVCC_TS_KEY.getBytes(), StandardCharsets.UTF_8),
        toBytes(0L));
  }

  protected void finishFileInfo() {
    // Record last key.
    fileInfoBlock.add(
        new String(LAST_KEY.getBytes(), StandardCharsets.UTF_8),
        addKeyLength(lastKey));
    fileInfoBlock.setStartOffsetInBuffForWrite(currentOffset);

    // Average key length.
    int avgKeyLen = totalNumberOfRecords == 0
        ? 0 : (int) (totalKeyLength / totalNumberOfRecords);
    fileInfoBlock.add(
        new String(HFileInfo.AVG_KEY_LEN.getBytes(), StandardCharsets.UTF_8),
        toBytes(avgKeyLen));
    fileInfoBlock.add(
        new String(HFileInfo.FILE_CREATION_TIME_TS.getBytes(), StandardCharsets.UTF_8),
        toBytes(context.getFileCreationTime()));

    // Average value length.
    int avgValueLen = totalNumberOfRecords == 0
        ? 0 : (int) (totalValueLength / totalNumberOfRecords);
    fileInfoBlock.add(
        new String(HFileInfo.AVG_VALUE_LEN.getBytes(), StandardCharsets.UTF_8),
        toBytes(avgValueLen));

    // NOTE: Set this property to make sure the key value and MVCC timestamp
    // pairs are properly decoded
    appendFileInfo(
        new String(HFileInfo.KEY_VALUE_VERSION.getBytes(), StandardCharsets.UTF_8),
        toBytes(KEY_VALUE_VERSION_WITH_MVCC_TS));
  }

  // Note: HFileReaderImpl assumes that:
  //   The last key should contain the content length bytes.
  public byte[] addKeyLength(byte[] key) {
    if (0 == key.length) {
      return new byte[0];
    }
    ByteBuffer byteBuffer = ByteBuffer.allocate(key.length + SIZEOF_INT16);
    byteBuffer.putShort((short) key.length);
    byteBuffer.put(key);
    return byteBuffer.array();
  }
}
