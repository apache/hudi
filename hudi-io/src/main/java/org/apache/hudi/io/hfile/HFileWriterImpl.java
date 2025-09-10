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
import static org.apache.hudi.io.hfile.HFileBlock.getVariableLengthEncodedBytes;
import static org.apache.hudi.io.hfile.HFileBlockType.TRAILER;
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

  // Variables used to record necessary information to reduce
  // the memory usage.
  private byte[] lastKey = new byte[0];
  private long firstDataBlockOffset = -1;
  private long lastDataBlockOffset;
  private long totalNumberOfRecords = 0;
  private long totalKeyLength = 0;
  private long totalValueLength = 0;

  public HFileWriterImpl(HFileContext context, OutputStream outputStream) {
    this.outputStream = outputStream;
    this.context = context;
    this.blockSize = this.context.getBlockSize();
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
    lastKey = keyBytes;
    totalKeyLength += keyBytes.length;
    totalValueLength += value.length;
    // Records with the same key must be put into the same block.
    // Here 9 = 4 bytes of key length + 4 bytes of value length + 1 byte MVCC.
    if (!Arrays.equals(currentDataBlock.getLastKeyContent(), keyBytes)
        && uncompressedDataBlockBytes + keyBytes.length + value.length + 9 > blockSize) {
      flushCurrentDataBlock();
      uncompressedDataBlockBytes = 0;
    }
    currentDataBlock.add(keyBytes, value);
    int uncompressedKeyValueSize = keyBytes.length + value.length;
    uncompressedDataBlockBytes += uncompressedKeyValueSize + 9;
    totalUncompressedDataBlockBytes += uncompressedKeyValueSize + 9;
  }

  // Append a metadata kv pair.
  public void appendMetaInfo(String name, byte[] value) {
    metaInfo.put(name, value);
  }

  // Append a file info kv pair even old value exists.
  public void appendFileInfo(String name, byte[] value) {
    fileInfoBlock.add(name, value);
  }

  // This is mainly used to test the impact of certain file properties.
  public void appendFileInfoIfNotExists(String name, byte[] value) {
    if (!fileInfoBlock.containsKey(name)) {
      fileInfoBlock.add(name, value);
    }
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
    // 4. Create a new data block.
    currentDataBlock = HFileDataBlock.createDataBlockToWrite(context, currentOffset);
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
    trailer.put(getVariableLengthEncodedBytes(trailerProto.getSerializedSize()));
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
        toBytes(context.getFileCreateTime()));

    // Average value length.
    int avgValueLen = totalNumberOfRecords == 0
        ? 0 : (int) (totalValueLength / totalNumberOfRecords);
    fileInfoBlock.add(
        new String(HFileInfo.AVG_VALUE_LEN.getBytes(), StandardCharsets.UTF_8),
        toBytes(avgValueLen));

    // NOTE: To make MVCC usage consistent cross different table versions,
    // we should set following properties.
    // After table versions <= 8 are deprecated, MVCC byte can be removed from key-value pair.
    appendFileInfoIfNotExists(
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
