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
import org.apache.hudi.common.util.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.hfile.protobuf.generated.HFileProtos;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure Java implementation of HFile writer (HFile v3 format) for Hudi.
 *
 * TODO: Known improvements to be done:
 *   1. fully compatible with Hbase reader.
 *   2. support given compression codec.
 */
public class HFileWriterImpl implements HFileWriter {
  private final OutputStream outputStream;
  private final HFileContext context;
  // Flushed data blocks.
  private final List<HFileDataBlock> dataBlocks = new ArrayList<>();
  // Meta Info map.
  private final Map<String, byte[]> metaInfo = new HashMap<>();
  // Data block under construction.
  private HFileDataBlock currentDataBlock;
  // Meta block under construction.
  private final HFileRootIndexBlock rootIndexBlock;
  private final HFileMetaIndexBlock metaIndexBlock;
  private final HFileFileInfoBlock fileInfoBlock;
  private long uncompressedBytes;
  private long totalUncompressedBytes;
  private long currentOffset;
  private long loadOnOpenSectionOffset;
  private final int blockSize;

  public HFileWriterImpl(HFileContext context, OutputStream outputStream) {
    this.outputStream = outputStream;
    this.context = context;
    this.blockSize = this.context.getBlockSize();
    this.uncompressedBytes = 0L;
    this.totalUncompressedBytes = 0L;
    this.currentOffset = 0L;
    this.currentDataBlock = new HFileDataBlock(context);
    this.rootIndexBlock = new HFileRootIndexBlock(context);
    this.metaIndexBlock = new HFileMetaIndexBlock(context);
    this.fileInfoBlock = new HFileFileInfoBlock(context);
    initFileInfo();
  }

  // Append a data kv pair.
  public void append(String key, byte[] value) throws IOException {
    byte[] keyBytes = StringUtils.getUTF8Bytes(key);
    if (uncompressedBytes + keyBytes.length + value.length + 9 > blockSize) {
      flushCurrentDataBlock();
      uncompressedBytes = 0;
    }
    currentDataBlock.add(keyBytes, value);
    int uncompressedKeyValueSize = keyBytes.length + value.length;
    uncompressedBytes += uncompressedKeyValueSize + 9;
    totalUncompressedBytes += uncompressedKeyValueSize + 9;
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
    if (currentDataBlock.isEmpty()) {
      return;
    }
    // Flush data block.
    ByteBuffer blockBuffer = currentDataBlock.serialize();
    // Current block offset.
    currentDataBlock.setStartOffsetInBuff(currentOffset);
    long blockOffset = currentOffset;
    writeBuffer(blockBuffer);
    dataBlocks.add(currentDataBlock);
    // Create an index entry.
    rootIndexBlock.add(
        currentDataBlock.getFirstKey(), blockOffset, blockBuffer.limit());
    // Create a new data block.
    currentDataBlock = new HFileDataBlock(context);
  }

  // NOTE that: reader assumes that every meta info piece
  // should be a separate meta block.
  private void flushMetaBlocks() throws IOException {
    for (Map.Entry<String, byte[]> e : metaInfo.entrySet()) {
      HFileMetaBlock currentMetaBlock = new HFileMetaBlock(context);
      byte[] key = StringUtils.getUTF8Bytes(e.getKey());
      currentMetaBlock.add(key, e.getValue());
      ByteBuffer blockBuffer = currentMetaBlock.serialize();
      long blockOffset = currentOffset;
      currentMetaBlock.setStartOffsetInBuff(currentOffset);
      writeBuffer(blockBuffer);
      metaIndexBlock.add(
          currentMetaBlock.getFirstKey(), blockOffset, blockBuffer.limit());
    }
  }

  private void writeLoadOnOpenSection() throws IOException {
    loadOnOpenSectionOffset = currentOffset;
    // Write Root Data Index
    ByteBuffer dataIndexBuffer = rootIndexBlock.serialize();
    rootIndexBlock.setStartOffsetInBuff(currentOffset);
    writeBuffer(dataIndexBuffer);
    // Write Meta Data Index.
    // Note: Even this block is empty, it has to be there
    //  due to the behavior of the reader.
    ByteBuffer metaIndexBuffer = metaIndexBlock.serialize();
    metaIndexBlock.setStartOffsetInBuff(currentOffset);
    writeBuffer(metaIndexBuffer);
    // Write File Info.
    if (!dataBlocks.isEmpty()) {
      fileInfoBlock.add(
          "hfile.LASTKEY", dataBlocks.get(dataBlocks.size() - 1).getLastKey());
    } else {
      fileInfoBlock.add(
          "hfile.LASTKEY", new byte[0]);
    }
    fileInfoBlock.setStartOffsetInBuff(currentOffset);
    writeBuffer(fileInfoBlock.serialize());
  }

  private void writeTrailer() throws IOException {
    HFileProtos.TrailerProto.Builder builder = HFileProtos.TrailerProto.newBuilder();
    builder.setFileInfoOffset(fileInfoBlock.getStartOffsetInBuff());
    builder.setLoadOnOpenDataOffset(loadOnOpenSectionOffset);
    builder.setUncompressedDataIndexSize(totalUncompressedBytes);
    builder.setDataIndexCount(rootIndexBlock.getNumOfEntries());
    builder.setMetaIndexCount(metaIndexBlock.getNumOfEntries());
    builder.setEntryCount(getTotalNumOfEntries());
    // TODO: support multiple levels.
    builder.setNumDataIndexLevels(1);
    if (!dataBlocks.isEmpty()) {
      builder.setFirstDataBlockOffset(dataBlocks.get(0).getStartOffsetInBuff());
      builder.setLastDataBlockOffset(dataBlocks.get(dataBlocks.size() - 1).getStartOffsetInBuff());
    }
    builder.setComparatorClassName("NA");
    builder.setCompressionCodec(2);
    // TODO: support compression.
    builder.setEncryptionKey(ByteString.EMPTY);
    HFileProtos.TrailerProto trailerProto = builder.build();

    // Encode the varint size into a ByteBuffer
    // This is necessary to make the parsing work.
    ByteArrayOutputStream varintBuffer = new ByteArrayOutputStream();
    CodedOutputStream varintOutput = CodedOutputStream.newInstance(varintBuffer);
    varintOutput.writeUInt32NoTag(trailerProto.getSerializedSize());
    varintOutput.flush();

    ByteBuffer trailer = ByteBuffer.allocate(4096);
    trailer.limit(4096);
    trailer.put(StringUtils.getUTF8Bytes("TRABLK\"$"));
    trailer.put(varintBuffer.toByteArray());
    trailer.put(trailerProto.toByteArray());
    // Force trailer to have fixed length.
    trailer.position(4095);
    trailer.put((byte)3);

    trailer.flip();
    writeBuffer(trailer);
  }

  private long getTotalNumOfEntries() {
    long totalNumOfEntries = 0L;
    for (HFileDataBlock db : dataBlocks) {
      totalNumOfEntries += db.getNumOfEntries();
    }
    return totalNumOfEntries;
  }

  private void writeBuffer(ByteBuffer buffer) throws IOException {
    // Note that: Use `write(byte[], off, len)`, instead of `write(byte[])`.
    outputStream.write(buffer.array(), 0, buffer.limit());
    currentOffset += buffer.limit();
  }

  private void initFileInfo() {
    fileInfoBlock.add("hfile.MAX_MEMSTORE_TS_KEY", new byte[]{0});
  }

  // Example to demonstrate the code is runnable.
  public static void main(String[] args) throws Exception {
    String fileName = "test.hfile";
    HFileContext context = HFileContext.builder().build();
    try (OutputStream outputStream = new DataOutputStream(
        Files.newOutputStream(Paths.get(fileName)));
         HFileWriterImpl writer = new HFileWriterImpl(context, outputStream)) {
      writer.append("key1", "value1".getBytes());
      writer.append("key2", "value2".getBytes());
    }

    Path file = Paths.get("test.hfile");
    byte[] content = Files.readAllBytes(file);
    try (HFileReader reader = new HFileReaderImpl(
        new ByteArraySeekableDataInputStream(
            new ByteBufferBackedInputStream(content)), content.length)) {
      reader.initializeMetadata();
      reader.getNumKeyValueEntries();
    }
  }
}



