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
import org.apache.hudi.io.hfile.writer.DataBlock;
import org.apache.hudi.io.hfile.writer.FileInfoBlock;
import org.apache.hudi.io.hfile.writer.IndexBlock;
import org.apache.hudi.io.hfile.writer.MetaBlock;
import org.apache.hudi.io.hfile.writer.RootIndexBlock;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure Java implementation of HFile writer (HFile v3 format) for Hudi.
 */
public class HFileWriterImpl implements HFileWriter {
  private final DataOutputStream outputStream;
  private final HFileContext context;
  // Flushed data blocks.
  private final List<DataBlock> dataBlocks = new ArrayList<>();
  // Data block under construction.
  private DataBlock currentDataBlock;
  // Meta block under construction.
  private MetaBlock currentMetaBlock;
  private final IndexBlock rootIndexBlock;
  private final IndexBlock metaIndexBlock;
  private final FileInfoBlock fileInfoBlock;
  private long uncompressedBytes;
  private long totalUncompressedBytes;
  private long currentOffset;
  private long loadOnOpenSectionOffset;
  private final int blockSize;

  public HFileWriterImpl(HFileContext context, DataOutputStream outputStream) {
    this.outputStream = outputStream;
    this.context = context;
    this.blockSize = this.context.getBlockSize();
    this.uncompressedBytes = 0L;
    this.totalUncompressedBytes = 0L;
    this.currentOffset = 0L;
    this.currentDataBlock = new DataBlock(blockSize);
    this.currentMetaBlock = new MetaBlock(blockSize);
    this.rootIndexBlock = new RootIndexBlock(blockSize);
    this.metaIndexBlock = new RootIndexBlock(blockSize);
    this.fileInfoBlock = new FileInfoBlock(blockSize);
    initFileInfo();
  }

  // Append a data kv pair.
  public void append(byte[] key, byte[] value) throws IOException {
    if (uncompressedBytes + key.length + value.length + 9 > blockSize) {
      flushCurrentDataBlock();
      uncompressedBytes = 0;
    }
    currentDataBlock.add(key, value);
    int uncompressedKeyValueSize = key.length + value.length;
    uncompressedBytes += uncompressedKeyValueSize + 9;
    totalUncompressedBytes += uncompressedKeyValueSize + 9;
  }

  // Append a metadata kv pair.
  public void appendMetaInfo(String name, byte[] value) {
    currentMetaBlock.add(
        StringUtils.getUTF8Bytes(name), value);
  }

  // Append a file info kv pair.
  public void appendFileInfo(String name, byte[] value) {
    fileInfoBlock.add(name, value);
  }

  @Override
  public void close() throws IOException {
    flushCurrentDataBlock();
    // TODO: support meta blocks if needed.
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
    currentDataBlock.setBlockOffset(currentOffset);
    long blockOffset = currentOffset;
    writeBuffer(blockBuffer);
    dataBlocks.add(currentDataBlock);

    currentOffset = blockOffset + blockBuffer.limit();
    // Create an index entry.
    rootIndexBlock.add(
        currentDataBlock.getFirstKey(), blockOffset, blockBuffer.limit());
    // Create a new data block.
    currentDataBlock = new DataBlock(blockSize);
  }

  private void writeLoadOnOpenSection() throws IOException {
    loadOnOpenSectionOffset = currentOffset;
    // Write Root Data Index
    ByteBuffer dataIndexBuffer = rootIndexBlock.serialize();
    rootIndexBlock.setBlockOffset(currentOffset);
    writeBuffer(dataIndexBuffer);
    // Write Meta Data Index.
    ByteBuffer metaIndexBuffer = metaIndexBlock.serialize();
    metaIndexBlock.setBlockOffset(currentOffset);
    writeBuffer(metaIndexBuffer);
    // Write File Info.
    if (!dataBlocks.isEmpty()) {
      fileInfoBlock.add(
          "hfile.LASTKEY", dataBlocks.get(dataBlocks.size() - 1).getLastKey());
    } else {
      fileInfoBlock.add(
          "hfile.LASTKEY", new byte[0]);
    }
    fileInfoBlock.setBlockOffset(currentOffset);
    writeBuffer(fileInfoBlock.serialize());
  }

  private void writeTrailer() throws IOException {
    HFileProtos.TrailerProto.Builder builder = HFileProtos.TrailerProto.newBuilder();
    builder.setFileInfoOffset(fileInfoBlock.getBlockOffset());
    builder.setLoadOnOpenDataOffset(loadOnOpenSectionOffset);
    builder.setUncompressedDataIndexSize(totalUncompressedBytes);
    builder.setDataIndexCount(rootIndexBlock.getNumOfEntries());
    builder.setMetaIndexCount(0);
    builder.setEntryCount(getTotalNumOfEntries());
    // TODO: support multiple levels.
    builder.setNumDataIndexLevels(1);
    if (!dataBlocks.isEmpty()) {
      builder.setFirstDataBlockOffset(dataBlocks.get(0).getBlockOffset());
      builder.setLastDataBlockOffset(dataBlocks.get(dataBlocks.size() - 1).getBlockOffset());
    }
    builder.setComparatorClassName("NA");
    builder.setCompressionCodec(2);
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
    trailer.position(4092);
    trailer.putInt(3 << 24);

    trailer.flip();
    writeBuffer(trailer);
  }

  private long getTotalNumOfEntries() {
    long totalNumOfEntries = 0L;
    for (DataBlock db : dataBlocks) {
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
    try (DataOutputStream outputStream = new DataOutputStream(
        Files.newOutputStream(Paths.get(fileName)));
         HFileWriterImpl writer = new HFileWriterImpl(context, outputStream)) {
      writer.append("key1".getBytes(), "value1".getBytes());
      writer.append("key2".getBytes(), "value2".getBytes());
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



