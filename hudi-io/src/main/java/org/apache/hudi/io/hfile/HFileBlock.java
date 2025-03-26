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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.compress.airlift.HoodieAirliftGzipDecompressor;

import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hudi.io.compress.CompressionCodec.GZIP;
import static org.apache.hudi.io.hfile.DataSize.MAGIC_LENGTH;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_BYTE;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT32;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT64;
import static org.apache.hudi.io.hfile.HFileBlockWriteAttributes.CHECKSUM_TYPE;
import static org.apache.hudi.io.hfile.HFileBlockWriteAttributes.DEFAULT_BYTES_PER_CHECKSUM;
import static org.apache.hudi.io.hfile.HFileBlockWriteAttributes.EMPTY_BYTE_ARRAY;

/**
 * Represents a block in a HFile. The types of blocks are defined in {@link HFileBlockType}.
 */
public abstract class HFileBlock {
  // The HFile block header size without checksum
  public static final int HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM =
      MAGIC_LENGTH + 2 * SIZEOF_INT32 + SIZEOF_INT64;
  // The HFile block header size with checksum
  // There is a 1 byte checksum type, followed by a 4 byte bytesPerChecksum
  // followed by another 4 byte value to store sizeofDataOnDisk.
  public static final int HFILEBLOCK_HEADER_SIZE =
      HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM + SIZEOF_BYTE + 2 * SIZEOF_INT32;

  // Each checksum value is an integer that can be stored in 4 bytes.
  static final int CHECKSUM_SIZE = SIZEOF_INT32;

  static class Header {
    // Format of header is:
    // 8 bytes - block magic
    // 4 bytes int - onDiskSizeWithoutHeader
    // 4 bytes int - uncompressedSizeWithoutHeader
    // 8 bytes long - prevBlockOffset
    // The following 3 are only present if header contains checksum information
    // (which are present for HFile version 3)
    // 1 byte - checksum type
    // 4 byte int - bytes per checksum
    // 4 byte int - onDiskDataSizeWithHeader
    static int BLOCK_MAGIC_INDEX = 0;
    static int ON_DISK_SIZE_WITHOUT_HEADER_INDEX = 8;
    static int UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX = 12;
    static int PREV_BLOCK_OFFSET_INDEX = 16;
    static int CHECKSUM_TYPE_INDEX = 24;
    static int BYTES_PER_CHECKSUM_INDEX = 25;
    static int ON_DISK_DATA_SIZE_WITH_HEADER_INDEX = 29;
  }

  protected final HFileContext context;
  private final HFileBlockType blockType;

  protected Option<HFileBlockReadAttributes> readAttributesOpt;
  protected Option<HFileBlockWriteAttributes> writeAttributesOpt;

  /**
   * Initialize HFileBlock for read.
   */
  protected HFileBlock(HFileContext context,
                       HFileBlockType blockType,
                       byte[] byteBuff,
                       int startOffsetInBuff) {
    this.context = context;
    this.blockType = blockType;
    HFileBlockReadAttributes readAttributes =
        new HFileBlockReadAttributes(this.context, byteBuff, startOffsetInBuff);
    this.readAttributesOpt = Option.of(readAttributes);
  }

  /**
   * Initialize HFileBlock for write.
   */
  protected HFileBlock(HFileContext context,
                       HFileBlockType blockType,
                       long previousBlockOffset) {
    this.context = context;
    this.blockType = blockType;
    HFileBlockWriteAttributes writeAttributes = new HFileBlockWriteAttributes.Builder()
        .blockSize(context.getBlockSize())
        .previousBlockOffset(previousBlockOffset)
        .build();
    writeAttributesOpt = Option.of(writeAttributes);
  }

  /**
   * Parses the HFile block header and returns the {@link HFileBlock} instance based on the input.
   *
   * @param context           HFile context.
   * @param byteBuff          input data.
   * @param startOffsetInBuff offset to start parsing.
   * @return the {@link HFileBlock} instance based on the input.
   * @throws IOException if the block cannot be parsed.
   */
  public static HFileBlock parse(HFileContext context, byte[] byteBuff, int startOffsetInBuff)
      throws IOException {
    HFileBlockType blockType = HFileBlockType.parse(byteBuff, startOffsetInBuff);
    switch (blockType) {
      case ROOT_INDEX:
        return new HFileRootIndexBlock(context, byteBuff, startOffsetInBuff);
      case LEAF_INDEX:
        return new HFileLeafIndexBlock(context, byteBuff, startOffsetInBuff);
      case INTERMEDIATE_INDEX:
        return new HFileIntermediateIndexBlock(context, byteBuff, startOffsetInBuff);
      case FILE_INFO:
        return new HFileFileInfoBlock(context, byteBuff, startOffsetInBuff);
      case DATA:
        return new HFileDataBlock(context, byteBuff, startOffsetInBuff);
      case META:
        return new HFileMetaBlock(context, byteBuff, startOffsetInBuff);
      default:
        throw new IOException(
            "Parsing of the HFile block type " + blockType + " is not supported");
    }
  }

  public HFileBlockType getBlockType() {
    return blockType;
  }

  public byte[] getByteBuff() {
    return readAttributesOpt.get().byteBuff;
  }

  public int getOnDiskSizeWithHeader() {
    return readAttributesOpt.get().onDiskSizeWithoutHeader + HFILEBLOCK_HEADER_SIZE;
  }

  /**
   * Decodes and decompresses the block content if the block content is compressed.
   * <p>
   * This must be called for an encoded and compressed block before any reads.
   *
   * @throws IOException upon decoding and decompression error.
   */
  public void unpack() throws IOException {
    if (!readAttributesOpt.get().isUnpacked) {
      // Should only be called for compressed blocks
      CompressionCodec compression = context.getCompressionCodec();
      if (compression != CompressionCodec.NONE) {
        // Copy the block header which is not compressed
        System.arraycopy(
            readAttributesOpt.get().compressedByteBuff,
            readAttributesOpt.get().startOffsetInCompressedBuff,
            readAttributesOpt.get().byteBuff,
            0,
            HFILEBLOCK_HEADER_SIZE);
        try (InputStream byteBuffInputStream = new ByteArrayInputStream(
            readAttributesOpt.get().compressedByteBuff,
            readAttributesOpt.get().startOffsetInCompressedBuff + HFILEBLOCK_HEADER_SIZE,
            readAttributesOpt.get().onDiskSizeWithoutHeader)) {
          context.getDecompressor().decompress(
              byteBuffInputStream,
              readAttributesOpt.get().byteBuff,
              HFILEBLOCK_HEADER_SIZE,
              readAttributesOpt.get().byteBuff.length - HFILEBLOCK_HEADER_SIZE);
        }
      }
      readAttributesOpt.get().isUnpacked = true;
    }
  }

  // ================ Below are for Write ================

  /**
   * Returns serialized "data" part of the block.
   * This function should be implemented by each block type separately.
   * By default, it does nothing.
   */
  public ByteBuffer getPayload() {
    return ByteBuffer.allocate(0);
  }

  /**
   * Return serialized block including header, data, checksum.
   */
  public ByteBuffer serialize() throws IOException {
    // Block payload.
    ByteBuffer payloadBuff = getPayload();
    // Compress if specified.
    ByteBuffer compressedPayload = compress(payloadBuff);
    // Buffer for building block.
    ByteBuffer buf = ByteBuffer.allocate(Math.max(
        context.getBlockSize() * 2,
        compressedPayload.limit() + HFILEBLOCK_HEADER_SIZE * 2));
    // Block header
    // 1. Magic is always 8 bytes.
    buf.put(blockType.getMagic(), 0, 8);
    // 2. onDiskSizeWithoutHeader.
    buf.putInt(compressedPayload.limit());
    // 3. uncompressedSizeWithoutHeader.
    buf.putInt(payloadBuff.limit());
    // 4. previous block offset.
    buf.putLong(writeAttributesOpt.get().previousBlockOffset);
    // TODO: set checksum type properly.
    // 5. checksum type.
    buf.put(CHECKSUM_TYPE.getCode());
    // TODO: verify that if hudi uses 4 bytes for checksum always.
    // 6. bytes covered per checksum.
    buf.putInt(DEFAULT_BYTES_PER_CHECKSUM);
    // 7. onDiskDataSizeWithHeader
    int onDiskDataSizeWithHeader =
        HFileBlock.HFILEBLOCK_HEADER_SIZE + payloadBuff.limit();
    buf.putInt(onDiskDataSizeWithHeader);
    // 8. payload.
    buf.put(compressedPayload);
    // 9. Checksum
    buf.put(calcChecksumBytes(CHECKSUM_TYPE));

    // Update sizes
    buf.flip();
    return buf;
  }

  protected ByteBuffer compress(ByteBuffer payload) throws IOException {
    if (context.getCompressionCodec() == GZIP) {
      byte[] temp = new byte[payload.remaining()];
      payload.get(temp);
      return ByteBuffer.wrap(new HoodieAirliftGzipDecompressor().compress(temp));
    } else {
      return payload;
    }
  }

  // TODO: support non-NULL checksum types.
  /**
   * Returns checksum bytes if checksum type is not NULL.
   */
  private byte[] calcChecksumBytes(ChecksumType type) {
    if (type == ChecksumType.NULL) {
      return EMPTY_BYTE_ARRAY;
    } else if (type == ChecksumType.CRC32) {
      return EMPTY_BYTE_ARRAY;
    } else {
      return EMPTY_BYTE_ARRAY;
    }
  }

  /**
   * Sets start offset of the block in the buffer.
   */
  public void setStartOffsetInBuff(long startOffsetInBuff) {
    this.writeAttributesOpt.get().startOffsetInBuff = startOffsetInBuff;
  }

  /**
   * Gets start offset of the block in the buffer.
   */
  public long getStartOffsetInBuff() {
    return this.writeAttributesOpt.get().startOffsetInBuff;
  }

  /**
   * Returns the number of bytes that should be used by checksum.
   * @param onDiskBlockBytesWithHeaderSize
   * @param bytesPerChecksum
   * @return
   */
  private long calcNumChecksumBytes(int onDiskBlockBytesWithHeaderSize, int bytesPerChecksum) {
    return numBytes(onDiskBlockBytesWithHeaderSize, bytesPerChecksum);
  }

  /**
   * Returns the number of bytes needed to store the checksums for a specified data size
   * @param datasize         number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of bytes needed to store the checksum values
   */
  static long numBytes(long datasize, int bytesPerChecksum) {
    return numChunks(datasize, bytesPerChecksum) * HFileBlock.CHECKSUM_SIZE;
  }

  /**
   * Returns the number of checksum chunks needed to store the checksums for a specified data size
   * @param datasize         number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of checksum chunks
   */
  static long numChunks(long datasize, int bytesPerChecksum) {
    long numChunks = datasize / bytesPerChecksum;
    if (datasize % bytesPerChecksum != 0) {
      numChunks++;
    }
    return numChunks;
  }

  static byte[] getVariableLengthEncodes(int length) throws IOException {
    ByteArrayOutputStream varintBuffer = new ByteArrayOutputStream();
    CodedOutputStream varintOutput = CodedOutputStream.newInstance(varintBuffer);
    varintOutput.writeUInt32NoTag(length);
    varintOutput.flush();
    return varintBuffer.toByteArray();
  }
}
