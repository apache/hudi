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

import org.apache.hudi.io.compress.CompressionCodec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.hudi.io.hfile.DataSize.MAGIC_LENGTH;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_BYTE;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT32;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT64;
import static org.apache.hudi.io.util.IOUtils.readInt;

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
  protected final byte[] byteBuff;
  protected final int startOffsetInBuff;
  protected final int sizeCheckSum;
  protected final int uncompressedEndOffset;
  private final HFileBlockType blockType;
  protected final int onDiskSizeWithoutHeader;
  protected final int uncompressedSizeWithoutHeader;
  protected final int bytesPerChecksum;
  private boolean isUnpacked = false;
  protected byte[] compressedByteBuff;
  protected int startOffsetInCompressedBuff;

  protected HFileBlock(HFileContext context,
                       HFileBlockType blockType,
                       byte[] byteBuff,
                       int startOffsetInBuff) {
    this.context = context;
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = readInt(
        byteBuff, startOffsetInBuff + Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    this.uncompressedSizeWithoutHeader = readInt(
        byteBuff, startOffsetInBuff + Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX);
    this.bytesPerChecksum = readInt(
        byteBuff, startOffsetInBuff + Header.BYTES_PER_CHECKSUM_INDEX);
    this.sizeCheckSum = numChecksumBytes(getOnDiskSizeWithHeader(), bytesPerChecksum);
    if (CompressionCodec.NONE.equals(context.getCompressionCodec())) {
      isUnpacked = true;
      this.startOffsetInBuff = startOffsetInBuff;
      this.byteBuff = byteBuff;
    } else {
      this.startOffsetInCompressedBuff = startOffsetInBuff;
      this.compressedByteBuff = byteBuff;
      this.startOffsetInBuff = 0;
      this.byteBuff = allocateBufferForUnpacking();
    }
    this.uncompressedEndOffset =
        this.startOffsetInBuff + HFILEBLOCK_HEADER_SIZE + uncompressedSizeWithoutHeader;
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

  /**
   * Returns the number of bytes needed to store the checksums based on data size.
   *
   * @param numBytes         number of bytes of data.
   * @param bytesPerChecksum number of bytes covered by one checksum.
   * @return the number of bytes needed to store the checksum values.
   */
  static int numChecksumBytes(long numBytes, int bytesPerChecksum) {
    return numChecksumChunks(numBytes, bytesPerChecksum) * HFileBlock.CHECKSUM_SIZE;
  }

  /**
   * Returns the number of checksum chunks needed to store the checksums based on data size.
   *
   * @param numBytes         number of bytes of data.
   * @param bytesPerChecksum number of bytes in a checksum chunk.
   * @return the number of checksum chunks.
   */
  static int numChecksumChunks(long numBytes, int bytesPerChecksum) {
    long numChunks = numBytes / bytesPerChecksum;
    if (numBytes % bytesPerChecksum != 0) {
      numChunks++;
    }
    if (numChunks > Integer.MAX_VALUE / HFileBlock.CHECKSUM_SIZE) {
      throw new IllegalArgumentException("The number of chunks is too large: " + numChunks);
    }
    return (int) numChunks;
  }

  public HFileBlockType getBlockType() {
    return blockType;
  }

  public byte[] getByteBuff() {
    return byteBuff;
  }

  public int getOnDiskSizeWithHeader() {
    return onDiskSizeWithoutHeader + HFILEBLOCK_HEADER_SIZE;
  }

  /**
   * Decodes and decompresses the block content if the block content is compressed.
   * <p>
   * This must be called for an encoded and compressed block before any reads.
   *
   * @throws IOException upon decoding and decompression error.
   */
  public void unpack() throws IOException {
    if (!isUnpacked) {
      // Should only be called for compressed blocks
      CompressionCodec compression = context.getCompressionCodec();
      if (compression != CompressionCodec.NONE) {
        // Copy the block header which is not compressed
        System.arraycopy(
            compressedByteBuff, startOffsetInCompressedBuff, byteBuff, 0, HFILEBLOCK_HEADER_SIZE);
        try (InputStream byteBuffInputStream = new ByteArrayInputStream(
            compressedByteBuff, startOffsetInCompressedBuff + HFILEBLOCK_HEADER_SIZE, onDiskSizeWithoutHeader)) {
          context.getDecompressor().decompress(
              byteBuffInputStream,
              byteBuff,
              HFILEBLOCK_HEADER_SIZE,
              byteBuff.length - HFILEBLOCK_HEADER_SIZE);
        }
      }
      isUnpacked = true;
    }
  }

  /**
   * Allocates new byte buffer for the uncompressed bytes.
   *
   * @return a new byte array based on the size of uncompressed data, holding the same header
   * bytes.
   */
  protected byte[] allocateBufferForUnpacking() {
    int capacity = HFILEBLOCK_HEADER_SIZE + uncompressedSizeWithoutHeader + sizeCheckSum;
    return new byte[capacity];
  }
}
