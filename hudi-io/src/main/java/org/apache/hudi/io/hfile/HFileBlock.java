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

import org.apache.hudi.io.hfile.compress.Compression;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.apache.hudi.io.hfile.ByteUtils.readInt;
import static org.apache.hudi.io.hfile.DataSize.MAGIC_LENGTH;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_BYTE;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_LONG;

/**
 * Represents a block in a HFile. The types of blocks are defined in {@link HFileBlockType}.
 */
public abstract class HFileBlock {
  /**
   * The HFile block header size without checksum
   */
  public static final int HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM =
      MAGIC_LENGTH + 2 * SIZEOF_INT + SIZEOF_LONG;
  /**
   * The HFile block header size with checksum
   * There is a 1 byte checksum type, followed by a 4 byte bytesPerChecksum
   * followed by another 4 byte value to store sizeofDataOnDisk.
   */
  public static final int HFILEBLOCK_HEADER_SIZE =
      HFILEBLOCK_HEADER_SIZE_NO_CHECKSUM + SIZEOF_BYTE + 2 * SIZEOF_INT;

  /**
   * Each checksum value is an integer that can be stored in 4 bytes.
   */
  static final int CHECKSUM_SIZE = SIZEOF_INT;

  static class Header {
    // Format of header is:
    // 8 bytes - block magic
    // 4 bytes int - onDiskSizeWithoutHeader
    // 4 bytes int - uncompressedSizeWithoutHeader
    // 8 bytes long - prevBlockOffset
    // The following 3 are only present if header contains checksum information
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
  private final HFileBlockType blockType;
  protected int onDiskSizeWithoutHeader;
  protected int uncompressedSizeWithoutHeader;

  protected HFileBlock(HFileContext context,
                       HFileBlockType blockType,
                       byte[] byteBuff,
                       int startOffsetInBuff) {
    this.context = context;
    this.byteBuff = byteBuff;
    this.startOffsetInBuff = startOffsetInBuff;
    this.blockType = blockType;
    this.onDiskSizeWithoutHeader = readInt(
        byteBuff, startOffsetInBuff + Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    this.uncompressedSizeWithoutHeader = readInt(
        byteBuff, startOffsetInBuff + Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX);
  }

  /**
   * Parses the HFile block header and returns the {@link HFileBlock} instance based on the input.
   *
   * @param context           HFile context.
   * @param byteBuff          Input data.
   * @param startOffsetInBuff Offset to start parsing.
   * @return The {@link HFileBlock} instance based on the input; {@code null} if not able to parse.
   * @throws IOException
   */
  public static HFileBlock parse(HFileContext context, byte[] byteBuff, int startOffsetInBuff) throws IOException {
    HFileBlockType blockType = HFileBlockType.parse(byteBuff, startOffsetInBuff);
    switch (blockType) {
      case ROOT_INDEX:
        return new HFileRootIndexBlock(context, byteBuff, startOffsetInBuff);
      case FILE_INFO:
        return new HFileFileInfoBlock(context, byteBuff, startOffsetInBuff);
      case DATA:
        return new HFileDataBlock(context, byteBuff, startOffsetInBuff);
      default:
        return null;
    }
  }

  /**
   * Allocates a new byte buffer for uncompressed data and returns a new {@link HFileBlock}
   * instance for the decompressed content.
   *
   * @return
   */
  public abstract HFileBlock cloneForUnpack();

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
   *
   * @return {@link HFileBlock} instance
   * @throws IOException
   */
  public HFileBlock unpack() throws IOException {
    // Should only be called for compressed blocks
    Compression.Algorithm compression = context.getCompressAlgo();
    if (compression != Compression.Algorithm.NONE) {
      HFileBlock unpacked = this.cloneForUnpack();
      ByteBuffer blockBufferWithoutHeader = ByteBuffer.wrap(
          unpacked.getByteBuff(),
          HFILEBLOCK_HEADER_SIZE,
          unpacked.getByteBuff().length - HFILEBLOCK_HEADER_SIZE);
      final InputStream byteBuffInputStream = new ByteArrayInputStream(
          byteBuff, startOffsetInBuff + HFILEBLOCK_HEADER_SIZE, onDiskSizeWithoutHeader);
      InputStream dataInputStream = new DataInputStream(byteBuffInputStream);
      Compression.decompress(blockBufferWithoutHeader, dataInputStream, uncompressedSizeWithoutHeader, compression);
      return unpacked;
    }
    return this;
  }

  /**
   * Allocates new byte buffer for the uncompressed bytes.
   *
   * @return A new byte array based on the size of uncompressed data, holding the same header
   * bytes.
   */
  protected byte[] allocateBuffer() {
    int checksumSize = (int) ChecksumUtils.numBytes(getOnDiskSizeWithHeader(), 16384);
    int headerSize = HFILEBLOCK_HEADER_SIZE;
    int capacity = headerSize + uncompressedSizeWithoutHeader + checksumSize;
    byte[] newByteBuff = new byte[capacity];
    System.arraycopy(byteBuff, startOffsetInBuff, newByteBuff, 0, headerSize);
    return newByteBuff;
  }
}
