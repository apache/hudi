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

package org.apache.hudi.io.hfile.writer;

import org.apache.hudi.io.hfile.HFileBlock;

import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class Block {
  public static final int DEFAULT_BYTES_PER_CHECKSUM = 16 * 1024;
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public static final ChecksumType CHECKSUM_TYPE = ChecksumType.NULL;
  protected final byte[] blockMagic;
  protected long blockOffset = -1;
  protected int blockSize;

  protected Block(byte[] magic, int blockSize) {
    this.blockMagic = magic;
    this.blockSize = blockSize;
  }

  public void setBlockOffset(long blockOffset) {
    this.blockOffset = blockOffset;
  }

  public long getBlockOffset() {
    return blockOffset;
  }

  protected byte[] getBlockMagic() {
    return blockMagic;
  }

  public abstract ByteBuffer getPayload();

  public ByteBuffer serialize() {
    // Block payload.
    ByteBuffer payloadBuff = getPayload();
    // Buffer for building block.
    ByteBuffer buf = ByteBuffer.allocate(blockSize * 2);
    // Block header
    // 1. Magic is always 8 bytes.
    buf.put(blockMagic, 0, 8);
    // TODO: add compress when configured.
    // 2. onDiskSizeWithoutHeader.
    buf.putInt(payloadBuff.limit());
    // 3. uncompressedSizeWithoutHeader.
    buf.putInt(payloadBuff.limit());
    // TODO: pass into previous block offset.
    // 4. previous block offset.
    buf.putLong(-1);
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
    buf.put(payloadBuff);
    // 9. Checksum
    buf.put(calcChecksumBytes(CHECKSUM_TYPE));

    // Update sizes
    buf.flip();
    return buf;
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
