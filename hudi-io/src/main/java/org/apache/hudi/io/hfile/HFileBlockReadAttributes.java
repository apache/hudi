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

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.util.IOUtils.readInt;

public class HFileBlockReadAttributes {
  public final byte[] byteBuff;
  public final int startOffsetInBuff;
  public final int sizeCheckSum;
  public final int uncompressedEndOffset;
  public final int onDiskSizeWithoutHeader;
  public final int uncompressedSizeWithoutHeader;
  public final int bytesPerChecksum;
  public boolean isUnpacked = false;
  public byte[] compressedByteBuff;
  public int startOffsetInCompressedBuff;

  public HFileBlockReadAttributes(HFileContext context,
                                  byte[] byteBuff,
                                  int startOffsetInBuff) {
    this.onDiskSizeWithoutHeader = readInt(
        byteBuff, startOffsetInBuff + HFileBlock.Header.ON_DISK_SIZE_WITHOUT_HEADER_INDEX);
    this.uncompressedSizeWithoutHeader = readInt(
        byteBuff, startOffsetInBuff + HFileBlock.Header.UNCOMPRESSED_SIZE_WITHOUT_HEADER_INDEX);
    this.bytesPerChecksum = readInt(
        byteBuff, startOffsetInBuff + HFileBlock.Header.BYTES_PER_CHECKSUM_INDEX);
    this.sizeCheckSum = numChecksumBytes(
        onDiskSizeWithoutHeader + (long) HFILEBLOCK_HEADER_SIZE, bytesPerChecksum);
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
   * Allocates new byte buffer for the uncompressed bytes.
   *
   * @return a new byte array based on the size of uncompressed data, holding the same header
   * bytes.
   */
  protected byte[] allocateBufferForUnpacking() {
    int capacity = HFILEBLOCK_HEADER_SIZE + uncompressedSizeWithoutHeader + sizeCheckSum;
    return new byte[capacity];
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
}
