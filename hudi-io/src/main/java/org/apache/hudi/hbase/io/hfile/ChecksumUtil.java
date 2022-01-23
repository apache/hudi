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

package org.apache.hudi.hbase.io.hfile;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.nio.SingleByteBuff;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hudi.hbase.util.ChecksumType;
import org.apache.hadoop.util.DataChecksum;

/**
 * Utility methods to compute and validate checksums.
 */
@InterfaceAudience.Private
public class ChecksumUtil {
  public static final Logger LOG = LoggerFactory.getLogger(ChecksumUtil.class);

  public static final int CHECKSUM_BUF_SIZE = 256;

  /**
   * This is used by unit tests to make checksum failures throw an
   * exception instead of returning null. Returning a null value from
   * checksum validation will cause the higher layer to retry that
   * read with hdfs-level checksums. Instead, we would like checksum
   * failures to cause the entire unit test to fail.
   */
  private static boolean generateExceptions = false;

  /**
   * Generates a checksum for all the data in indata. The checksum is
   * written to outdata.
   * @param indata input data stream
   * @param startOffset starting offset in the indata stream from where to
   *                    compute checkums from
   * @param endOffset ending offset in the indata stream upto
   *                   which checksums needs to be computed
   * @param outdata the output buffer where checksum values are written
   * @param outOffset the starting offset in the outdata where the
   *                  checksum values are written
   * @param checksumType type of checksum
   * @param bytesPerChecksum number of bytes per checksum value
   */
  static void generateChecksums(byte[] indata, int startOffset, int endOffset,
                                byte[] outdata, int outOffset, ChecksumType checksumType,
                                int bytesPerChecksum) throws IOException {

    if (checksumType == ChecksumType.NULL) {
      return; // No checksum for this block.
    }

    DataChecksum checksum = DataChecksum.newDataChecksum(
        checksumType.getDataChecksumType(), bytesPerChecksum);

    checksum.calculateChunkedSums(
        ByteBuffer.wrap(indata, startOffset, endOffset - startOffset),
        ByteBuffer.wrap(outdata, outOffset, outdata.length - outOffset));
  }

  /**
   * Like the hadoop's {@link DataChecksum#verifyChunkedSums(ByteBuffer, ByteBuffer, String, long)},
   * this method will also verify checksum of each chunk in data. the difference is: this method can
   * accept {@link ByteBuff} as arguments, we can not add it in hadoop-common so defined here.
   * @param dataChecksum to calculate the checksum.
   * @param data as the input
   * @param checksums to compare
   * @param pathName indicate that the data is read from which file.
   * @return a flag indicate the checksum match or mismatch.
   * @see org.apache.hadoop.util.DataChecksum#verifyChunkedSums(ByteBuffer, ByteBuffer, String,
   *      long)
   */
  private static boolean verifyChunkedSums(DataChecksum dataChecksum, ByteBuff data,
                                           ByteBuff checksums, String pathName) {
    // Almost all of the HFile Block are about 64KB, and it would be a SingleByteBuff, use the
    // Hadoop's verify checksum directly, because it'll use the native checksum, which has no extra
    // byte[] allocation or copying. (HBASE-21917)
    if (data instanceof SingleByteBuff && checksums instanceof SingleByteBuff) {
      // the checksums ByteBuff must also be an SingleByteBuff because it's duplicated from data.
      ByteBuffer dataBB = (ByteBuffer) (data.nioByteBuffers()[0]).duplicate()
          .position(data.position()).limit(data.limit());
      ByteBuffer checksumBB = (ByteBuffer) (checksums.nioByteBuffers()[0]).duplicate()
          .position(checksums.position()).limit(checksums.limit());
      try {
        dataChecksum.verifyChunkedSums(dataBB, checksumBB, pathName, 0);
        return true;
      } catch (ChecksumException e) {
        return false;
      }
    }

    // If the block is a MultiByteBuff. we use a small byte[] to update the checksum many times for
    // reducing GC pressure. it's a rare case.
    int checksumTypeSize = dataChecksum.getChecksumType().size;
    if (checksumTypeSize == 0) {
      return true;
    }
    // we have 5 checksum type now: NULL,DEFAULT,MIXED,CRC32,CRC32C. the former three need 0 byte,
    // and the other two need 4 bytes.
    assert checksumTypeSize == 4;

    int bytesPerChecksum = dataChecksum.getBytesPerChecksum();
    int startDataPos = data.position();
    data.mark();
    checksums.mark();
    try {
      // allocate an small buffer for reducing young GC (HBASE-21917), and copy 256 bytes from
      // ByteBuff to update the checksum each time. if we upgrade to an future JDK and hadoop
      // version which support DataCheckSum#update(ByteBuffer), we won't need to update the checksum
      // multiple times then.
      byte[] buf = new byte[CHECKSUM_BUF_SIZE];
      byte[] sum = new byte[checksumTypeSize];
      while (data.remaining() > 0) {
        int n = Math.min(data.remaining(), bytesPerChecksum);
        checksums.get(sum);
        dataChecksum.reset();
        for (int remain = n, len; remain > 0; remain -= len) {
          // Copy 256 bytes from ByteBuff to update the checksum each time, if the remaining
          // bytes is less than 256, then just update the remaining bytes.
          len = Math.min(CHECKSUM_BUF_SIZE, remain);
          data.get(buf, 0, len);
          dataChecksum.update(buf, 0, len);
        }
        int calculated = (int) dataChecksum.getValue();
        int stored = (sum[0] << 24 & 0xff000000) | (sum[1] << 16 & 0xff0000)
            | (sum[2] << 8 & 0xff00) | (sum[3] & 0xff);
        if (calculated != stored) {
          if (LOG.isTraceEnabled()) {
            long errPos = data.position() - startDataPos - n;
            LOG.trace("Checksum error: {} at {} expected: {} got: {}", pathName, errPos, stored,
                calculated);
          }
          return false;
        }
      }
    } finally {
      data.reset();
      checksums.reset();
    }
    return true;
  }

  /**
   * Validates that the data in the specified HFileBlock matches the checksum. Generates the
   * checksums for the data and then validate that it matches those stored in the end of the data.
   * @param buf Contains the data in following order: HFileBlock header, data, checksums.
   * @param pathName Path of the HFile to which the {@code data} belongs. Only used for logging.
   * @param offset offset of the data being validated. Only used for logging.
   * @param hdrSize Size of the block header in {@code data}. Only used for logging.
   * @return True if checksum matches, else false.
   */
  static boolean validateChecksum(ByteBuff buf, String pathName, long offset, int hdrSize) {
    ChecksumType ctype = ChecksumType.codeToType(buf.get(HFileBlock.Header.CHECKSUM_TYPE_INDEX));
    if (ctype == ChecksumType.NULL) {
      return true;// No checksum validations needed for this block.
    }

    // read in the stored value of the checksum size from the header.
    int bytesPerChecksum = buf.getInt(HFileBlock.Header.BYTES_PER_CHECKSUM_INDEX);
    DataChecksum dataChecksum =
        DataChecksum.newDataChecksum(ctype.getDataChecksumType(), bytesPerChecksum);
    assert dataChecksum != null;
    int onDiskDataSizeWithHeader =
        buf.getInt(HFileBlock.Header.ON_DISK_DATA_SIZE_WITH_HEADER_INDEX);
    LOG.trace("dataLength={}, sizeWithHeader={}, checksumType={}, file={}, "
            + "offset={}, headerSize={}, bytesPerChecksum={}", buf.capacity(), onDiskDataSizeWithHeader,
        ctype.getName(), pathName, offset, hdrSize, bytesPerChecksum);
    ByteBuff data = buf.duplicate().position(0).limit(onDiskDataSizeWithHeader);
    ByteBuff checksums = buf.duplicate().position(onDiskDataSizeWithHeader).limit(buf.limit());
    return verifyChunkedSums(dataChecksum, data, checksums, pathName);
  }

  /**
   * Returns the number of bytes needed to store the checksums for
   * a specified data size
   * @param datasize number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of bytes needed to store the checksum values
   */
  static long numBytes(long datasize, int bytesPerChecksum) {
    return numChunks(datasize, bytesPerChecksum) * HFileBlock.CHECKSUM_SIZE;
  }

  /**
   * Returns the number of checksum chunks needed to store the checksums for
   * a specified data size
   * @param datasize number of bytes of data
   * @param bytesPerChecksum number of bytes in a checksum chunk
   * @return The number of checksum chunks
   */
  static long numChunks(long datasize, int bytesPerChecksum) {
    long numChunks = datasize/bytesPerChecksum;
    if (datasize % bytesPerChecksum != 0) {
      numChunks++;
    }
    return numChunks;
  }

  /**
   * Mechanism to throw an exception in case of hbase checksum
   * failure. This is used by unit tests only.
   * @param value Setting this to true will cause hbase checksum
   *              verification failures to generate exceptions.
   */
  public static void generateExceptionForChecksumFailureForTest(boolean value) {
    generateExceptions = value;
  }
}
