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
import org.apache.hudi.io.hfile.protobuf.generated.HFileProtos;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hudi.io.hfile.DataSize.MAGIC_LENGTH;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT32;

/**
 * Represents a HFile trailer, which is serialized and deserialized using
 * {@link HFileProtos.TrailerProto} with Protobuf.
 */
public class HFileTrailer {
  // This is the trailer size for HFile V3
  public static final int TRAILER_SIZE = 1024 * 4;
  private static final int NOT_PB_SIZE = MAGIC_LENGTH + SIZEOF_INT32;


  /**
   * Offset to the fileinfo data, a small block of vitals. Necessary in v1 but
   * only potentially useful for pretty-printing in v2.
   */
  private long fileInfoOffset;

  /**
   * In version 1, the offset to the data block index. Starting from version 2,
   * the meaning of this field is the offset to the section of the file that
   * should be loaded at the time the file is being opened: i.e. on open we load
   * the root index, file info, etc. See http://hbase.apache.org/book.html#_hfile_format_2
   * in the reference guide.
   */
  private long loadOnOpenDataOffset;

  /**
   * The number of entries in the root data index.
   */
  private int dataIndexCount;

  /**
   * Total uncompressed size of all blocks of the data index
   */
  private long uncompressedDataIndexSize;

  /**
   * The number of entries in the meta index
   */
  private int metaIndexCount;

  /**
   * The total uncompressed size of keys/values stored in the file.
   */
  private long totalUncompressedBytes;

  /**
   * The number of key/value pairs in the file. This field was int in version 1,
   * but is now long.
   */
  private long entryCount;

  /**
   * The compression codec used for all blocks.
   */
  private CompressionCodec compressionCodec = CompressionCodec.NONE;

  /**
   * The number of levels in the potentially multi-level data index. Used from
   * version 2 onwards.
   */
  private int numDataIndexLevels;

  /**
   * The offset of the first data block.
   */
  private long firstDataBlockOffset;

  /**
   * It is guaranteed that no key/value data blocks start after this offset in
   * the file.
   */
  private long lastDataBlockOffset;

  /**
   * Raw key comparator class name in version 3
   */
  // We could write the actual class name from 2.0 onwards and handle BC
  private String comparatorClassName = "CellComparator.getInstance().getClass().getName()";

  /**
   * The encryption key
   */
  private byte[] encryptionKey;

  private final int majorVersion;
  private final int minorVersion;

  public HFileTrailer(int majorVersion, int minorVersion) {
    this.majorVersion = majorVersion;
    this.minorVersion = minorVersion;
  }

  public static int getTrailerSize() {
    return TRAILER_SIZE;
  }

  public long getLoadOnOpenDataOffset() {
    return loadOnOpenDataOffset;
  }

  public int getNumDataIndexLevels() {
    return numDataIndexLevels;
  }

  public int getDataIndexCount() {
    return dataIndexCount;
  }

  public CompressionCodec getCompressionCodec() {
    return compressionCodec;
  }

  public void deserialize(DataInputStream stream) throws IOException {
    HFileBlockType.TRAILER.readAndCheckMagic(stream);
    // Read Protobuf
    int start = stream.available();
    HFileProtos.TrailerProto trailerProto =
        HFileProtos.TrailerProto.PARSER.parseDelimitedFrom(stream);
    int size = start - stream.available();
    stream.skip(getTrailerSize() - NOT_PB_SIZE - size);
    // May optionally read version again and validate
    // process the PB
    if (trailerProto.hasFileInfoOffset()) {
      fileInfoOffset = trailerProto.getFileInfoOffset();
    }
    if (trailerProto.hasLoadOnOpenDataOffset()) {
      loadOnOpenDataOffset = trailerProto.getLoadOnOpenDataOffset();
    }
    if (trailerProto.hasUncompressedDataIndexSize()) {
      uncompressedDataIndexSize = trailerProto.getUncompressedDataIndexSize();
    }
    if (trailerProto.hasTotalUncompressedBytes()) {
      totalUncompressedBytes = trailerProto.getTotalUncompressedBytes();
    }
    if (trailerProto.hasDataIndexCount()) {
      dataIndexCount = trailerProto.getDataIndexCount();
    }
    if (trailerProto.hasMetaIndexCount()) {
      metaIndexCount = trailerProto.getMetaIndexCount();
    }
    if (trailerProto.hasEntryCount()) {
      entryCount = trailerProto.getEntryCount();
    }
    if (trailerProto.hasNumDataIndexLevels()) {
      numDataIndexLevels = trailerProto.getNumDataIndexLevels();
    }
    if (trailerProto.hasFirstDataBlockOffset()) {
      firstDataBlockOffset = trailerProto.getFirstDataBlockOffset();
    }
    if (trailerProto.hasLastDataBlockOffset()) {
      lastDataBlockOffset = trailerProto.getLastDataBlockOffset();
    }
    if (trailerProto.hasComparatorClassName()) {
      comparatorClassName = trailerProto.getComparatorClassName();
    }
    if (trailerProto.hasCompressionCodec()) {
      compressionCodec = HFileCompressionCodec.values()[trailerProto.getCompressionCodec()]
          .toHoodieCompressionCodec();
    } else {
      compressionCodec = CompressionCodec.NONE;
    }
    if (trailerProto.hasEncryptionKey()) {
      encryptionKey = trailerProto.getEncryptionKey().toByteArray();
    }
  }

  @Override
  public String toString() {
    return "HFileTrailer{"
        + "fileInfoOffset=" + fileInfoOffset
        + ", loadOnOpenDataOffset=" + loadOnOpenDataOffset
        + ", dataIndexCount=" + dataIndexCount
        + ", uncompressedDataIndexSize=" + uncompressedDataIndexSize
        + ", metaIndexCount=" + metaIndexCount
        + ", totalUncompressedBytes=" + totalUncompressedBytes
        + ", entryCount=" + entryCount
        + ", compressionCodec=" + compressionCodec
        + ", numDataIndexLevels=" + numDataIndexLevels
        + ", firstDataBlockOffset=" + firstDataBlockOffset
        + ", lastDataBlockOffset=" + lastDataBlockOffset
        + ", comparatorClassName='" + comparatorClassName + '\''
        + ", encryptionKey=" + Arrays.toString(encryptionKey)
        + ", majorVersion=" + majorVersion
        + ", minorVersion=" + minorVersion
        + '}';
  }
}
