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

import lombok.Getter;
import lombok.ToString;

import java.io.DataInputStream;
import java.io.IOException;

import static org.apache.hudi.io.hfile.DataSize.MAGIC_LENGTH;
import static org.apache.hudi.io.hfile.DataSize.SIZEOF_INT32;

/**
 * Represents a HFile trailer, which is serialized and deserialized using
 * {@link HFileProtos.TrailerProto} with Protobuf.
 */
@ToString
public class HFileTrailer {

  // This is the trailer size for HFile V3
  public static final int TRAILER_SIZE = 1024 * 4;
  private static final int NOT_PB_SIZE = MAGIC_LENGTH + SIZEOF_INT32;

  // Offset to the fileinfo data, a small block of vitals
  private long fileInfoOffset;

  // The offset to the section of the file that should be loaded at the time the file is
  // being opened: i.e. on open we load the root index, file info, etc.
  @Getter
  private long loadOnOpenDataOffset;

  // The number of entries in the root data index
  @Getter
  private int dataIndexCount;

  // Total uncompressed size of all blocks of the data index
  private long uncompressedDataIndexSize;

  // The number of entries in the meta index
  @Getter
  private int metaIndexCount;

  // The total uncompressed size of keys/values stored in the file
  private long totalUncompressedBytes;

  // The number of key/value pairs in the file
  private long keyValueEntryCount;

  // The compression codec used for all blocks.
  @Getter
  private CompressionCodec compressionCodec = CompressionCodec.NONE;

  // The number of levels in the potentially multi-level data index.
  @Getter
  private int numDataIndexLevels;

  // The offset of the first data block.
  private long firstDataBlockOffset;

  // It is guaranteed that no key/value data blocks start after this offset in the file
  private long lastDataBlockOffset;

  // The comparator class name. We don't use this but for reference we still it
  private String comparatorClassName = "";

  // The encryption key
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

  public long getNumKeyValueEntries() {
    return keyValueEntryCount;
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
      keyValueEntryCount = trailerProto.getEntryCount();
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
      compressionCodec = CompressionCodec.decodeCompressionCodec(trailerProto.getCompressionCodec());
    } else {
      compressionCodec = CompressionCodec.NONE;
    }
    if (trailerProto.hasEncryptionKey()) {
      encryptionKey = trailerProto.getEncryptionKey().toByteArray();
    }
  }
}
