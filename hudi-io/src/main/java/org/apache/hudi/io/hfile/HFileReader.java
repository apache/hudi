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

import org.apache.hudi.io.util.Option;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A reader reading a HFile.
 */
public class HFileReader {
  private final FSDataInputStream stream;
  private final long fileSize;
  private boolean isMetadataInitialized = false;
  private HFileContext context;
  private TreeMap<Key, BlockIndexEntry> blockIndexEntryMap;
  private HFileBlock metaIndexBlock;
  private HFileBlock fileInfoBlock;

  public HFileReader(FSDataInputStream stream, long fileSize) {
    this.stream = stream;
    this.fileSize = fileSize;
  }

  /**
   * Initializes the metadata by reading the "Load-on-open" section.
   *
   * @throws IOException upon error.
   */
  public void initializeMetadata() throws IOException {
    assert !this.isMetadataInitialized;

    // Read Trailer (serialized in Proto)
    HFileTrailer trailer = readTrailer(stream, fileSize);
    this.context = HFileContext.builder()
        .compressionCodec(trailer.getCompressionCodec())
        .build();
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, trailer.getLoadOnOpenDataOffset(), fileSize - HFileTrailer.getTrailerSize());
    HFileRootIndexBlock dataIndexBlock =
        (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    this.blockIndexEntryMap = dataIndexBlock.readDataIndex(trailer.getDataIndexCount());
    this.metaIndexBlock = blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    this.fileInfoBlock = blockReader.nextBlock(HFileBlockType.FILE_INFO);

    this.isMetadataInitialized = true;
  }

  /**
   * Seeks to the key to look up.
   *
   * @param key Key to look up.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or empty {@link Option} if the lookup key does not exist.
   * @throws IOException upon error.
   */
  public Option<KeyValue> seekTo(Key key) throws IOException {
    // Searches the block that may contain the lookup key based the starting keys of
    // all blocks (sorted in the TreeMap of block index entries), using binary search.
    // The result contains the greatest key less than or equal to the given key,
    // or null if there is no such key.
    Map.Entry<Key, BlockIndexEntry> floorEntry = blockIndexEntryMap.floorEntry(key);
    if (floorEntry == null) {
      // Key smaller than the start key of the first block
      return Option.empty();
    }
    BlockIndexEntry blockToRead = floorEntry.getValue();
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, blockToRead.getOffset(),
        blockToRead.getOffset() + (long) blockToRead.getSize());
    HFileDataBlock dataBlock = (HFileDataBlock) blockReader.nextBlock(HFileBlockType.DATA);
    return seekToKeyInBlock(dataBlock, key);
  }

  /**
   * Reads the HFile major version from the input.
   *
   * @param bytes  Input data.
   * @param offset Offset to start reading.
   * @return Major version of the file.
   */
  public static int readMajorVersion(byte[] bytes, int offset) {
    int ch1 = bytes[offset] & 0xFF;
    int ch2 = bytes[offset + 1] & 0xFF;
    int ch3 = bytes[offset + 2] & 0xFF;
    return ((ch1 << 16) + (ch2 << 8) + ch3);
  }

  /**
   * Reads and parses the HFile trailer.
   *
   * @param stream   HFile input.
   * @param fileSize HFile size.
   * @return {@link HFileTrailer} instance.
   * @throws IOException upon error.
   */
  private static HFileTrailer readTrailer(FSDataInputStream stream,
                                          long fileSize) throws IOException {
    int bufferSize = HFileTrailer.getTrailerSize();
    long seekPos = fileSize - bufferSize;
    if (seekPos < 0) {
      // It is hard to imagine such a small HFile.
      seekPos = 0;
      bufferSize = (int) fileSize;
    }
    stream.seek(seekPos);

    byte[] byteBuff = new byte[bufferSize];
    stream.readFully(byteBuff);

    int majorVersion = readMajorVersion(byteBuff, bufferSize - 3);
    int minorVersion = byteBuff[bufferSize - 3];

    HFileTrailer trailer = new HFileTrailer(majorVersion, minorVersion);
    trailer.deserialize(new DataInputStream(new ByteArrayInputStream(byteBuff)));
    return trailer;
  }

  /**
   * Seeks to the lookup key inside a {@link HFileDataBlock}.
   *
   * @param dataBlock The data block to seek.
   * @param key       The key to lookup.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or empty {@link Option} if the lookup key does not exist.
   */
  private Option<KeyValue> seekToKeyInBlock(HFileDataBlock dataBlock, Key key) {
    return dataBlock.seekTo(key);
  }
}
