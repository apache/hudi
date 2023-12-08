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

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * A reader reading a HFile.
 */
public class HFileReader {
  private final FSDataInputStream stream;
  private final long fileSize;
  private boolean isMetadataInitialized = false;
  private HFileContext context;
  private List<BlockIndexEntry> blockIndexEntryList;
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
        .compressAlgo(trailer.getCompressionCodec())
        .build();
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, trailer.getLoadOnOpenDataOffset(), fileSize - HFileTrailer.getTrailerSize());
    HFileRootIndexBlock dataIndexBlock =
        (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    this.blockIndexEntryList = dataIndexBlock.readDataIndex(trailer.getDataIndexCount());
    this.metaIndexBlock = blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    this.fileInfoBlock = blockReader.nextBlock(HFileBlockType.FILE_INFO);

    this.isMetadataInitialized = true;
  }

  /**
   * Seeks to the key to look up.
   *
   * @param key Key to look up.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or empty {@link Optional} if the lookup key does not exist.
   * @throws IOException upon error.
   */
  public Optional<KeyValue> seekTo(Key key) throws IOException {
    BlockIndexEntry lookUpKey = new BlockIndexEntry(key, -1, -1);
    int rootLevelBlockIndex = searchBlockByKey(lookUpKey);
    if (rootLevelBlockIndex < 0) {
      // Key smaller than the start key of the first block
      return Optional.empty();
    }
    BlockIndexEntry blockToRead = blockIndexEntryList.get(rootLevelBlockIndex);
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, blockToRead.getOffset(), blockToRead.getOffset() + (long) blockToRead.getSize());
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
   * Searches the block that may contain the lookup key based the starting keys
   * of all blocks (sorted in the input list), using binary search.
   *
   * @param lookUpKey The key to lookup.
   * @return Block index in the input. An index outside the range of input means the key does not
   * exist in the HFile.
   */
  private int searchBlockByKey(BlockIndexEntry lookUpKey) {
    int pos = Collections.binarySearch(blockIndexEntryList, lookUpKey);
    // pos is between -(blockKeys.length + 1) to blockKeys.length - 1, see
    // binarySearch's javadoc.

    if (pos >= 0) {
      // This means this is an exact match with an element of blockKeys.
      assert pos < blockIndexEntryList.size();
      return pos;
    }

    // Otherwise, pos = -(i + 1), where blockKeys[i - 1] < key < blockKeys[i],
    // and i is in [0, blockKeys.length]. We are returning j = i - 1 such that
    // blockKeys[j] <= key < blockKeys[j + 1]. In particular, j = -1 if
    // key < blockKeys[0], meaning the file does not contain the given key.

    int i = -pos - 1;
    assert 0 <= i && i <= blockIndexEntryList.size();
    return i - 1;
  }

  /**
   * Seeks to the lookup key inside a {@link HFileDataBlock}.
   *
   * @param dataBlock The data block to seek.
   * @param key       The key to lookup.
   * @return The {@link KeyValue} instance in the block that contains the exact same key as the
   * lookup key; or empty {@link Optional} if the lookup key does not exist.
   */
  private Optional<KeyValue> seekToKeyInBlock(HFileDataBlock dataBlock, Key key) {
    return dataBlock.seekTo(key);
  }
}
