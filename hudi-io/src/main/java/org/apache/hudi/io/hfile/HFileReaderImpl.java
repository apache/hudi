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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.io.SeekableDataInputStream;

import org.apache.logging.log4j.util.Strings;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.hfile.HFileUtils.readMajorVersion;

/**
 * Base implementation of {@link HFileReader} without caching.
 * This provides the core functionality for reading HFile format data.
 */
public class HFileReaderImpl implements HFileReader {
  protected final SeekableDataInputStream stream;
  protected final long fileSize;

  protected final HFileCursor cursor;
  protected boolean isMetadataInitialized = false;
  protected HFileTrailer trailer;
  protected HFileContext context;
  protected TreeMap<Key, BlockIndexEntry> dataBlockIndexEntryMap;
  protected TreeMap<Key, BlockIndexEntry> metaBlockIndexEntryMap;
  protected HFileInfo fileInfo;
  protected Option<BlockIndexEntry> currentDataBlockEntry;
  protected Option<HFileDataBlock> currentDataBlock;

  public HFileReaderImpl(SeekableDataInputStream stream, long fileSize) {
    this.stream = stream;
    this.fileSize = fileSize;
    this.cursor = new HFileCursor();
    this.currentDataBlockEntry = Option.empty();
    this.currentDataBlock = Option.empty();
  }

  @Override
  public synchronized void initializeMetadata() throws IOException {
    if (this.isMetadataInitialized) {
      return;
    }

    // Read Trailer (serialized in Proto)
    this.trailer = readTrailer(stream, fileSize);
    this.context = HFileContext.builder()
        .compressionCodec(trailer.getCompressionCodec())
        .build();
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, trailer.getLoadOnOpenDataOffset(),
        fileSize - HFileTrailer.getTrailerSize());
    this.dataBlockIndexEntryMap = readDataBlockIndex(
        blockReader, trailer.getDataIndexCount(), trailer.getNumDataIndexLevels());
    HFileRootIndexBlock metaIndexBlock =
        (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    this.metaBlockIndexEntryMap = metaIndexBlock.readBlockIndex(trailer.getMetaIndexCount(), true);
    HFileFileInfoBlock fileInfoBlock =
        (HFileFileInfoBlock) blockReader.nextBlock(HFileBlockType.FILE_INFO);
    this.fileInfo = fileInfoBlock.readFileInfo();
    this.isMetadataInitialized = true;
  }

  @Override
  public Option<byte[]> getMetaInfo(UTF8StringKey key) throws IOException {
    initializeMetadata();
    return Option.ofNullable(fileInfo.get(key));
  }

  @Override
  public Option<ByteBuffer> getMetaBlock(String metaBlockName) throws IOException {
    initializeMetadata();
    BlockIndexEntry blockIndexEntry = metaBlockIndexEntryMap.get(new UTF8StringKey(metaBlockName));
    if (blockIndexEntry == null) {
      return Option.empty();
    }
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, blockIndexEntry.getOffset(),
        blockIndexEntry.getOffset() + blockIndexEntry.getSize());
    HFileMetaBlock block = (HFileMetaBlock) blockReader.nextBlock(HFileBlockType.META);
    return Option.of(block.readContent());
  }

  @Override
  public long getNumKeyValueEntries() {
    try {
      initializeMetadata();
      return trailer.getNumKeyValueEntries();
    } catch (IOException e) {
      throw new RuntimeException("Cannot read HFile", e);
    }
  }

  @Override
  public int seekTo(Key key) throws IOException {
    Option<KeyValue> currentKeyValue = getKeyValue();
    if (!currentKeyValue.isPresent()) {
      return SEEK_TO_EOF;
    }
    int compareCurrent = key.compareTo(currentKeyValue.get().getKey());
    if (compareCurrent > 0) {
      if (currentDataBlockEntry.get().getNextBlockFirstKey().isPresent()) {
        int comparedNextBlockFirstKey =
            key.compareTo(currentDataBlockEntry.get().getNextBlockFirstKey().get());
        if (comparedNextBlockFirstKey >= 0) {
          // Searches the block that may contain the lookup key based the starting keys of
          // all blocks (sorted in the TreeMap of block index entries), using binary search.
          // The result contains the greatest key less than or equal to the given key.

          Map.Entry<Key, BlockIndexEntry> floorEntry = dataBlockIndexEntryMap.floorEntry(key);
          if (floorEntry == null) {
            // Key smaller than the start key of the first block which should never happen here
            throw new IllegalStateException(
                "Unexpected state of the HFile reader when looking up the key: " + key
                    + " data block index: "
                    + Strings.join(dataBlockIndexEntryMap.values(), ','));
          }
          currentDataBlockEntry = Option.of(floorEntry.getValue());
          currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
          cursor.setOffset(
              (int) currentDataBlockEntry.get().getOffset() + HFILEBLOCK_HEADER_SIZE);
        }
      }
      if (!currentDataBlockEntry.get().getNextBlockFirstKey().isPresent()) {
        // This is the last data block. Check against the last key.
        if (fileInfo.getLastKey().isPresent()) {
          int comparedLastKey = key.compareTo(fileInfo.getLastKey().get());
          if (comparedLastKey > 0) {
            currentDataBlockEntry = Option.empty();
            currentDataBlock = Option.empty();
            cursor.setEof();
            return SEEK_TO_EOF;
          }
        }
      }

      if (!currentDataBlock.isPresent()) {
        currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
      }

      return currentDataBlock.get()
          .seekTo(cursor, key, (int) currentDataBlockEntry.get().getOffset());
    }
    if (compareCurrent == 0) {
      return SEEK_TO_FOUND;
    }
    // compareCurrent < 0, i.e., the lookup key is lexicographically smaller than
    // the key at the current cursor
    // We need to check two cases that are allowed:
    // 1. The lookup key is lexicographically greater than or equal to the fake first key
    // of the data block based on the block index and lexicographically smaller than
    // the actual first key of the data block
    // See HFileReader#SEEK_TO_BEFORE_BLOCK_FIRST_KEY for more information
    if (isAtFirstKeyOfBlock(currentDataBlockEntry.get())
        && key.compareTo(currentDataBlockEntry.get().getFirstKey()) >= 0) {
      return SEEK_TO_BEFORE_BLOCK_FIRST_KEY;
    }
    // 2. The lookup key is lexicographically smaller than the first key of the file
    if (!dataBlockIndexEntryMap.isEmpty()
        && isAtFirstKeyOfBlock(dataBlockIndexEntryMap.firstEntry().getValue())) {
      // the lookup key is lexicographically smaller than the first key of the file
      return SEEK_TO_BEFORE_FILE_FIRST_KEY;
    }
    // For invalid backward seekTo, throw exception
    throw new IllegalStateException(
        "The current lookup key is less than the current position of the cursor, "
            + "i.e., backward seekTo, which is not supported and should be avoided. "
            + "key=" + key + " cursor=" + cursor);
  }

  @Override
  public boolean seekTo() throws IOException {
    initializeMetadata();
    if (trailer.getNumKeyValueEntries() == 0) {
      cursor.setEof();
      return false;
    }
    // Move the current position to the beginning of the first data block
    cursor.setOffset(dataBlockIndexEntryMap.firstKey().getOffset() + HFILEBLOCK_HEADER_SIZE);
    cursor.unsetEof();
    currentDataBlockEntry = Option.of(dataBlockIndexEntryMap.firstEntry().getValue());
    // The data block will be read when {@link #getKeyValue} is called
    currentDataBlock = Option.empty();
    return true;
  }

  @Override
  public boolean next() throws IOException {
    if (cursor.isValid()) {
      if (!currentDataBlock.isPresent()) {
        currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
      }
      if (currentDataBlock.get().next(cursor, (int) currentDataBlockEntry.get().getOffset())) {
        // The position is advanced by the data block instance
        return true;
      }
      currentDataBlockEntry = getNextBlockIndexEntry(currentDataBlockEntry.get());
      currentDataBlock = Option.empty();
      if (!currentDataBlockEntry.isPresent()) {
        cursor.setEof();
        return false;
      }
      cursor.setOffset((int) currentDataBlockEntry.get().getOffset() + HFILEBLOCK_HEADER_SIZE);
      return true;
    }
    return false;
  }

  @Override
  public Option<KeyValue> getKeyValue() throws IOException {
    if (cursor.isValid()) {
      Option<KeyValue> keyValue = cursor.getKeyValue();
      if (!keyValue.isPresent()) {
        if (!currentDataBlock.isPresent()) {
          currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
        }
        keyValue =
            Option.of(currentDataBlock.get().readKeyValue(
                cursor.getOffset() - (int) currentDataBlockEntry.get().getOffset()));
        cursor.setKeyValue(keyValue.get());
      }
      return keyValue;
    }
    return Option.empty();
  }

  @Override
  public boolean isSeeked() {
    return cursor.isSeeked();
  }

  @Override
  public void close() throws IOException {
    currentDataBlockEntry = Option.empty();
    currentDataBlock = Option.empty();
    cursor.setEof();
    stream.close();
  }

  HFileTrailer getTrailer() {
    return trailer;
  }

  Map<Key, BlockIndexEntry> getDataBlockIndexMap() {
    return dataBlockIndexEntryMap;
  }

  /**
   * Reads and parses the HFile trailer.
   *
   * @param stream   HFile input.
   * @param fileSize HFile size.
   * @return {@link HFileTrailer} instance.
   * @throws IOException upon error.
   */
  private static HFileTrailer readTrailer(SeekableDataInputStream stream,
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
    int minorVersion = byteBuff[bufferSize - 4];

    HFileTrailer trailer = new HFileTrailer(majorVersion, minorVersion);
    trailer.deserialize(new DataInputStream(new ByteArrayInputStream(byteBuff)));
    return trailer;
  }

  private Option<BlockIndexEntry> getNextBlockIndexEntry(BlockIndexEntry entry) {
    Map.Entry<Key, BlockIndexEntry> keyBlockIndexEntryEntry =
        dataBlockIndexEntryMap.higherEntry(entry.getFirstKey());
    if (keyBlockIndexEntryEntry == null) {
      return Option.empty();
    }
    return Option.of(keyBlockIndexEntryEntry.getValue());
  }

  /**
   * Creates an HFile data block.
   *
   * @param blockToRead the block index entry to read
   * @return the instantiated HFile data block
   * @throws IOException if there's an error reading the block
   */
  public HFileDataBlock instantiateHFileDataBlock(BlockIndexEntry blockToRead) throws IOException {
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, blockToRead.getOffset(),
        blockToRead.getOffset() + (long) blockToRead.getSize());
    return (HFileDataBlock) blockReader.nextBlock(HFileBlockType.DATA);
  }

  private boolean isAtFirstKeyOfBlock(BlockIndexEntry indexEntry) {
    if (cursor.isValid()) {
      return cursor.getOffset() == indexEntry.getOffset() + HFILEBLOCK_HEADER_SIZE;
    }
    return false;
  }

  /**
   * Read single-level or multiple-level data block index, and load all data block
   * information into memory in BFS fashion.
   *
   * @param rootBlockReader a {@link HFileBlockReader} used to read root data index block;
   *                        this reader will be used to read subsequent meta index block
   *                        afterward
   * @param numEntries      the number of entries in the root index block
   * @param levels          the level of the indexes
   * @return single/multiple-level data block index
   */
  private TreeMap<Key, BlockIndexEntry> readDataBlockIndex(HFileBlockReader rootBlockReader,
      int numEntries,
      int levels) throws IOException {
    ValidationUtils.checkArgument(levels > 0,
        "levels of data block index must be greater than 0");
    // Parse root data index block
    HFileRootIndexBlock rootDataIndexBlock =
        (HFileRootIndexBlock) rootBlockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    if (levels == 1) {
      // Single-level data block index
      return rootDataIndexBlock.readBlockIndex(numEntries, false);
    }

    // Multi-level data block index
    // This list stores next patch of leaf index entries in order
    List<BlockIndexEntry> indexEntryList =
        rootDataIndexBlock.readBlockIndexEntry(numEntries, false);
    levels--;

    // Supports BFS search for leaf index entries
    Queue<BlockIndexEntry> queue = new LinkedList<>();
    while (levels >= 1) {
      // (2) Put intermediate / leaf index entries to the queue
      queue.addAll(indexEntryList);
      indexEntryList.clear();

      // (3) BFS
      while (!queue.isEmpty()) {
        BlockIndexEntry indexEntry = queue.poll();
        HFileBlockReader blockReader = new HFileBlockReader(
            context, stream, indexEntry.getOffset(), indexEntry.getOffset() + indexEntry.getSize());
        HFileBlockType blockType = levels > 1
            ? HFileBlockType.INTERMEDIATE_INDEX : HFileBlockType.LEAF_INDEX;
        HFileBlock tempBlock = blockReader.nextBlock(blockType);
        indexEntryList.addAll(((HFileLeafIndexBlock) tempBlock).readBlockIndex());
      }

      // (4) Lower index level
      levels--;
    }

    // (5) Now all entries are data block index entries. Put them into the map
    TreeMap<Key, BlockIndexEntry> blockIndexEntryMap = new TreeMap<>();
    for (int i = 0; i < indexEntryList.size(); i++) {
      Key key = indexEntryList.get(i).getFirstKey();
      blockIndexEntryMap.put(
          key,
          new BlockIndexEntry(
              key,
              i < indexEntryList.size() - 1
                  ? Option.of(indexEntryList.get(i + 1).getFirstKey())
                  : Option.empty(),
              indexEntryList.get(i).getOffset(),
              indexEntryList.get(i).getSize()));
    }

    // (6) Returns the combined index entry map
    return blockIndexEntryMap;
  }
}