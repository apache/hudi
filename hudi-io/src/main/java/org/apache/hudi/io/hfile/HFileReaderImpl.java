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

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hudi.io.hfile.HFileBlock.HFILEBLOCK_HEADER_SIZE;
import static org.apache.hudi.io.hfile.HFileUtils.readMajorVersion;

/**
 * An implementation a {@link HFileReader}.
 */
public class HFileReaderImpl implements HFileReader {
  private final FSDataInputStream stream;
  private final long fileSize;

  private final HFilePosition currentPos;
  private boolean isMetadataInitialized = false;
  private HFileTrailer trailer;
  private HFileContext context;
  private TreeMap<Key, BlockIndexEntry> dataBlockIndexEntryMap;
  private TreeMap<Key, BlockIndexEntry> metaBlockIndexEntryMap;
  private HFileInfo fileInfo;
  private Option<BlockIndexEntry> currentDataBlockEntry;
  private Option<HFileDataBlock> currentDataBlock;

  public HFileReaderImpl(FSDataInputStream stream, long fileSize) {
    this.stream = stream;
    this.fileSize = fileSize;
    this.currentPos = new HFilePosition();
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
    HFileRootIndexBlock dataIndexBlock =
        (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    this.dataBlockIndexEntryMap = dataIndexBlock.readBlockIndex(trailer.getDataIndexCount(), false);
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
    byte[] bytes = fileInfo.get(key);
    return bytes != null ? Option.of(bytes) : Option.empty();
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
      if (currentDataBlockEntry.get().getNextBlockKey().isPresent()) {
        int comparedNextBlockFirstKey =
            key.compareTo(currentDataBlockEntry.get().getNextBlockKey().get());
        if (comparedNextBlockFirstKey >= 0) {
          // Searches the block that may contain the lookup key based the starting keys of
          // all blocks (sorted in the TreeMap of block index entries), using binary search.
          // The result contains the greatest key less than or equal to the given key,
          // or null if there is no such key.

          Map.Entry<Key, BlockIndexEntry> floorEntry = dataBlockIndexEntryMap.floorEntry(key);
          if (floorEntry == null) {
            // Key smaller than the start key of the first block
            return SEEK_TO_BACKWARDS;
          }
          currentDataBlockEntry = Option.of(floorEntry.getValue());
          currentDataBlock = Option.empty();
          currentPos.setOffset(
              (int) currentDataBlockEntry.get().getOffset() + HFILEBLOCK_HEADER_SIZE);
        }
      }
      if (!currentDataBlockEntry.get().getNextBlockKey().isPresent()) {
        // This is the last data block.  Check against the last key.
        if (fileInfo.getLastKey().isPresent()) {
          int comparedLastKey = key.compareTo(fileInfo.getLastKey().get());
          if (comparedLastKey > 0) {
            currentDataBlockEntry = Option.empty();
            currentDataBlock = Option.empty();
            currentPos.setEof();
            return SEEK_TO_EOF;
          }
        }
      }

      if (!currentDataBlock.isPresent()) {
        currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
      }

      return currentDataBlock.get()
          .seekTo(currentPos, key, (int) currentDataBlockEntry.get().getOffset());
    }
    if (compareCurrent == 0) {
      return SEEK_TO_FOUND;
    }
    // Backward seek not supported
    return SEEK_TO_BACKWARDS;
  }

  @Override
  public boolean seekTo() throws IOException {
    initializeMetadata();
    if (trailer.getNumKeyValueEntries() == 0) {
      currentPos.setEof();
      return false;
    }
    // Move the current position to the beginning of the first data block
    currentPos.setOffset(dataBlockIndexEntryMap.firstKey().getOffset() + HFILEBLOCK_HEADER_SIZE);
    currentPos.unsetEof();
    currentDataBlockEntry = Option.of(dataBlockIndexEntryMap.firstEntry().getValue());
    // The data block will be read when {@link #getKeyValue} is called
    currentDataBlock = Option.empty();
    return true;
  }

  @Override
  public boolean next() throws IOException {
    if (currentPos.isValid()) {
      if (!currentDataBlock.isPresent()) {
        currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
      }
      if (currentDataBlock.get().next(currentPos, (int) currentDataBlockEntry.get().getOffset())) {
        // The position is advanced by the data block instance
        return true;
      }
      currentDataBlockEntry = getNextBlockIndexEntry(currentDataBlockEntry.get());
      currentDataBlock = Option.empty();
      if (!currentDataBlockEntry.isPresent()) {
        currentPos.setEof();
        return false;
      }
      currentPos.setOffset((int) currentDataBlockEntry.get().getOffset() + HFILEBLOCK_HEADER_SIZE);
      return true;
    }
    return false;
  }

  @Override
  public Option<KeyValue> getKeyValue() throws IOException {
    if (currentPos.isValid()) {
      Option<KeyValue> keyValue = currentPos.getKeyValue();
      if (!keyValue.isPresent()) {
        if (!currentDataBlock.isPresent()) {
          currentDataBlock = Option.of(instantiateHFileDataBlock(currentDataBlockEntry.get()));
        }
        keyValue =
            Option.of(currentDataBlock.get().readKeyValue(
                currentPos.getOffset() - (int) currentDataBlockEntry.get().getOffset()));
        currentPos.setKeyValue(keyValue.get());
      }
      return keyValue;
    }
    return Option.empty();
  }

  @Override
  public boolean isSeeked() {
    return currentPos.isSeeked();
  }
  
  @Override
  public void close() throws IOException {
    stream.close();
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

  private Option<BlockIndexEntry> getNextBlockIndexEntry(BlockIndexEntry entry) {
    Map.Entry<Key, BlockIndexEntry> keyBlockIndexEntryEntry =
        dataBlockIndexEntryMap.higherEntry(entry.getKey());
    if (keyBlockIndexEntryEntry == null) {
      return Option.empty();
    }
    return Option.of(keyBlockIndexEntryEntry.getValue());
  }

  private HFileDataBlock instantiateHFileDataBlock(BlockIndexEntry blockToRead) throws IOException {
    HFileBlockReader blockReader = new HFileBlockReader(
        context, stream, blockToRead.getOffset(),
        blockToRead.getOffset() + (long) blockToRead.getSize());
    return (HFileDataBlock) blockReader.nextBlock(HFileBlockType.DATA);
  }
}
