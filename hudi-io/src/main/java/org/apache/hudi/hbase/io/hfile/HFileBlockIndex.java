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

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hudi.hbase.ByteBufferKeyOnlyKeyValue;
import org.apache.hudi.hbase.Cell;
import org.apache.hudi.hbase.CellComparator;
//import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hudi.hbase.CellUtil;
import org.apache.hudi.hbase.PrivateCellUtil;
import org.apache.hudi.hbase.KeyValue;
import org.apache.hudi.hbase.KeyValue.KeyOnlyKeyValue;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hudi.hbase.io.HeapSize;
import org.apache.hudi.hbase.io.encoding.DataBlockEncoding;
import org.apache.hudi.hbase.io.hfile.HFile.CachingBlockReader;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.regionserver.KeyValueScanner;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.ClassSize;
import org.apache.hudi.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.StringUtils;

/**
 * Provides functionality to write ({@link BlockIndexWriter}) and read
 * BlockIndexReader
 * single-level and multi-level block indexes.
 *
 * Examples of how to use the block index writer can be found in
 * {@link org.apache.hadoop.hbase.io.hfile.CompoundBloomFilterWriter} and
 *  {@link HFileWriterImpl}. Examples of how to use the reader can be
 *  found in {@link HFileReaderImpl} and
 *  org.apache.hadoop.hbase.io.hfile.TestHFileBlockIndex.
 */
@InterfaceAudience.Private
public class HFileBlockIndex {

  private static final Logger LOG = LoggerFactory.getLogger(HFileBlockIndex.class);

  static final int DEFAULT_MAX_CHUNK_SIZE = 128 * 1024;

  /**
   * The maximum size guideline for index blocks (both leaf, intermediate, and
   * root). If not specified, <code>DEFAULT_MAX_CHUNK_SIZE</code> is used.
   */
  public static final String MAX_CHUNK_SIZE_KEY = "hfile.index.block.max.size";

  /**
   * Minimum number of entries in a single index block. Even if we are above the
   * hfile.index.block.max.size we will keep writing to the same block unless we have that many
   * entries. We should have at least a few entries so that we don't have too many levels in the
   * multi-level index. This should be at least 2 to make sure there is no infinite recursion.
   */
  public static final String MIN_INDEX_NUM_ENTRIES_KEY = "hfile.index.block.min.entries";

  static final int DEFAULT_MIN_INDEX_NUM_ENTRIES = 16;

  /**
   * The number of bytes stored in each "secondary index" entry in addition to
   * key bytes in the non-root index block format. The first long is the file
   * offset of the deeper-level block the entry points to, and the int that
   * follows is that block's on-disk size without including header.
   */
  static final int SECONDARY_INDEX_ENTRY_OVERHEAD = Bytes.SIZEOF_INT
      + Bytes.SIZEOF_LONG;

  /**
   * Error message when trying to use inline block API in single-level mode.
   */
  private static final String INLINE_BLOCKS_NOT_ALLOWED =
      "Inline blocks are not allowed in the single-level-only mode";

  /**
   * The size of a meta-data record used for finding the mid-key in a
   * multi-level index. Consists of the middle leaf-level index block offset
   * (long), its on-disk size without header included (int), and the mid-key
   * entry's zero-based index in that leaf index block.
   */
  private static final int MID_KEY_METADATA_SIZE = Bytes.SIZEOF_LONG +
      2 * Bytes.SIZEOF_INT;

  /**
   * An implementation of the BlockIndexReader that deals with block keys which are plain
   * byte[] like MetaBlock or the Bloom Block for ROW bloom.
   * Does not need a comparator. It can work on Bytes.BYTES_RAWCOMPARATOR
   */
  static class ByteArrayKeyBlockIndexReader extends BlockIndexReader {

    private byte[][] blockKeys;

    public ByteArrayKeyBlockIndexReader(final int treeLevel) {
      // Can be null for METAINDEX block
      searchTreeLevel = treeLevel;
    }

    @Override
    protected long calculateHeapSizeForBlockKeys(long heapSize) {
      // Calculating the size of blockKeys
      if (blockKeys != null) {
        heapSize += ClassSize.REFERENCE;
        // Adding array + references overhead
        heapSize += ClassSize.align(ClassSize.ARRAY + blockKeys.length * ClassSize.REFERENCE);

        // Adding bytes
        for (byte[] key : blockKeys) {
          heapSize += ClassSize.align(ClassSize.ARRAY + key.length);
        }
      }
      return heapSize;
    }

    @Override
    public boolean isEmpty() {
      return blockKeys.length == 0;
    }

    /**
     * @param i
     *          from 0 to {@link #getRootBlockCount() - 1}
     */
    public byte[] getRootBlockKey(int i) {
      return blockKeys[i];
    }

    @Override
    public BlockWithScanInfo loadDataBlockWithScanInfo(Cell key, HFileBlock currentBlock,
                                                       boolean cacheBlocks, boolean pread, boolean isCompaction,
                                                       DataBlockEncoding expectedDataBlockEncoding,
                                                       CachingBlockReader cachingBlockReader) throws IOException {
      // this would not be needed
      return null;
    }

    @Override
    public Cell midkey(CachingBlockReader cachingBlockReader) throws IOException {
      // Not needed here
      return null;
    }

    @Override
    protected void initialize(int numEntries) {
      blockKeys = new byte[numEntries][];
    }

    @Override
    protected void add(final byte[] key, final long offset, final int dataSize) {
      blockOffsets[rootCount] = offset;
      blockKeys[rootCount] = key;
      blockDataSizes[rootCount] = dataSize;
      rootCount++;
    }

    @Override
    public int rootBlockContainingKey(byte[] key, int offset, int length, CellComparator comp) {
      int pos = Bytes.binarySearch(blockKeys, key, offset, length);
      // pos is between -(blockKeys.length + 1) to blockKeys.length - 1, see
      // binarySearch's javadoc.

      if (pos >= 0) {
        // This means this is an exact match with an element of blockKeys.
        assert pos < blockKeys.length;
        return pos;
      }

      // Otherwise, pos = -(i + 1), where blockKeys[i - 1] < key < blockKeys[i],
      // and i is in [0, blockKeys.length]. We are returning j = i - 1 such that
      // blockKeys[j] <= key < blockKeys[j + 1]. In particular, j = -1 if
      // key < blockKeys[0], meaning the file does not contain the given key.

      int i = -pos - 1;
      assert 0 <= i && i <= blockKeys.length;
      return i - 1;
    }

    @Override
    public int rootBlockContainingKey(Cell key) {
      // Should not be called on this because here it deals only with byte[]
      throw new UnsupportedOperationException(
          "Cannot search for a key that is of Cell type. Only plain byte array keys " +
              "can be searched for");
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("size=" + rootCount).append("\n");
      for (int i = 0; i < rootCount; i++) {
        sb.append("key=").append(KeyValue.keyToString(blockKeys[i]))
            .append("\n  offset=").append(blockOffsets[i])
            .append(", dataSize=" + blockDataSizes[i]).append("\n");
      }
      return sb.toString();
    }
  }

  /**
   * An implementation of the BlockIndexReader that deals with block keys which are the key
   * part of a cell like the Data block index or the ROW_COL bloom blocks
   * This needs a comparator to work with the Cells
   */
  static class CellBasedKeyBlockIndexReader extends BlockIndexReader {

    private Cell[] blockKeys;
    /** Pre-computed mid-key */
    private AtomicReference<Cell> midKey = new AtomicReference<>();
    /** Needed doing lookup on blocks. */
    private CellComparator comparator;

    public CellBasedKeyBlockIndexReader(final CellComparator c, final int treeLevel) {
      // Can be null for METAINDEX block
      comparator = c;
      searchTreeLevel = treeLevel;
    }

    @Override
    protected long calculateHeapSizeForBlockKeys(long heapSize) {
      if (blockKeys != null) {
        heapSize += ClassSize.REFERENCE;
        // Adding array + references overhead
        heapSize += ClassSize.align(ClassSize.ARRAY + blockKeys.length * ClassSize.REFERENCE);

        // Adding blockKeys
        for (Cell key : blockKeys) {
          heapSize += ClassSize.align(key.heapSize());
        }
      }
      // Add comparator and the midkey atomicreference
      heapSize += 2 * ClassSize.REFERENCE;
      return heapSize;
    }

    @Override
    public boolean isEmpty() {
      return blockKeys.length == 0;
    }

    /**
     * @param i
     *          from 0 to {@link #getRootBlockCount() - 1}
     */
    public Cell getRootBlockKey(int i) {
      return blockKeys[i];
    }

    @Override
    public BlockWithScanInfo loadDataBlockWithScanInfo(Cell key, HFileBlock currentBlock,
                                                       boolean cacheBlocks, boolean pread, boolean isCompaction,
                                                       DataBlockEncoding expectedDataBlockEncoding,
                                                       CachingBlockReader cachingBlockReader) throws IOException {
      int rootLevelIndex = rootBlockContainingKey(key);
      if (rootLevelIndex < 0 || rootLevelIndex >= blockOffsets.length) {
        return null;
      }

      // the next indexed key
      Cell nextIndexedKey = null;

      // Read the next-level (intermediate or leaf) index block.
      long currentOffset = blockOffsets[rootLevelIndex];
      int currentOnDiskSize = blockDataSizes[rootLevelIndex];

      if (rootLevelIndex < blockKeys.length - 1) {
        nextIndexedKey = blockKeys[rootLevelIndex + 1];
      } else {
        nextIndexedKey = KeyValueScanner.NO_NEXT_INDEXED_KEY;
      }

      int lookupLevel = 1; // How many levels deep we are in our lookup.
      int index = -1;

      HFileBlock block = null;
      KeyOnlyKeyValue tmpNextIndexKV = new KeyValue.KeyOnlyKeyValue();
      while (true) {
        try {
          // Must initialize it with null here, because if don't and once an exception happen in
          // readBlock, then we'll release the previous assigned block twice in the finally block.
          // (See HBASE-22422)
          block = null;
          if (currentBlock != null && currentBlock.getOffset() == currentOffset) {
            // Avoid reading the same block again, even with caching turned off.
            // This is crucial for compaction-type workload which might have
            // caching turned off. This is like a one-block cache inside the
            // scanner.
            block = currentBlock;
          } else {
            // Call HFile's caching block reader API. We always cache index
            // blocks, otherwise we might get terrible performance.
            boolean shouldCache = cacheBlocks || (lookupLevel < searchTreeLevel);
            BlockType expectedBlockType;
            if (lookupLevel < searchTreeLevel - 1) {
              expectedBlockType = BlockType.INTERMEDIATE_INDEX;
            } else if (lookupLevel == searchTreeLevel - 1) {
              expectedBlockType = BlockType.LEAF_INDEX;
            } else {
              // this also accounts for ENCODED_DATA
              expectedBlockType = BlockType.DATA;
            }
            block = cachingBlockReader.readBlock(currentOffset, currentOnDiskSize, shouldCache,
                pread, isCompaction, true, expectedBlockType, expectedDataBlockEncoding);
          }

          if (block == null) {
            throw new IOException("Failed to read block at offset " + currentOffset
                + ", onDiskSize=" + currentOnDiskSize);
          }

          // Found a data block, break the loop and check our level in the tree.
          if (block.getBlockType().isData()) {
            break;
          }

          // Not a data block. This must be a leaf-level or intermediate-level
          // index block. We don't allow going deeper than searchTreeLevel.
          if (++lookupLevel > searchTreeLevel) {
            throw new IOException("Search Tree Level overflow: lookupLevel=" + lookupLevel
                + ", searchTreeLevel=" + searchTreeLevel);
          }

          // Locate the entry corresponding to the given key in the non-root
          // (leaf or intermediate-level) index block.
          ByteBuff buffer = block.getBufferWithoutHeader();
          index = locateNonRootIndexEntry(buffer, key, comparator);
          if (index == -1) {
            // This has to be changed
            // For now change this to key value
            throw new IOException("The key "
                + CellUtil.getCellKeyAsString(key)
                + " is before the" + " first key of the non-root index block " + block);
          }

          currentOffset = buffer.getLong();
          currentOnDiskSize = buffer.getInt();

          // Only update next indexed key if there is a next indexed key in the current level
          byte[] nonRootIndexedKey = getNonRootIndexedKey(buffer, index + 1);
          if (nonRootIndexedKey != null) {
            tmpNextIndexKV.setKey(nonRootIndexedKey, 0, nonRootIndexedKey.length);
            nextIndexedKey = tmpNextIndexKV;
          }
        } finally {
          if (block != null && !block.getBlockType().isData()) {
            // Release the block immediately if it is not the data block
            block.release();
          }
        }
      }

      if (lookupLevel != searchTreeLevel) {
        assert block.getBlockType().isData();
        // Though we have retrieved a data block we have found an issue
        // in the retrieved data block. Hence returned the block so that
        // the ref count can be decremented
        if (block != null) {
          block.release();
        }
        throw new IOException("Reached a data block at level " + lookupLevel
            + " but the number of levels is " + searchTreeLevel);
      }

      // set the next indexed key for the current block.
      return new BlockWithScanInfo(block, nextIndexedKey);
    }

    @Override
    public Cell midkey(CachingBlockReader cachingBlockReader) throws IOException {
      if (rootCount == 0)
        throw new IOException("HFile empty");

      Cell targetMidKey = this.midKey.get();
      if (targetMidKey != null) {
        return targetMidKey;
      }

      if (midLeafBlockOffset >= 0) {
        if (cachingBlockReader == null) {
          throw new IOException("Have to read the middle leaf block but " +
              "no block reader available");
        }

        // Caching, using pread, assuming this is not a compaction.
        HFileBlock midLeafBlock = cachingBlockReader.readBlock(
            midLeafBlockOffset, midLeafBlockOnDiskSize, true, true, false, true,
            BlockType.LEAF_INDEX, null);
        try {
          ByteBuff b = midLeafBlock.getBufferWithoutHeader();
          int numDataBlocks = b.getIntAfterPosition(0);
          int keyRelOffset = b.getIntAfterPosition(Bytes.SIZEOF_INT * (midKeyEntry + 1));
          int keyLen = b.getIntAfterPosition(Bytes.SIZEOF_INT * (midKeyEntry + 2)) - keyRelOffset
              - SECONDARY_INDEX_ENTRY_OVERHEAD;
          int keyOffset =
              Bytes.SIZEOF_INT * (numDataBlocks + 2) + keyRelOffset
                  + SECONDARY_INDEX_ENTRY_OVERHEAD;
          byte[] bytes = b.toBytes(keyOffset, keyLen);
          targetMidKey = new KeyValue.KeyOnlyKeyValue(bytes, 0, bytes.length);
        } finally {
          midLeafBlock.release();
        }
      } else {
        // The middle of the root-level index.
        targetMidKey = blockKeys[rootCount / 2];
      }

      this.midKey.set(targetMidKey);
      return targetMidKey;
    }

    @Override
    protected void initialize(int numEntries) {
      blockKeys = new Cell[numEntries];
    }

    /**
     * Adds a new entry in the root block index. Only used when reading.
     *
     * @param key Last key in the block
     * @param offset file offset where the block is stored
     * @param dataSize the uncompressed data size
     */
    @Override
    protected void add(final byte[] key, final long offset, final int dataSize) {
      blockOffsets[rootCount] = offset;
      // Create the blockKeys as Cells once when the reader is opened
      blockKeys[rootCount] = new KeyValue.KeyOnlyKeyValue(key, 0, key.length);
      blockDataSizes[rootCount] = dataSize;
      rootCount++;
    }

    @Override
    public int rootBlockContainingKey(final byte[] key, int offset, int length,
                                      CellComparator comp) {
      // This should always be called with Cell not with a byte[] key
      throw new UnsupportedOperationException("Cannot find for a key containing plain byte " +
          "array. Only cell based keys can be searched for");
    }

    @Override
    public int rootBlockContainingKey(Cell key) {
      // Here the comparator should not be null as this happens for the root-level block
      int pos = Bytes.binarySearch(blockKeys, key, comparator);
      // pos is between -(blockKeys.length + 1) to blockKeys.length - 1, see
      // binarySearch's javadoc.

      if (pos >= 0) {
        // This means this is an exact match with an element of blockKeys.
        assert pos < blockKeys.length;
        return pos;
      }

      // Otherwise, pos = -(i + 1), where blockKeys[i - 1] < key < blockKeys[i],
      // and i is in [0, blockKeys.length]. We are returning j = i - 1 such that
      // blockKeys[j] <= key < blockKeys[j + 1]. In particular, j = -1 if
      // key < blockKeys[0], meaning the file does not contain the given key.

      int i = -pos - 1;
      assert 0 <= i && i <= blockKeys.length;
      return i - 1;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("size=" + rootCount).append("\n");
      for (int i = 0; i < rootCount; i++) {
        sb.append("key=").append((blockKeys[i]))
            .append("\n  offset=").append(blockOffsets[i])
            .append(", dataSize=" + blockDataSizes[i]).append("\n");
      }
      return sb.toString();
    }
  }

  /**
   * The reader will always hold the root level index in the memory. Index
   * blocks at all other levels will be cached in the LRU cache in practice,
   * although this API does not enforce that.
   *
   * <p>All non-root (leaf and intermediate) index blocks contain what we call a
   * "secondary index": an array of offsets to the entries within the block.
   * This allows us to do binary search for the entry corresponding to the
   * given key without having to deserialize the block.
   */
  static abstract class BlockIndexReader implements HeapSize {

    protected long[] blockOffsets;
    protected int[] blockDataSizes;
    protected int rootCount = 0;

    // Mid-key metadata.
    protected long midLeafBlockOffset = -1;
    protected int midLeafBlockOnDiskSize = -1;
    protected int midKeyEntry = -1;

    /**
     * The number of levels in the block index tree. One if there is only root
     * level, two for root and leaf levels, etc.
     */
    protected int searchTreeLevel;

    /**
     * @return true if the block index is empty.
     */
    public abstract boolean isEmpty();

    /**
     * Verifies that the block index is non-empty and throws an
     * {@link IllegalStateException} otherwise.
     */
    public void ensureNonEmpty() {
      if (isEmpty()) {
        throw new IllegalStateException("Block index is empty or not loaded");
      }
    }

    /**
     * Return the data block which contains this key. This function will only
     * be called when the HFile version is larger than 1.
     *
     * @param key the key we are looking for
     * @param currentBlock the current block, to avoid re-reading the same block
     * @param cacheBlocks
     * @param pread
     * @param isCompaction
     * @param expectedDataBlockEncoding the data block encoding the caller is
     *          expecting the data block to be in, or null to not perform this
     *          check and return the block irrespective of the encoding
     * @return reader a basic way to load blocks
     * @throws IOException
     */
    public HFileBlock seekToDataBlock(final Cell key, HFileBlock currentBlock, boolean cacheBlocks,
                                      boolean pread, boolean isCompaction, DataBlockEncoding expectedDataBlockEncoding,
                                      CachingBlockReader cachingBlockReader) throws IOException {
      BlockWithScanInfo blockWithScanInfo = loadDataBlockWithScanInfo(key, currentBlock,
          cacheBlocks, pread, isCompaction, expectedDataBlockEncoding, cachingBlockReader);
      if (blockWithScanInfo == null) {
        return null;
      } else {
        return blockWithScanInfo.getHFileBlock();
      }
    }

    /**
     * Return the BlockWithScanInfo, a data structure which contains the Data HFileBlock with
     * other scan info such as the key that starts the next HFileBlock. This function will only
     * be called when the HFile version is larger than 1.
     *
     * @param key the key we are looking for
     * @param currentBlock the current block, to avoid re-reading the same block
     * @param expectedDataBlockEncoding the data block encoding the caller is
     *          expecting the data block to be in, or null to not perform this
     *          check and return the block irrespective of the encoding.
     * @return the BlockWithScanInfo which contains the DataBlock with other
     *         scan info such as nextIndexedKey.
     * @throws IOException
     */
    public abstract BlockWithScanInfo loadDataBlockWithScanInfo(Cell key, HFileBlock currentBlock,
                                                                boolean cacheBlocks, boolean pread, boolean isCompaction,
                                                                DataBlockEncoding expectedDataBlockEncoding,
                                                                CachingBlockReader cachingBlockReader) throws IOException;

    /**
     * An approximation to the {@link HFile}'s mid-key. Operates on block
     * boundaries, and does not go inside blocks. In other words, returns the
     * first key of the middle block of the file.
     *
     * @return the first key of the middle block
     */
    public abstract Cell midkey(CachingBlockReader cachingBlockReader) throws IOException;

    /**
     * @param i from 0 to {@link #getRootBlockCount() - 1}
     */
    public long getRootBlockOffset(int i) {
      return blockOffsets[i];
    }

    /**
     * @param i zero-based index of a root-level block
     * @return the on-disk size of the root-level block for version 2, or the
     *         uncompressed size for version 1
     */
    public int getRootBlockDataSize(int i) {
      return blockDataSizes[i];
    }

    /**
     * @return the number of root-level blocks in this block index
     */
    public int getRootBlockCount() {
      return rootCount;
    }

    /**
     * Finds the root-level index block containing the given key.
     *
     * @param key
     *          Key to find
     * @param comp
     *          the comparator to be used
     * @return Offset of block containing <code>key</code> (between 0 and the
     *         number of blocks - 1) or -1 if this file does not contain the
     *         request.
     */
    // When we want to find the meta index block or bloom block for ROW bloom
    // type Bytes.BYTES_RAWCOMPARATOR would be enough. For the ROW_COL bloom case we need the
    // CellComparator.
    public abstract int rootBlockContainingKey(final byte[] key, int offset, int length,
                                               CellComparator comp);

    /**
     * Finds the root-level index block containing the given key.
     *
     * @param key
     *          Key to find
     * @return Offset of block containing <code>key</code> (between 0 and the
     *         number of blocks - 1) or -1 if this file does not contain the
     *         request.
     */
    // When we want to find the meta index block or bloom block for ROW bloom
    // type
    // Bytes.BYTES_RAWCOMPARATOR would be enough. For the ROW_COL bloom case we
    // need the CellComparator.
    public int rootBlockContainingKey(final byte[] key, int offset, int length) {
      return rootBlockContainingKey(key, offset, length, null);
    }

    /**
     * Finds the root-level index block containing the given key.
     *
     * @param key
     *          Key to find
     */
    public abstract int rootBlockContainingKey(final Cell key);

    /**
     * The indexed key at the ith position in the nonRootIndex. The position starts at 0.
     * @param nonRootIndex
     * @param i the ith position
     * @return The indexed key at the ith position in the nonRootIndex.
     */
    protected byte[] getNonRootIndexedKey(ByteBuff nonRootIndex, int i) {
      int numEntries = nonRootIndex.getInt(0);
      if (i < 0 || i >= numEntries) {
        return null;
      }

      // Entries start after the number of entries and the secondary index.
      // The secondary index takes numEntries + 1 ints.
      int entriesOffset = Bytes.SIZEOF_INT * (numEntries + 2);
      // Targetkey's offset relative to the end of secondary index
      int targetKeyRelOffset = nonRootIndex.getInt(
          Bytes.SIZEOF_INT * (i + 1));

      // The offset of the target key in the blockIndex buffer
      int targetKeyOffset = entriesOffset     // Skip secondary index
          + targetKeyRelOffset               // Skip all entries until mid
          + SECONDARY_INDEX_ENTRY_OVERHEAD;  // Skip offset and on-disk-size

      // We subtract the two consecutive secondary index elements, which
      // gives us the size of the whole (offset, onDiskSize, key) tuple. We
      // then need to subtract the overhead of offset and onDiskSize.
      int targetKeyLength = nonRootIndex.getInt(Bytes.SIZEOF_INT * (i + 2)) -
          targetKeyRelOffset - SECONDARY_INDEX_ENTRY_OVERHEAD;

      // TODO check whether we can make BB backed Cell here? So can avoid bytes copy.
      return nonRootIndex.toBytes(targetKeyOffset, targetKeyLength);
    }

    /**
     * Performs a binary search over a non-root level index block. Utilizes the
     * secondary index, which records the offsets of (offset, onDiskSize,
     * firstKey) tuples of all entries.
     *
     * @param key
     *          the key we are searching for offsets to individual entries in
     *          the blockIndex buffer
     * @param nonRootIndex
     *          the non-root index block buffer, starting with the secondary
     *          index. The position is ignored.
     * @return the index i in [0, numEntries - 1] such that keys[i] <= key <
     *         keys[i + 1], if keys is the array of all keys being searched, or
     *         -1 otherwise
     * @throws IOException
     */
    static int binarySearchNonRootIndex(Cell key, ByteBuff nonRootIndex,
                                        CellComparator comparator) {

      int numEntries = nonRootIndex.getIntAfterPosition(0);
      int low = 0;
      int high = numEntries - 1;
      int mid = 0;

      // Entries start after the number of entries and the secondary index.
      // The secondary index takes numEntries + 1 ints.
      int entriesOffset = Bytes.SIZEOF_INT * (numEntries + 2);

      // If we imagine that keys[-1] = -Infinity and
      // keys[numEntries] = Infinity, then we are maintaining an invariant that
      // keys[low - 1] < key < keys[high + 1] while narrowing down the range.
      ByteBufferKeyOnlyKeyValue nonRootIndexkeyOnlyKV = new ByteBufferKeyOnlyKeyValue();
      ObjectIntPair<ByteBuffer> pair = new ObjectIntPair<>();
      while (low <= high) {
        mid = low + ((high - low) >> 1);

        // Midkey's offset relative to the end of secondary index
        int midKeyRelOffset = nonRootIndex.getIntAfterPosition(Bytes.SIZEOF_INT * (mid + 1));

        // The offset of the middle key in the blockIndex buffer
        int midKeyOffset = entriesOffset       // Skip secondary index
            + midKeyRelOffset                  // Skip all entries until mid
            + SECONDARY_INDEX_ENTRY_OVERHEAD;  // Skip offset and on-disk-size

        // We subtract the two consecutive secondary index elements, which
        // gives us the size of the whole (offset, onDiskSize, key) tuple. We
        // then need to subtract the overhead of offset and onDiskSize.
        int midLength = nonRootIndex.getIntAfterPosition(Bytes.SIZEOF_INT * (mid + 2)) -
            midKeyRelOffset - SECONDARY_INDEX_ENTRY_OVERHEAD;

        // we have to compare in this order, because the comparator order
        // has special logic when the 'left side' is a special key.
        // TODO make KeyOnlyKeyValue to be Buffer backed and avoid array() call. This has to be
        // done after HBASE-12224 & HBASE-12282
        // TODO avoid array call.
        nonRootIndex.asSubByteBuffer(midKeyOffset, midLength, pair);
        nonRootIndexkeyOnlyKV.setKey(pair.getFirst(), pair.getSecond(), midLength);
        int cmp = PrivateCellUtil.compareKeyIgnoresMvcc(comparator, key, nonRootIndexkeyOnlyKV);

        // key lives above the midpoint
        if (cmp > 0)
          low = mid + 1; // Maintain the invariant that keys[low - 1] < key
          // key lives below the midpoint
        else if (cmp < 0)
          high = mid - 1; // Maintain the invariant that key < keys[high + 1]
        else
          return mid; // exact match
      }

      // As per our invariant, keys[low - 1] < key < keys[high + 1], meaning
      // that low - 1 < high + 1 and (low - high) <= 1. As per the loop break
      // condition, low >= high + 1. Therefore, low = high + 1.

      if (low != high + 1) {
        throw new IllegalStateException("Binary search broken: low=" + low
            + " " + "instead of " + (high + 1));
      }

      // OK, our invariant says that keys[low - 1] < key < keys[low]. We need to
      // return i such that keys[i] <= key < keys[i + 1]. Therefore i = low - 1.
      int i = low - 1;

      // Some extra validation on the result.
      if (i < -1 || i >= numEntries) {
        throw new IllegalStateException("Binary search broken: result is " +
            i + " but expected to be between -1 and (numEntries - 1) = " +
            (numEntries - 1));
      }

      return i;
    }

    /**
     * Search for one key using the secondary index in a non-root block. In case
     * of success, positions the provided buffer at the entry of interest, where
     * the file offset and the on-disk-size can be read.
     *
     * @param nonRootBlock
     *          a non-root block without header. Initial position does not
     *          matter.
     * @param key
     *          the byte array containing the key
     * @return the index position where the given key was found, otherwise
     *         return -1 in the case the given key is before the first key.
     *
     */
    static int locateNonRootIndexEntry(ByteBuff nonRootBlock, Cell key,
                                       CellComparator comparator) {
      int entryIndex = binarySearchNonRootIndex(key, nonRootBlock, comparator);

      if (entryIndex != -1) {
        int numEntries = nonRootBlock.getIntAfterPosition(0);

        // The end of secondary index and the beginning of entries themselves.
        int entriesOffset = Bytes.SIZEOF_INT * (numEntries + 2);

        // The offset of the entry we are interested in relative to the end of
        // the secondary index.
        int entryRelOffset = nonRootBlock
            .getIntAfterPosition(Bytes.SIZEOF_INT * (1 + entryIndex));

        nonRootBlock.position(entriesOffset + entryRelOffset);
      }

      return entryIndex;
    }

    /**
     * Read in the root-level index from the given input stream. Must match
     * what was written into the root level by
     * {@link BlockIndexWriter#writeIndexBlocks(FSDataOutputStream)} at the
     * offset that function returned.
     *
     * @param in the buffered input stream or wrapped byte input stream
     * @param numEntries the number of root-level index entries
     * @throws IOException
     */
    public void readRootIndex(DataInput in, final int numEntries) throws IOException {
      blockOffsets = new long[numEntries];
      initialize(numEntries);
      blockDataSizes = new int[numEntries];

      // If index size is zero, no index was written.
      if (numEntries > 0) {
        for (int i = 0; i < numEntries; ++i) {
          long offset = in.readLong();
          int dataSize = in.readInt();
          byte[] key = Bytes.readByteArray(in);
          add(key, offset, dataSize);
        }
      }
    }

    protected abstract void initialize(int numEntries);

    protected abstract void add(final byte[] key, final long offset, final int dataSize);

    /**
     * Read in the root-level index from the given input stream. Must match
     * what was written into the root level by
     * {@link BlockIndexWriter#writeIndexBlocks(FSDataOutputStream)} at the
     * offset that function returned.
     *
     * @param blk the HFile block
     * @param numEntries the number of root-level index entries
     * @return the buffered input stream or wrapped byte input stream
     * @throws IOException
     */
    public DataInputStream readRootIndex(HFileBlock blk, final int numEntries) throws IOException {
      DataInputStream in = blk.getByteStream();
      readRootIndex(in, numEntries);
      return in;
    }

    /**
     * Read the root-level metadata of a multi-level block index. Based on
     * {@link #readRootIndex(DataInput, int)}, but also reads metadata
     * necessary to compute the mid-key in a multi-level index.
     *
     * @param blk the HFile block
     * @param numEntries the number of root-level index entries
     * @throws IOException
     */
    public void readMultiLevelIndexRoot(HFileBlock blk,
                                        final int numEntries) throws IOException {
      DataInputStream in = readRootIndex(blk, numEntries);
      // after reading the root index the checksum bytes have to
      // be subtracted to know if the mid key exists.
      int checkSumBytes = blk.totalChecksumBytes();
      if ((in.available() - checkSumBytes) < MID_KEY_METADATA_SIZE) {
        // No mid-key metadata available.
        return;
      }
      midLeafBlockOffset = in.readLong();
      midLeafBlockOnDiskSize = in.readInt();
      midKeyEntry = in.readInt();
    }

    @Override
    public long heapSize() {
      // The BlockIndexReader does not have the blockKey, comparator and the midkey atomic reference
      long heapSize = ClassSize.align(3 * ClassSize.REFERENCE +
          2 * Bytes.SIZEOF_INT + ClassSize.OBJECT);

      // Mid-key metadata.
      heapSize += MID_KEY_METADATA_SIZE;

      heapSize = calculateHeapSizeForBlockKeys(heapSize);

      if (blockOffsets != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockOffsets.length
            * Bytes.SIZEOF_LONG);
      }

      if (blockDataSizes != null) {
        heapSize += ClassSize.align(ClassSize.ARRAY + blockDataSizes.length
            * Bytes.SIZEOF_INT);
      }

      return ClassSize.align(heapSize);
    }

    protected abstract long calculateHeapSizeForBlockKeys(long heapSize);
  }

  /**
   * Writes the block index into the output stream. Generate the tree from
   * bottom up. The leaf level is written to disk as a sequence of inline
   * blocks, if it is larger than a certain number of bytes. If the leaf level
   * is not large enough, we write all entries to the root level instead.
   *
   * After all leaf blocks have been written, we end up with an index
   * referencing the resulting leaf index blocks. If that index is larger than
   * the allowed root index size, the writer will break it up into
   * reasonable-size intermediate-level index block chunks write those chunks
   * out, and create another index referencing those chunks. This will be
   * repeated until the remaining index is small enough to become the root
   * index. However, in most practical cases we will only have leaf-level
   * blocks and the root index, or just the root index.
   */
  public static class BlockIndexWriter implements InlineBlockWriter {
    /**
     * While the index is being written, this represents the current block
     * index referencing all leaf blocks, with one exception. If the file is
     * being closed and there are not enough blocks to complete even a single
     * leaf block, no leaf blocks get written and this contains the entire
     * block index. After all levels of the index were written by
     * {@link #writeIndexBlocks(FSDataOutputStream)}, this contains the final
     * root-level index.
     */
    private BlockIndexChunk rootChunk = new BlockIndexChunk();

    /**
     * Current leaf-level chunk. New entries referencing data blocks get added
     * to this chunk until it grows large enough to be written to disk.
     */
    private BlockIndexChunk curInlineChunk = new BlockIndexChunk();

    /**
     * The number of block index levels. This is one if there is only root
     * level (even empty), two if there a leaf level and root level, and is
     * higher if there are intermediate levels. This is only final after
     * {@link #writeIndexBlocks(FSDataOutputStream)} has been called. The
     * initial value accounts for the root level, and will be increased to two
     * as soon as we find out there is a leaf-level in
     * {@link #blockWritten(long, int, int)}.
     */
    private int numLevels = 1;

    private HFileBlock.Writer blockWriter;
    private byte[] firstKey = null;

    /**
     * The total number of leaf-level entries, i.e. entries referenced by
     * leaf-level blocks. For the data block index this is equal to the number
     * of data blocks.
     */
    private long totalNumEntries;

    /** Total compressed size of all index blocks. */
    private long totalBlockOnDiskSize;

    /** Total uncompressed size of all index blocks. */
    private long totalBlockUncompressedSize;

    /** The maximum size guideline of all multi-level index blocks. */
    private int maxChunkSize;

    /** The maximum level of multi-level index blocks */
    private int minIndexNumEntries;

    /** Whether we require this block index to always be single-level. */
    private boolean singleLevelOnly;

    /** CacheConfig, or null if cache-on-write is disabled */
    private CacheConfig cacheConf;

    /** Name to use for computing cache keys */
    private String nameForCaching;

    /** Creates a single-level block index writer */
    public BlockIndexWriter() {
      this(null, null, null);
      singleLevelOnly = true;
    }

    /**
     * Creates a multi-level block index writer.
     *
     * @param blockWriter the block writer to use to write index blocks
     * @param cacheConf used to determine when and how a block should be cached-on-write.
     */
    public BlockIndexWriter(HFileBlock.Writer blockWriter,
                            CacheConfig cacheConf, String nameForCaching) {
      if ((cacheConf == null) != (nameForCaching == null)) {
        throw new IllegalArgumentException("Block cache and file name for " +
            "caching must be both specified or both null");
      }

      this.blockWriter = blockWriter;
      this.cacheConf = cacheConf;
      this.nameForCaching = nameForCaching;
      this.maxChunkSize = HFileBlockIndex.DEFAULT_MAX_CHUNK_SIZE;
      this.minIndexNumEntries = HFileBlockIndex.DEFAULT_MIN_INDEX_NUM_ENTRIES;
    }

    public void setMaxChunkSize(int maxChunkSize) {
      if (maxChunkSize <= 0) {
        throw new IllegalArgumentException("Invalid maximum index block size");
      }
      this.maxChunkSize = maxChunkSize;
    }

    public void setMinIndexNumEntries(int minIndexNumEntries) {
      if (minIndexNumEntries <= 1) {
        throw new IllegalArgumentException("Invalid maximum index level, should be >= 2");
      }
      this.minIndexNumEntries = minIndexNumEntries;
    }

    /**
     * Writes the root level and intermediate levels of the block index into
     * the output stream, generating the tree from bottom up. Assumes that the
     * leaf level has been inline-written to the disk if there is enough data
     * for more than one leaf block. We iterate by breaking the current level
     * of the block index, starting with the index of all leaf-level blocks,
     * into chunks small enough to be written to disk, and generate its parent
     * level, until we end up with a level small enough to become the root
     * level.
     *
     * If the leaf level is not large enough, there is no inline block index
     * anymore, so we only write that level of block index to disk as the root
     * level.
     *
     * @param out FSDataOutputStream
     * @return position at which we entered the root-level index.
     * @throws IOException
     */
    public long writeIndexBlocks(FSDataOutputStream out) throws IOException {
      if (curInlineChunk != null && curInlineChunk.getNumEntries() != 0) {
        throw new IOException("Trying to write a multi-level block index, " +
            "but are " + curInlineChunk.getNumEntries() + " entries in the " +
            "last inline chunk.");
      }

      // We need to get mid-key metadata before we create intermediate
      // indexes and overwrite the root chunk.
      byte[] midKeyMetadata = numLevels > 1 ? rootChunk.getMidKeyMetadata()
          : null;

      if (curInlineChunk != null) {
        while (rootChunk.getRootSize() > maxChunkSize
            // HBASE-16288: if firstKey is larger than maxChunkSize we will loop indefinitely
            && rootChunk.getNumEntries() > minIndexNumEntries
            // Sanity check. We will not hit this (minIndexNumEntries ^ 16) blocks can be addressed
            && numLevels < 16) {
          rootChunk = writeIntermediateLevel(out, rootChunk);
          numLevels += 1;
        }
      }

      // write the root level
      long rootLevelIndexPos = out.getPos();

      {
        DataOutput blockStream =
            blockWriter.startWriting(BlockType.ROOT_INDEX);
        rootChunk.writeRoot(blockStream);
        if (midKeyMetadata != null)
          blockStream.write(midKeyMetadata);
        blockWriter.writeHeaderAndData(out);
        if (cacheConf != null) {
          cacheConf.getBlockCache().ifPresent(cache -> {
            HFileBlock blockForCaching = blockWriter.getBlockForCaching(cacheConf);
            cache.cacheBlock(new BlockCacheKey(nameForCaching, rootLevelIndexPos, true,
                blockForCaching.getBlockType()), blockForCaching);
          });
        }
      }

      // Add root index block size
      totalBlockOnDiskSize += blockWriter.getOnDiskSizeWithoutHeader();
      totalBlockUncompressedSize +=
          blockWriter.getUncompressedSizeWithoutHeader();

      if (LOG.isTraceEnabled()) {
        LOG.trace("Wrote a " + numLevels + "-level index with root level at pos "
            + rootLevelIndexPos + ", " + rootChunk.getNumEntries()
            + " root-level entries, " + totalNumEntries + " total entries, "
            + StringUtils.humanReadableInt(this.totalBlockOnDiskSize) +
            " on-disk size, "
            + StringUtils.humanReadableInt(totalBlockUncompressedSize) +
            " total uncompressed size.");
      }
      return rootLevelIndexPos;
    }

    /**
     * Writes the block index data as a single level only. Does not do any
     * block framing.
     *
     * @param out the buffered output stream to write the index to. Typically a
     *          stream writing into an {@link HFile} block.
     * @param description a short description of the index being written. Used
     *          in a log message.
     * @throws IOException
     */
    public void writeSingleLevelIndex(DataOutput out, String description)
        throws IOException {
      expectNumLevels(1);

      if (!singleLevelOnly)
        throw new IOException("Single-level mode is turned off");

      if (rootChunk.getNumEntries() > 0)
        throw new IOException("Root-level entries already added in " +
            "single-level mode");

      rootChunk = curInlineChunk;
      curInlineChunk = new BlockIndexChunk();

      if (LOG.isTraceEnabled()) {
        LOG.trace("Wrote a single-level " + description + " index with "
            + rootChunk.getNumEntries() + " entries, " + rootChunk.getRootSize()
            + " bytes");
      }
      rootChunk.writeRoot(out);
    }

    /**
     * Split the current level of the block index into intermediate index
     * blocks of permitted size and write those blocks to disk. Return the next
     * level of the block index referencing those intermediate-level blocks.
     *
     * @param out
     * @param currentLevel the current level of the block index, such as the a
     *          chunk referencing all leaf-level index blocks
     * @return the parent level block index, which becomes the root index after
     *         a few (usually zero) iterations
     * @throws IOException
     */
    private BlockIndexChunk writeIntermediateLevel(FSDataOutputStream out,
                                                   BlockIndexChunk currentLevel) throws IOException {
      // Entries referencing intermediate-level blocks we are about to create.
      BlockIndexChunk parent = new BlockIndexChunk();

      // The current intermediate-level block index chunk.
      BlockIndexChunk curChunk = new BlockIndexChunk();

      for (int i = 0; i < currentLevel.getNumEntries(); ++i) {
        curChunk.add(currentLevel.getBlockKey(i),
            currentLevel.getBlockOffset(i), currentLevel.getOnDiskDataSize(i));

        // HBASE-16288: We have to have at least minIndexNumEntries(16) items in the index so that
        // we won't end up with too-many levels for a index with very large rowKeys. Also, if the
        // first key is larger than maxChunkSize this will cause infinite recursion.
        if (i >= minIndexNumEntries && curChunk.getRootSize() >= maxChunkSize) {
          writeIntermediateBlock(out, parent, curChunk);
        }
      }

      if (curChunk.getNumEntries() > 0) {
        writeIntermediateBlock(out, parent, curChunk);
      }

      return parent;
    }

    private void writeIntermediateBlock(FSDataOutputStream out,
                                        BlockIndexChunk parent, BlockIndexChunk curChunk) throws IOException {
      long beginOffset = out.getPos();
      DataOutputStream dos = blockWriter.startWriting(
          BlockType.INTERMEDIATE_INDEX);
      curChunk.writeNonRoot(dos);
      byte[] curFirstKey = curChunk.getBlockKey(0);
      blockWriter.writeHeaderAndData(out);

      if (getCacheOnWrite()) {
        cacheConf.getBlockCache().ifPresent(cache -> {
          HFileBlock blockForCaching = blockWriter.getBlockForCaching(cacheConf);
          cache.cacheBlock(
              new BlockCacheKey(nameForCaching, beginOffset, true, blockForCaching.getBlockType()),
              blockForCaching);
        });
      }

      // Add intermediate index block size
      totalBlockOnDiskSize += blockWriter.getOnDiskSizeWithoutHeader();
      totalBlockUncompressedSize +=
          blockWriter.getUncompressedSizeWithoutHeader();

      // OFFSET is the beginning offset the chunk of block index entries.
      // SIZE is the total byte size of the chunk of block index entries
      // + the secondary index size
      // FIRST_KEY is the first key in the chunk of block index
      // entries.
      parent.add(curFirstKey, beginOffset,
          blockWriter.getOnDiskSizeWithHeader());

      // clear current block index chunk
      curChunk.clear();
      curFirstKey = null;
    }

    /**
     * @return how many block index entries there are in the root level
     */
    public final int getNumRootEntries() {
      return rootChunk.getNumEntries();
    }

    /**
     * @return the number of levels in this block index.
     */
    public int getNumLevels() {
      return numLevels;
    }

    private void expectNumLevels(int expectedNumLevels) {
      if (numLevels != expectedNumLevels) {
        throw new IllegalStateException("Number of block index levels is "
            + numLevels + "but is expected to be " + expectedNumLevels);
      }
    }

    /**
     * Whether there is an inline block ready to be written. In general, we
     * write an leaf-level index block as an inline block as soon as its size
     * as serialized in the non-root format reaches a certain threshold.
     */
    @Override
    public boolean shouldWriteBlock(boolean closing) {
      if (singleLevelOnly) {
        throw new UnsupportedOperationException(INLINE_BLOCKS_NOT_ALLOWED);
      }

      if (curInlineChunk == null) {
        throw new IllegalStateException("curInlineChunk is null; has shouldWriteBlock been " +
            "called with closing=true and then called again?");
      }

      if (curInlineChunk.getNumEntries() == 0) {
        return false;
      }

      // We do have some entries in the current inline chunk.
      if (closing) {
        if (rootChunk.getNumEntries() == 0) {
          // We did not add any leaf-level blocks yet. Instead of creating a
          // leaf level with one block, move these entries to the root level.

          expectNumLevels(1);
          rootChunk = curInlineChunk;
          curInlineChunk = null;  // Disallow adding any more index entries.
          return false;
        }

        return true;
      } else {
        return curInlineChunk.getNonRootSize() >= maxChunkSize;
      }
    }

    /**
     * Write out the current inline index block. Inline blocks are non-root
     * blocks, so the non-root index format is used.
     *
     * @param out
     */
    @Override
    public void writeInlineBlock(DataOutput out) throws IOException {
      if (singleLevelOnly)
        throw new UnsupportedOperationException(INLINE_BLOCKS_NOT_ALLOWED);

      // Write the inline block index to the output stream in the non-root
      // index block format.
      curInlineChunk.writeNonRoot(out);

      // Save the first key of the inline block so that we can add it to the
      // parent-level index.
      firstKey = curInlineChunk.getBlockKey(0);

      // Start a new inline index block
      curInlineChunk.clear();
    }

    /**
     * Called after an inline block has been written so that we can add an
     * entry referring to that block to the parent-level index.
     */
    @Override
    public void blockWritten(long offset, int onDiskSize, int uncompressedSize) {
      // Add leaf index block size
      totalBlockOnDiskSize += onDiskSize;
      totalBlockUncompressedSize += uncompressedSize;

      if (singleLevelOnly)
        throw new UnsupportedOperationException(INLINE_BLOCKS_NOT_ALLOWED);

      if (firstKey == null) {
        throw new IllegalStateException("Trying to add second-level index " +
            "entry with offset=" + offset + " and onDiskSize=" + onDiskSize +
            "but the first key was not set in writeInlineBlock");
      }

      if (rootChunk.getNumEntries() == 0) {
        // We are writing the first leaf block, so increase index level.
        expectNumLevels(1);
        numLevels = 2;
      }

      // Add another entry to the second-level index. Include the number of
      // entries in all previous leaf-level chunks for mid-key calculation.
      rootChunk.add(firstKey, offset, onDiskSize, totalNumEntries);
      firstKey = null;
    }

    @Override
    public BlockType getInlineBlockType() {
      return BlockType.LEAF_INDEX;
    }

    /**
     * Add one index entry to the current leaf-level block. When the leaf-level
     * block gets large enough, it will be flushed to disk as an inline block.
     *
     * @param firstKey the first key of the data block
     * @param blockOffset the offset of the data block
     * @param blockDataSize the on-disk size of the data block ({@link HFile}
     *          format version 2), or the uncompressed size of the data block (
     *          {@link HFile} format version 1).
     */
    public void addEntry(byte[] firstKey, long blockOffset, int blockDataSize) {
      curInlineChunk.add(firstKey, blockOffset, blockDataSize);
      ++totalNumEntries;
    }

    /**
     * @throws IOException if we happened to write a multi-level index.
     */
    public void ensureSingleLevel() throws IOException {
      if (numLevels > 1) {
        throw new IOException ("Wrote a " + numLevels + "-level index with " +
            rootChunk.getNumEntries() + " root-level entries, but " +
            "this is expected to be a single-level block index.");
      }
    }

    /**
     * @return true if we are using cache-on-write. This is configured by the
     *         caller of the constructor by either passing a valid block cache
     *         or null.
     */
    @Override
    public boolean getCacheOnWrite() {
      return cacheConf != null && cacheConf.shouldCacheIndexesOnWrite();
    }

    /**
     * The total uncompressed size of the root index block, intermediate-level
     * index blocks, and leaf-level index blocks.
     *
     * @return the total uncompressed size of all index blocks
     */
    public long getTotalUncompressedSize() {
      return totalBlockUncompressedSize;
    }

  }

  /**
   * A single chunk of the block index in the process of writing. The data in
   * this chunk can become a leaf-level, intermediate-level, or root index
   * block.
   */
  static class BlockIndexChunk {

    /** First keys of the key range corresponding to each index entry. */
    private final List<byte[]> blockKeys = new ArrayList<>();

    /** Block offset in backing stream. */
    private final List<Long> blockOffsets = new ArrayList<>();

    /** On-disk data sizes of lower-level data or index blocks. */
    private final List<Integer> onDiskDataSizes = new ArrayList<>();

    /**
     * The cumulative number of sub-entries, i.e. entries on deeper-level block
     * index entries. numSubEntriesAt[i] is the number of sub-entries in the
     * blocks corresponding to this chunk's entries #0 through #i inclusively.
     */
    private final List<Long> numSubEntriesAt = new ArrayList<>();

    /**
     * The offset of the next entry to be added, relative to the end of the
     * "secondary index" in the "non-root" format representation of this index
     * chunk. This is the next value to be added to the secondary index.
     */
    private int curTotalNonRootEntrySize = 0;

    /**
     * The accumulated size of this chunk if stored in the root index format.
     */
    private int curTotalRootSize = 0;

    /**
     * The "secondary index" used for binary search over variable-length
     * records in a "non-root" format block. These offsets are relative to the
     * end of this secondary index.
     */
    private final List<Integer> secondaryIndexOffsetMarks = new ArrayList<>();

    /**
     * Adds a new entry to this block index chunk.
     *
     * @param firstKey the first key in the block pointed to by this entry
     * @param blockOffset the offset of the next-level block pointed to by this
     *          entry
     * @param onDiskDataSize the on-disk data of the block pointed to by this
     *          entry, including header size
     * @param curTotalNumSubEntries if this chunk is the root index chunk under
     *          construction, this specifies the current total number of
     *          sub-entries in all leaf-level chunks, including the one
     *          corresponding to the second-level entry being added.
     */
    void add(byte[] firstKey, long blockOffset, int onDiskDataSize,
             long curTotalNumSubEntries) {
      // Record the offset for the secondary index
      secondaryIndexOffsetMarks.add(curTotalNonRootEntrySize);
      curTotalNonRootEntrySize += SECONDARY_INDEX_ENTRY_OVERHEAD
          + firstKey.length;

      curTotalRootSize += Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT
          + WritableUtils.getVIntSize(firstKey.length) + firstKey.length;

      blockKeys.add(firstKey);
      blockOffsets.add(blockOffset);
      onDiskDataSizes.add(onDiskDataSize);

      if (curTotalNumSubEntries != -1) {
        numSubEntriesAt.add(curTotalNumSubEntries);

        // Make sure the parallel arrays are in sync.
        if (numSubEntriesAt.size() != blockKeys.size()) {
          throw new IllegalStateException("Only have key/value count " +
              "stats for " + numSubEntriesAt.size() + " block index " +
              "entries out of " + blockKeys.size());
        }
      }
    }

    /**
     * The same as {@link #add(byte[], long, int, long)} but does not take the
     * key/value into account. Used for single-level indexes.
     *
     * @see #add(byte[], long, int, long)
     */
    public void add(byte[] firstKey, long blockOffset, int onDiskDataSize) {
      add(firstKey, blockOffset, onDiskDataSize, -1);
    }

    public void clear() {
      blockKeys.clear();
      blockOffsets.clear();
      onDiskDataSizes.clear();
      secondaryIndexOffsetMarks.clear();
      numSubEntriesAt.clear();
      curTotalNonRootEntrySize = 0;
      curTotalRootSize = 0;
    }

    /**
     * Finds the entry corresponding to the deeper-level index block containing
     * the given deeper-level entry (a "sub-entry"), assuming a global 0-based
     * ordering of sub-entries.
     *
     * <p>
     * <i> Implementation note. </i> We are looking for i such that
     * numSubEntriesAt[i - 1] <= k < numSubEntriesAt[i], because a deeper-level
     * block #i (0-based) contains sub-entries # numSubEntriesAt[i - 1]'th
     * through numSubEntriesAt[i] - 1, assuming a global 0-based ordering of
     * sub-entries. i is by definition the insertion point of k in
     * numSubEntriesAt.
     *
     * @param k sub-entry index, from 0 to the total number sub-entries - 1
     * @return the 0-based index of the entry corresponding to the given
     *         sub-entry
     */
    public int getEntryBySubEntry(long k) {
      // We define mid-key as the key corresponding to k'th sub-entry
      // (0-based).

      int i = Collections.binarySearch(numSubEntriesAt, k);

      // Exact match: cumulativeWeight[i] = k. This means chunks #0 through
      // #i contain exactly k sub-entries, and the sub-entry #k (0-based)
      // is in the (i + 1)'th chunk.
      if (i >= 0)
        return i + 1;

      // Inexact match. Return the insertion point.
      return -i - 1;
    }

    /**
     * Used when writing the root block index of a multi-level block index.
     * Serializes additional information allowing to efficiently identify the
     * mid-key.
     *
     * @return a few serialized fields for finding the mid-key
     * @throws IOException if could not create metadata for computing mid-key
     */
    public byte[] getMidKeyMetadata() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream(
          MID_KEY_METADATA_SIZE);
      DataOutputStream baosDos = new DataOutputStream(baos);
      long totalNumSubEntries = numSubEntriesAt.get(blockKeys.size() - 1);
      if (totalNumSubEntries == 0) {
        throw new IOException("No leaf-level entries, mid-key unavailable");
      }
      long midKeySubEntry = (totalNumSubEntries - 1) / 2;
      int midKeyEntry = getEntryBySubEntry(midKeySubEntry);

      baosDos.writeLong(blockOffsets.get(midKeyEntry));
      baosDos.writeInt(onDiskDataSizes.get(midKeyEntry));

      long numSubEntriesBefore = midKeyEntry > 0
          ? numSubEntriesAt.get(midKeyEntry - 1) : 0;
      long subEntryWithinEntry = midKeySubEntry - numSubEntriesBefore;
      if (subEntryWithinEntry < 0 || subEntryWithinEntry > Integer.MAX_VALUE)
      {
        throw new IOException("Could not identify mid-key index within the "
            + "leaf-level block containing mid-key: out of range ("
            + subEntryWithinEntry + ", numSubEntriesBefore="
            + numSubEntriesBefore + ", midKeySubEntry=" + midKeySubEntry
            + ")");
      }

      baosDos.writeInt((int) subEntryWithinEntry);

      if (baosDos.size() != MID_KEY_METADATA_SIZE) {
        throw new IOException("Could not write mid-key metadata: size=" +
            baosDos.size() + ", correct size: " + MID_KEY_METADATA_SIZE);
      }

      // Close just to be good citizens, although this has no effect.
      baos.close();

      return baos.toByteArray();
    }

    /**
     * Writes the block index chunk in the non-root index block format. This
     * format contains the number of entries, an index of integer offsets
     * for quick binary search on variable-length records, and tuples of
     * block offset, on-disk block size, and the first key for each entry.
     *
     * @param out
     * @throws IOException
     */
    void writeNonRoot(DataOutput out) throws IOException {
      // The number of entries in the block.
      out.writeInt(blockKeys.size());

      if (secondaryIndexOffsetMarks.size() != blockKeys.size()) {
        throw new IOException("Corrupted block index chunk writer: " +
            blockKeys.size() + " entries but " +
            secondaryIndexOffsetMarks.size() + " secondary index items");
      }

      // For each entry, write a "secondary index" of relative offsets to the
      // entries from the end of the secondary index. This works, because at
      // read time we read the number of entries and know where the secondary
      // index ends.
      for (int currentSecondaryIndex : secondaryIndexOffsetMarks)
        out.writeInt(currentSecondaryIndex);

      // We include one other element in the secondary index to calculate the
      // size of each entry more easily by subtracting secondary index elements.
      out.writeInt(curTotalNonRootEntrySize);

      for (int i = 0; i < blockKeys.size(); ++i) {
        out.writeLong(blockOffsets.get(i));
        out.writeInt(onDiskDataSizes.get(i));
        out.write(blockKeys.get(i));
      }
    }

    /**
     * @return the size of this chunk if stored in the non-root index block
     *         format
     */
    int getNonRootSize() {
      return Bytes.SIZEOF_INT                          // Number of entries
          + Bytes.SIZEOF_INT * (blockKeys.size() + 1)  // Secondary index
          + curTotalNonRootEntrySize;                  // All entries
    }

    /**
     * Writes this chunk into the given output stream in the root block index
     * format. This format is similar to the {@link HFile} version 1 block
     * index format, except that we store on-disk size of the block instead of
     * its uncompressed size.
     *
     * @param out the data output stream to write the block index to. Typically
     *          a stream writing into an {@link HFile} block.
     * @throws IOException
     */
    void writeRoot(DataOutput out) throws IOException {
      for (int i = 0; i < blockKeys.size(); ++i) {
        out.writeLong(blockOffsets.get(i));
        out.writeInt(onDiskDataSizes.get(i));
        Bytes.writeByteArray(out, blockKeys.get(i));
      }
    }

    /**
     * @return the size of this chunk if stored in the root index block format
     */
    int getRootSize() {
      return curTotalRootSize;
    }

    /**
     * @return the number of entries in this block index chunk
     */
    public int getNumEntries() {
      return blockKeys.size();
    }

    public byte[] getBlockKey(int i) {
      return blockKeys.get(i);
    }

    public long getBlockOffset(int i) {
      return blockOffsets.get(i);
    }

    public int getOnDiskDataSize(int i) {
      return onDiskDataSizes.get(i);
    }

    public long getCumulativeNumKV(int i) {
      if (i < 0)
        return 0;
      return numSubEntriesAt.get(i);
    }

  }

  public static int getMaxChunkSize(Configuration conf) {
    return conf.getInt(MAX_CHUNK_SIZE_KEY, DEFAULT_MAX_CHUNK_SIZE);
  }

  public static int getMinIndexNumEntries(Configuration conf) {
    return conf.getInt(MIN_INDEX_NUM_ENTRIES_KEY, DEFAULT_MIN_INDEX_NUM_ENTRIES);
  }
}
