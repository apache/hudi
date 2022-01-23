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
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.hbase.metrics.impl.FastLongHistogram;
import org.apache.hudi.hbase.util.Bytes;
import org.apache.hudi.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.gson.TypeAdapter;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonReader;
import org.apache.hbase.thirdparty.com.google.gson.stream.JsonWriter;

/**
 * Utilty for aggregating counts in CachedBlocks and toString/toJSON CachedBlocks and BlockCaches.
 * No attempt has been made at making this thread safe.
 */
@InterfaceAudience.Private
public class BlockCacheUtil {

  private static final Logger LOG = LoggerFactory.getLogger(BlockCacheUtil.class);

  public static final long NANOS_PER_SECOND = 1000000000;

  /**
   * Needed generating JSON.
   */
  private static final Gson GSON = GsonUtil.createGson()
      .registerTypeAdapter(FastLongHistogram.class, new TypeAdapter<FastLongHistogram>() {

        @Override
        public void write(JsonWriter out, FastLongHistogram value) throws IOException {
          AgeSnapshot snapshot = new AgeSnapshot(value);
          out.beginObject();
          out.name("mean").value(snapshot.getMean());
          out.name("min").value(snapshot.getMin());
          out.name("max").value(snapshot.getMax());
          out.name("75thPercentile").value(snapshot.get75thPercentile());
          out.name("95thPercentile").value(snapshot.get95thPercentile());
          out.name("98thPercentile").value(snapshot.get98thPercentile());
          out.name("99thPercentile").value(snapshot.get99thPercentile());
          out.name("999thPercentile").value(snapshot.get999thPercentile());
          out.endObject();
        }

        @Override
        public FastLongHistogram read(JsonReader in) throws IOException {
          throw new UnsupportedOperationException();
        }
      }).setPrettyPrinting().create();

  /**
   * @param cb
   * @return The block content as String.
   */
  public static String toString(final CachedBlock cb, final long now) {
    return "filename=" + cb.getFilename() + ", " + toStringMinusFileName(cb, now);
  }

  /**
   * Little data structure to hold counts for a file.
   * Used doing a toJSON.
   */
  static class CachedBlockCountsPerFile {
    private int count = 0;
    private long size = 0;
    private int countData = 0;
    private long sizeData = 0;
    private final String filename;

    CachedBlockCountsPerFile(final String filename) {
      this.filename = filename;
    }

    public int getCount() {
      return count;
    }

    public long getSize() {
      return size;
    }

    public int getCountData() {
      return countData;
    }

    public long getSizeData() {
      return sizeData;
    }

    public String getFilename() {
      return filename;
    }
  }

  /**
   * @return A JSON String of <code>filename</code> and counts of <code>blocks</code>
   */
  public static String toJSON(String filename, NavigableSet<CachedBlock> blocks)
      throws IOException {
    CachedBlockCountsPerFile counts = new CachedBlockCountsPerFile(filename);
    for (CachedBlock cb : blocks) {
      counts.count++;
      counts.size += cb.getSize();
      BlockType bt = cb.getBlockType();
      if (bt != null && bt.isData()) {
        counts.countData++;
        counts.sizeData += cb.getSize();
      }
    }
    return GSON.toJson(counts);
  }

  /**
   * @return JSON string of <code>cbsf</code> aggregated
   */
  public static String toJSON(CachedBlocksByFile cbsbf) throws IOException {
    return GSON.toJson(cbsbf);
  }

  /**
   * @return JSON string of <code>bc</code> content.
   */
  public static String toJSON(BlockCache bc) throws IOException {
    return GSON.toJson(bc);
  }

  /**
   * @param cb
   * @return The block content of <code>bc</code> as a String minus the filename.
   */
  public static String toStringMinusFileName(final CachedBlock cb, final long now) {
    return "offset=" + cb.getOffset() +
        ", size=" + cb.getSize() +
        ", age=" + (now - cb.getCachedTime()) +
        ", type=" + cb.getBlockType() +
        ", priority=" + cb.getBlockPriority();
  }

  /**
   * Get a {@link CachedBlocksByFile} instance and load it up by iterating content in
   * {@link BlockCache}.
   * @param conf Used to read configurations
   * @param bc Block Cache to iterate.
   * @return Laoded up instance of CachedBlocksByFile
   */
  public static CachedBlocksByFile getLoadedCachedBlocksByFile(final Configuration conf,
                                                               final BlockCache bc) {
    CachedBlocksByFile cbsbf = new CachedBlocksByFile(conf);
    for (CachedBlock cb: bc) {
      if (cbsbf.update(cb)) break;
    }
    return cbsbf;
  }

  private static int compareCacheBlock(Cacheable left, Cacheable right,
                                       boolean includeNextBlockMetadata) {
    ByteBuffer l = ByteBuffer.allocate(left.getSerializedLength());
    left.serialize(l, includeNextBlockMetadata);
    ByteBuffer r = ByteBuffer.allocate(right.getSerializedLength());
    right.serialize(r, includeNextBlockMetadata);
    return Bytes.compareTo(l.array(), l.arrayOffset(), l.limit(),
        r.array(), r.arrayOffset(), r.limit());
  }

  /**
   * Validate that the existing and newBlock are the same without including the nextBlockMetadata,
   * if not, throw an exception. If they are the same without the nextBlockMetadata,
   * return the comparison.
   *
   * @param existing block that is existing in the cache.
   * @param newBlock block that is trying to be cached.
   * @param cacheKey the cache key of the blocks.
   * @return comparison of the existing block to the newBlock.
   */
  public static int validateBlockAddition(Cacheable existing, Cacheable newBlock,
                                          BlockCacheKey cacheKey) {
    int comparison = compareCacheBlock(existing, newBlock, false);
    if (comparison != 0) {
      throw new RuntimeException("Cached block contents differ, which should not have happened."
          + "cacheKey:" + cacheKey);
    }
    if ((existing instanceof HFileBlock) && (newBlock instanceof HFileBlock)) {
      comparison = ((HFileBlock) existing).getNextBlockOnDiskSize()
          - ((HFileBlock) newBlock).getNextBlockOnDiskSize();
    }
    return comparison;
  }

  /**
   * Because of the region splitting, it's possible that the split key locate in the middle of a
   * block. So it's possible that both the daughter regions load the same block from their parent
   * HFile. When pread, we don't force the read to read all of the next block header. So when two
   * threads try to cache the same block, it's possible that one thread read all of the next block
   * header but the other one didn't. if the already cached block hasn't next block header but the
   * new block to cache has, then we can replace the existing block with the new block for better
   * performance.(HBASE-20447)
   * @param blockCache BlockCache to check
   * @param cacheKey the block cache key
   * @param newBlock the new block which try to put into the block cache.
   * @return true means need to replace existing block with new block for the same block cache key.
   *         false means just keep the existing block.
   */
  public static boolean shouldReplaceExistingCacheBlock(BlockCache blockCache,
                                                        BlockCacheKey cacheKey, Cacheable newBlock) {
    // NOTICE: The getBlock has retained the existingBlock inside.
    Cacheable existingBlock = blockCache.getBlock(cacheKey, false, false, false);
    if (existingBlock == null) {
      return true;
    }
    try {
      int comparison = BlockCacheUtil.validateBlockAddition(existingBlock, newBlock, cacheKey);
      if (comparison < 0) {
        LOG.warn("Cached block contents differ by nextBlockOnDiskSize, the new block has "
            + "nextBlockOnDiskSize set. Caching new block.");
        return true;
      } else if (comparison > 0) {
        LOG.warn("Cached block contents differ by nextBlockOnDiskSize, the existing block has "
            + "nextBlockOnDiskSize set, Keeping cached block.");
        return false;
      } else {
        LOG.debug("Caching an already cached block: {}. This is harmless and can happen in rare "
                + "cases (see HBASE-8547)",
            cacheKey);
        return false;
      }
    } finally {
      // Release this block to decrement the reference count.
      existingBlock.release();
    }
  }

  /**
   * Use one of these to keep a running account of cached blocks by file.  Throw it away when done.
   * This is different than metrics in that it is stats on current state of a cache.
   * See getLoadedCachedBlocksByFile
   */
  public static class CachedBlocksByFile {
    private int count;
    private int dataBlockCount;
    private long size;
    private long dataSize;
    private final long now = System.nanoTime();
    /**
     * How many blocks to look at before we give up.
     * There could be many millions of blocks. We don't want the
     * ui to freeze while we run through 1B blocks... users will
     * think hbase dead. UI displays warning in red when stats
     * are incomplete.
     */
    private final int max;
    public static final int DEFAULT_MAX = 1000000;

    CachedBlocksByFile() {
      this(null);
    }

    CachedBlocksByFile(final Configuration c) {
      this.max = c == null? DEFAULT_MAX: c.getInt("hbase.ui.blockcache.by.file.max", DEFAULT_MAX);
    }

    /**
     * Map by filename. use concurent utils because we want our Map and contained blocks sorted.
     */
    private transient NavigableMap<String, NavigableSet<CachedBlock>> cachedBlockByFile =
        new ConcurrentSkipListMap<>();
    FastLongHistogram hist = new FastLongHistogram();

    /**
     * @param cb
     * @return True if full.... if we won't be adding any more.
     */
    public boolean update(final CachedBlock cb) {
      if (isFull()) return true;
      NavigableSet<CachedBlock> set = this.cachedBlockByFile.get(cb.getFilename());
      if (set == null) {
        set = new ConcurrentSkipListSet<>();
        this.cachedBlockByFile.put(cb.getFilename(), set);
      }
      set.add(cb);
      this.size += cb.getSize();
      this.count++;
      BlockType bt = cb.getBlockType();
      if (bt != null && bt.isData()) {
        this.dataBlockCount++;
        this.dataSize += cb.getSize();
      }
      long age = (this.now - cb.getCachedTime())/NANOS_PER_SECOND;
      this.hist.add(age, 1);
      return false;
    }

    /**
     * @return True if full; i.e. there are more items in the cache but we only loaded up
     * the maximum set in configuration <code>hbase.ui.blockcache.by.file.max</code>
     * (Default: DEFAULT_MAX).
     */
    public boolean isFull() {
      return this.count >= this.max;
    }

    public NavigableMap<String, NavigableSet<CachedBlock>> getCachedBlockStatsByFile() {
      return this.cachedBlockByFile;
    }

    /**
     * @return count of blocks in the cache
     */
    public int getCount() {
      return count;
    }

    public int getDataCount() {
      return dataBlockCount;
    }

    /**
     * @return size of blocks in the cache
     */
    public long getSize() {
      return size;
    }

    /**
     * @return Size of data.
     */
    public long getDataSize() {
      return dataSize;
    }

    public AgeSnapshot getAgeInCacheSnapshot() {
      return new AgeSnapshot(this.hist);
    }

    @Override
    public String toString() {
      AgeSnapshot snapshot = getAgeInCacheSnapshot();
      return "count=" + count + ", dataBlockCount=" + dataBlockCount + ", size=" + size +
          ", dataSize=" + getDataSize() +
          ", mean age=" + snapshot.getMean() +
          ", min age=" + snapshot.getMin() +
          ", max age=" + snapshot.getMax() +
          ", 75th percentile age="   + snapshot.get75thPercentile() +
          ", 95th percentile age="   + snapshot.get95thPercentile() +
          ", 98th percentile age="   + snapshot.get98thPercentile() +
          ", 99th percentile age="   + snapshot.get99thPercentile() +
          ", 99.9th percentile age=" + snapshot.get99thPercentile();
    }
  }
}
