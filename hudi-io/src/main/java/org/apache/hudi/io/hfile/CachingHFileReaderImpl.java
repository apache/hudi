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
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.util.Lazy;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * HFile reader implementation with integrated caching functionality.
 * Uses shared caches across all instances to maximize cache hits when multiple readers access the same file.
 */
@Slf4j
public class CachingHFileReaderImpl extends HFileReaderImpl {

  private static volatile HFileBlockCache GLOBAL_BLOCK_CACHE;
  /**
   * Caches the full load-on-open region as a single file-scoped entry instead of caching the
   * individual metadata blocks in {@link #GLOBAL_BLOCK_CACHE}.
   *
   * <p>The 4 objects in this cache are always loaded together during metadata initialization:
   * trailer, root data index block, meta root index block, and file info block. Treating them as
   * one cache unit keeps the metadata loading path simple:
   * <ul>
   *   <li>Cache hit means {@code initializeMetadata()} can rebuild all reader metadata without
   *   touching the file stream.</li>
   *   <li>Cache miss means we pay a single sequential read for the whole load-on-open region.</li>
   *   <li>All 4 objects share the same lifecycle and expire together, which matches how the reader
   *   consumes them.</li>
   * </ul>
   *
   * <p>This is intentionally separate from the original data block cache. {@link HFileBlockCache}
   * is still the right fit for independently addressable blocks such as DATA, META,
   * INTERMEDIATE_INDEX and LEAF_INDEX, where lookups happen by block offset/size and different
   * blocks are reused on different code paths. The load-on-open objects do not have that access
   * pattern: they are always read as a contiguous file-level header region and are always consumed
   * together to initialize the reader.
   */
  private static volatile Cache<String, LoadOnOpenDataBlocks> GLOBAL_LOAD_ON_OPEN_DATA_CACHE;
  private static volatile Integer INITIAL_BLOCK_CACHE_SIZE;
  private static volatile Integer INITIAL_LOAD_ON_OPEN_DATA_CACHE_SIZE;
  private static volatile Integer INITIAL_CACHE_TTL_MINUTES;
  private static final Object CACHE_LOCK = new Object();

  private final String filePath;

  public CachingHFileReaderImpl(Lazy<SeekableDataInputStream> lazyStream,
                                Lazy<Long> lazyFileSize,
                                String filePath,
                                int blockCacheSize,
                                int indexBlockCacheSize,
                                int cacheTtlMinutes) {
    super(lazyStream, lazyFileSize);
    this.filePath = filePath;
    getGlobalBlockCache(blockCacheSize, cacheTtlMinutes);
    getGlobalLoadOnOpenDataCache(indexBlockCacheSize, cacheTtlMinutes);
  }

  @Override
  public synchronized void initializeMetadata() throws IOException {
    if (this.isMetadataInitialized) {
      return;
    }

    LoadOnOpenDataBlocks loadOnOpenDataBlocks = getOrLoadLoadOnOpenData();
    this.trailer = loadOnOpenDataBlocks.trailer;
    this.context = HFileContext.builder()
        .compressionCodec(trailer.getCompressionCodec())
        .build();
    this.dataBlockIndexEntryMap =
        readDataBlockIndex(loadOnOpenDataBlocks.rootDataIndexBlock, trailer.getDataIndexCount(), trailer.getNumDataIndexLevels());
    this.metaBlockIndexEntryMap =
        loadOnOpenDataBlocks.metaRootIndexBlock.readBlockIndex(trailer.getMetaIndexCount(), true);
    this.fileInfo = loadOnOpenDataBlocks.fileInfoBlock.readFileInfo();
    this.isMetadataInitialized = true;
  }

  @Override
  public Option<ByteBuffer> getMetaBlock(String metaBlockName) throws IOException {
    initializeMetadata();
    BlockIndexEntry blockIndexEntry = metaBlockIndexEntryMap.get(new UTF8StringKey(metaBlockName));
    if (blockIndexEntry == null) {
      return Option.empty();
    }
    HFileMetaBlock block = getOrLoadBlock(
        blockIndexEntry.getOffset(), blockIndexEntry.getSize(), HFileBlockType.META, HFileMetaBlock.class);
    return Option.of(block.readContent());
  }

  @Override
  public HFileDataBlock instantiateHFileDataBlock(BlockIndexEntry blockToRead) throws IOException {
    return getOrLoadBlock(
        blockToRead.getOffset(), blockToRead.getSize(), HFileBlockType.DATA, HFileDataBlock.class);
  }

  public long getCacheSize() {
    return GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.size() : 0;
  }

  public void clearCache() {
    if (GLOBAL_BLOCK_CACHE != null) {
      GLOBAL_BLOCK_CACHE.clear();
    }
    if (GLOBAL_LOAD_ON_OPEN_DATA_CACHE != null) {
      GLOBAL_LOAD_ON_OPEN_DATA_CACHE.invalidateAll();
    }
  }

  public String getCacheStats() {
    long blockCacheSize = GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.size() : 0;
    long loadOnOpenDataCacheSize =
        GLOBAL_LOAD_ON_OPEN_DATA_CACHE != null ? GLOBAL_LOAD_ON_OPEN_DATA_CACHE.estimatedSize() : 0;
    return "HFileReader Cache Stats - Block Cache Size: " + blockCacheSize
        + ", Load On Open Data Cache Size: " + loadOnOpenDataCacheSize;
  }

  public static void resetGlobalCache() {
    synchronized (CACHE_LOCK) {
      if (GLOBAL_BLOCK_CACHE != null) {
        GLOBAL_BLOCK_CACHE.clear();
        GLOBAL_BLOCK_CACHE = null;
      }
      if (GLOBAL_LOAD_ON_OPEN_DATA_CACHE != null) {
        GLOBAL_LOAD_ON_OPEN_DATA_CACHE.invalidateAll();
        GLOBAL_LOAD_ON_OPEN_DATA_CACHE = null;
      }
      INITIAL_BLOCK_CACHE_SIZE = null;
      INITIAL_LOAD_ON_OPEN_DATA_CACHE_SIZE = null;
      INITIAL_CACHE_TTL_MINUTES = null;
    }
  }

  private static HFileBlockCache getGlobalBlockCache(int cacheSize, int cacheTtlMinutes) {
    if (GLOBAL_BLOCK_CACHE == null) {
      synchronized (CACHE_LOCK) {
        if (GLOBAL_BLOCK_CACHE == null) {
          log.info("Initializing global HFileBlockCache with size: {}, TTL: {} minutes.",
              cacheSize, cacheTtlMinutes);
          INITIAL_BLOCK_CACHE_SIZE = cacheSize;
          INITIAL_CACHE_TTL_MINUTES = cacheTtlMinutes;
          GLOBAL_BLOCK_CACHE = new HFileBlockCache(cacheSize, cacheTtlMinutes, TimeUnit.MINUTES);
        } else if (!INITIAL_BLOCK_CACHE_SIZE.equals(cacheSize)
            || !INITIAL_CACHE_TTL_MINUTES.equals(cacheTtlMinutes)) {
          log.warn("HFile block cache is already initialized. The provided configuration is being ignored. "
                  + "Existing config: [Size: {}, TTL: {} mins], Ignored config: [Size: {}, TTL: {} mins].",
              INITIAL_BLOCK_CACHE_SIZE, INITIAL_CACHE_TTL_MINUTES, cacheSize, cacheTtlMinutes);
        }
      }
    }
    return GLOBAL_BLOCK_CACHE;
  }

  private static Cache<String, LoadOnOpenDataBlocks> getGlobalLoadOnOpenDataCache(int cacheSize,
                                                                                  int cacheTtlMinutes) {
    if (GLOBAL_LOAD_ON_OPEN_DATA_CACHE == null) {
      synchronized (CACHE_LOCK) {
        if (GLOBAL_LOAD_ON_OPEN_DATA_CACHE == null) {
          log.info("Initializing global load-on-open data cache with size: {}, TTL: {} minutes.",
              cacheSize, cacheTtlMinutes);
          INITIAL_LOAD_ON_OPEN_DATA_CACHE_SIZE = cacheSize;
          if (INITIAL_CACHE_TTL_MINUTES == null) {
            INITIAL_CACHE_TTL_MINUTES = cacheTtlMinutes;
          }
          GLOBAL_LOAD_ON_OPEN_DATA_CACHE = Caffeine.newBuilder()
              .maximumSize(cacheSize)
              .expireAfterAccess(Duration.ofMinutes(cacheTtlMinutes))
              .build();
        } else if (!INITIAL_LOAD_ON_OPEN_DATA_CACHE_SIZE.equals(cacheSize)
            || !INITIAL_CACHE_TTL_MINUTES.equals(cacheTtlMinutes)) {
          log.warn("HFile load-on-open data cache is already initialized. The provided configuration is being ignored. "
                  + "Existing config: [Size: {}, TTL: {} mins], Ignored config: [Size: {}, TTL: {} mins].",
              INITIAL_LOAD_ON_OPEN_DATA_CACHE_SIZE, INITIAL_CACHE_TTL_MINUTES, cacheSize, cacheTtlMinutes);
        }
      }
    }
    return GLOBAL_LOAD_ON_OPEN_DATA_CACHE;
  }

  private LoadOnOpenDataBlocks getOrLoadLoadOnOpenData() throws IOException {
    LoadOnOpenDataBlocks cached = GLOBAL_LOAD_ON_OPEN_DATA_CACHE.getIfPresent(filePath);
    if (cached != null) {
      return cached;
    }

    SeekableDataInputStream stream = lazyStream.get();
    HFileTrailer loadedTrailer = readTrailer(stream, lazyFileSize.get());
    HFileContext loadOnOpenContext = HFileContext.builder()
        .compressionCodec(loadedTrailer.getCompressionCodec())
        .build();
    HFileBlockReader blockReader = new HFileBlockReader(loadOnOpenContext, stream,
        loadedTrailer.getLoadOnOpenDataOffset(), lazyFileSize.get() - HFileTrailer.getTrailerSize());
    HFileRootIndexBlock rootDataIndexBlock = (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    HFileRootIndexBlock metaRootIndexBlock = (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
    HFileFileInfoBlock fileInfoBlock = (HFileFileInfoBlock) blockReader.nextBlock(HFileBlockType.FILE_INFO);
    LoadOnOpenDataBlocks loaded = new LoadOnOpenDataBlocks(
        loadedTrailer, rootDataIndexBlock, metaRootIndexBlock, fileInfoBlock);
    GLOBAL_LOAD_ON_OPEN_DATA_CACHE.put(filePath, loaded);
    return loaded;
  }

  @Override
  protected List<BlockIndexEntry> readDataBlockIndexEntries(BlockIndexEntry indexEntry,
                                                            HFileBlockType blockType) {
    HFileLeafIndexBlock block = getOrLoadBlock(
        indexEntry.getOffset(), indexEntry.getSize(), blockType, HFileLeafIndexBlock.class);
    return block.readBlockIndex();
  }

  private <T extends HFileBlock> T getOrLoadBlock(long offset,
                                                  int size,
                                                  HFileBlockType expectedBlockType,
                                                  Class<T> blockClass) {
    HFileBlockCache.BlockCacheKey cacheKey = new HFileBlockCache.BlockCacheKey(filePath, offset);
    HFileBlock block = GLOBAL_BLOCK_CACHE.getOrCompute(cacheKey, new BlockLoader(offset, size, expectedBlockType));
    return blockClass.cast(block);
  }

  private class BlockLoader implements Callable<HFileBlock> {
    private final long offset;
    private final int size;
    private final HFileBlockType expectedBlockType;

    private BlockLoader(long offset, int size, HFileBlockType expectedBlockType) {
      this.offset = offset;
      this.size = size;
      this.expectedBlockType = expectedBlockType;
    }

    @Override
    public HFileBlock call() throws Exception {
      HFileBlockReader blockReader = new HFileBlockReader(context, lazyStream.get(), offset, offset + size);
      return blockReader.nextBlock(expectedBlockType);
    }
  }

  /**
   * Materialized representation of the file's load-on-open region.
   *
   * <p>These objects are cached as a whole because they are read from a single contiguous region
   * and are all required to initialize reader metadata.
   */
  private static class LoadOnOpenDataBlocks {
    private final HFileTrailer trailer;
    private final HFileRootIndexBlock rootDataIndexBlock;
    private final HFileRootIndexBlock metaRootIndexBlock;
    private final HFileFileInfoBlock fileInfoBlock;

    private LoadOnOpenDataBlocks(HFileTrailer trailer,
                                 HFileRootIndexBlock rootDataIndexBlock,
                                 HFileRootIndexBlock metaRootIndexBlock,
                                 HFileFileInfoBlock fileInfoBlock) {
      this.trailer = trailer;
      this.rootDataIndexBlock = rootDataIndexBlock;
      this.metaRootIndexBlock = metaRootIndexBlock;
      this.fileInfoBlock = fileInfoBlock;
    }
  }
}
