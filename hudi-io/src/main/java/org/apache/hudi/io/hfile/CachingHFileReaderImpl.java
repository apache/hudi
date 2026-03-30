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
  private static volatile Cache<String, HFileTrailer> GLOBAL_TRAILER_CACHE;
  private static volatile Integer INITIAL_CACHE_SIZE;
  private static volatile Integer INITIAL_CACHE_TTL;
  private static final Object CACHE_LOCK = new Object();

  private final String filePath;

  public CachingHFileReaderImpl(Lazy<SeekableDataInputStream> lazyStream,
                                long fileSize,
                                String filePath,
                                int cacheSize,
                                int cacheTtlMinutes) {
    super(lazyStream, fileSize);
    this.filePath = filePath;
    getGlobalBlockCache(cacheSize, cacheTtlMinutes);
    getGlobalTrailerCache(cacheSize, cacheTtlMinutes);
  }

  @Override
  public synchronized void initializeMetadata() throws IOException {
    if (this.isMetadataInitialized) {
      return;
    }

    this.trailer = getOrLoadTrailer();
    this.context = HFileContext.builder()
        .compressionCodec(trailer.getCompressionCodec())
        .build();

    long rootIndexOffset = trailer.getLoadOnOpenDataOffset();
    HFileRootIndexBlock rootDataIndexBlock =
        getOrLoadMetadataBlock(rootIndexOffset, HFileBlockType.ROOT_INDEX, HFileRootIndexBlock.class);
    this.dataBlockIndexEntryMap = readDataBlockIndex(
        rootDataIndexBlock, trailer.getDataIndexCount(), trailer.getNumDataIndexLevels());

    long metaRootIndexOffset = rootIndexOffset + rootDataIndexBlock.getOnDiskSizeWithHeader();
    HFileRootIndexBlock metaRootIndexBlock =
        getOrLoadMetadataBlock(metaRootIndexOffset, HFileBlockType.ROOT_INDEX, HFileRootIndexBlock.class);
    this.metaBlockIndexEntryMap = metaRootIndexBlock.readBlockIndex(trailer.getMetaIndexCount(), true);

    long fileInfoOffset = metaRootIndexOffset + metaRootIndexBlock.getOnDiskSizeWithHeader();
    HFileFileInfoBlock fileInfoBlock =
        getOrLoadMetadataBlock(fileInfoOffset, HFileBlockType.FILE_INFO, HFileFileInfoBlock.class);
    this.fileInfo = fileInfoBlock.readFileInfo();
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

  @Override
  public void close() throws IOException {
    super.close();
  }

  public long getCacheSize() {
    return GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.size() : 0;
  }

  public void clearCache() {
    if (GLOBAL_BLOCK_CACHE != null) {
      GLOBAL_BLOCK_CACHE.clear();
    }
    if (GLOBAL_TRAILER_CACHE != null) {
      GLOBAL_TRAILER_CACHE.invalidateAll();
    }
  }

  public String getCacheStats() {
    long blockCacheSize = GLOBAL_BLOCK_CACHE != null ? GLOBAL_BLOCK_CACHE.size() : 0;
    long trailerCacheSize = GLOBAL_TRAILER_CACHE != null ? GLOBAL_TRAILER_CACHE.estimatedSize() : 0;
    return "HFileReader Cache Stats - Block Cache Size: " + blockCacheSize
        + ", Trailer Cache Size: " + trailerCacheSize;
  }

  public static void resetGlobalCache() {
    synchronized (CACHE_LOCK) {
      if (GLOBAL_BLOCK_CACHE != null) {
        GLOBAL_BLOCK_CACHE.clear();
        GLOBAL_BLOCK_CACHE = null;
      }
      if (GLOBAL_TRAILER_CACHE != null) {
        GLOBAL_TRAILER_CACHE.invalidateAll();
        GLOBAL_TRAILER_CACHE = null;
      }
      INITIAL_CACHE_SIZE = null;
      INITIAL_CACHE_TTL = null;
    }
  }

  private static HFileBlockCache getGlobalBlockCache(int cacheSize, int cacheTtlMinutes) {
    if (GLOBAL_BLOCK_CACHE == null) {
      synchronized (CACHE_LOCK) {
        if (GLOBAL_BLOCK_CACHE == null) {
          log.info("Initializing global HFileBlockCache with size: {}, TTL: {} minutes.",
              cacheSize, cacheTtlMinutes);
          INITIAL_CACHE_SIZE = cacheSize;
          INITIAL_CACHE_TTL = cacheTtlMinutes;
          GLOBAL_BLOCK_CACHE = new HFileBlockCache(cacheSize, cacheTtlMinutes, TimeUnit.MINUTES);
        } else if (!INITIAL_CACHE_SIZE.equals(cacheSize) || !INITIAL_CACHE_TTL.equals(cacheTtlMinutes)) {
          log.warn("HFile block cache is already initialized. The provided configuration is being ignored. "
                  + "Existing config: [Size: {}, TTL: {} mins], Ignored config: [Size: {}, TTL: {} mins].",
              INITIAL_CACHE_SIZE, INITIAL_CACHE_TTL, cacheSize, cacheTtlMinutes);
        }
      }
    }
    return GLOBAL_BLOCK_CACHE;
  }

  private static Cache<String, HFileTrailer> getGlobalTrailerCache(int cacheSize, int cacheTtlMinutes) {
    if (GLOBAL_TRAILER_CACHE == null) {
      synchronized (CACHE_LOCK) {
        if (GLOBAL_TRAILER_CACHE == null) {
          GLOBAL_TRAILER_CACHE = Caffeine.newBuilder()
              .maximumSize(cacheSize)
              .expireAfterAccess(Duration.ofMinutes(cacheTtlMinutes))
              .build();
        }
      }
    }
    return GLOBAL_TRAILER_CACHE;
  }

  private HFileTrailer getOrLoadTrailer() throws IOException {
    HFileTrailer cached = GLOBAL_TRAILER_CACHE.getIfPresent(filePath);
    if (cached != null) {
      return cached;
    }

    HFileTrailer loadedTrailer = readTrailer(lazyStream.get(), fileSize);
    GLOBAL_TRAILER_CACHE.put(filePath, loadedTrailer);
    return loadedTrailer;
  }

  private <T extends HFileBlock> T getOrLoadMetadataBlock(long offset,
                                                          HFileBlockType expectedBlockType,
                                                          Class<T> blockClass) throws IOException {
    long loadOnOpenEndOffset = fileSize - HFileTrailer.getTrailerSize();
    int size = Math.toIntExact(loadOnOpenEndOffset - offset);
    return getOrLoadBlock(offset, size, expectedBlockType, blockClass);
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
}
