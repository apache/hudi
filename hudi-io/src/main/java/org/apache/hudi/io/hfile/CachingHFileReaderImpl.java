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

import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.SeekableDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * HFile reader implementation with integrated caching functionality.
 * Uses shared caches across all instances to maximize cache hits when multiple readers access the same file.
 */
public class CachingHFileReaderImpl extends HFileReaderImpl {

  private final String fileIdentity;
  private final boolean cacheTrailerAndLoadOnOpenBlocks;
  private final HFileReaderCacheManager cacheManager;

  /**
   * @param lazyStream                      the file content, opened on first use
   * @param lazyFileSize                    the file size, resolved on first use
   * @param fileIdentity                    identifies the file content in the shared caches; it must
   *                                        change when a different file is written to the same path
   * @param cacheTrailerAndLoadOnOpenBlocks whether to cache the trailer and the "load-on-open"
   *                                        section as well as the blocks; false for content that is
   *                                        already in memory, where there is no I/O to save
   * @param cacheManager                    the shared caches
   */
  public CachingHFileReaderImpl(Lazy<SeekableDataInputStream> lazyStream,
                                Lazy<Long> lazyFileSize,
                                String fileIdentity,
                                boolean cacheTrailerAndLoadOnOpenBlocks,
                                HFileReaderCacheManager cacheManager) {
    super(lazyStream, lazyFileSize);
    this.fileIdentity = fileIdentity;
    this.cacheTrailerAndLoadOnOpenBlocks = cacheTrailerAndLoadOnOpenBlocks;
    this.cacheManager = cacheManager;
  }

  @Override
  protected HFileTrailerAndLoadOnOpenBlocks getTrailerAndLoadOnOpenBlocks() throws IOException {
    if (!cacheTrailerAndLoadOnOpenBlocks) {
      return readTrailerAndLoadOnOpenBlocks();
    }
    return cacheManager.getOrLoadTrailerAndLoadOnOpenBlocks(fileIdentity, this::readTrailerAndLoadOnOpenBlocks);
  }

  @Override
  public Option<ByteBuffer> getMetaBlock(String metaBlockName) throws IOException {
    initializeMetadata();
    BlockIndexEntry blockIndexEntry = metaBlockIndexEntryMap.get(new UTF8StringKey(metaBlockName));
    if (blockIndexEntry == null) {
      return Option.empty();
    }
    HFileMetaBlock block = getOrComputeBlock(
        blockIndexEntry.getOffset(), blockIndexEntry.getSize(), HFileBlockType.META, HFileMetaBlock.class);
    return Option.of(block.readContent());
  }

  @Override
  public HFileDataBlock instantiateHFileDataBlock(BlockIndexEntry blockToRead) throws IOException {
    return getOrComputeBlock(
        blockToRead.getOffset(), blockToRead.getSize(), HFileBlockType.DATA, HFileDataBlock.class);
  }

  @Override
  protected List<BlockIndexEntry> readDataBlockIndexEntries(BlockIndexEntry indexEntry,
                                                            HFileBlockType blockType) throws IOException {
    HFileLeafIndexBlock block = getOrComputeBlock(indexEntry.getOffset(), indexEntry.getSize(), blockType, HFileLeafIndexBlock.class);
    return block.readBlockIndex();
  }

  public long getCacheSize() {
    return cacheManager.getBlockCacheSize();
  }

  public void clearCache() {
    cacheManager.clear();
  }

  public String getCacheStats() {
    return cacheManager.getStats();
  }

  /**
   * Clears the global caches. Should only be used for testing.
   */
  public static void resetGlobalCache() {
    HFileReaderCacheManager.reset();
  }

  private <T extends HFileBlock> T getOrComputeBlock(long offset,
                                                     int size,
                                                     HFileBlockType expectedBlockType,
                                                     Class<T> blockClass) throws IOException {
    return cacheManager.getOrComputeBlock(fileIdentity, offset, size, blockClass, () -> {
      HFileBlockReader blockReader = new HFileBlockReader(context, getStream(), offset, offset + size);
      return blockReader.nextBlock(expectedBlockType);
    });
  }
}
