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
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.SeekableDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * HFile reader implementation with integrated caching functionality.
 * Uses shared caches across all instances to maximize cache hits when multiple readers access the same file.
 */
public class CachingHFileReaderImpl extends HFileReaderImpl {

  private final String filePath;
  private final HFileReaderCacheManager cacheManager;

  public CachingHFileReaderImpl(Lazy<SeekableDataInputStream> lazyStream,
                                Lazy<Long> lazyFileSize,
                                String filePath,
                                int blockCacheSize,
                                long cacheMaxWeightBytes,
                                int loadOnOpenCacheSize,
                                int cacheTtlMinutes) {
    super(lazyStream, lazyFileSize);
    this.filePath = filePath;
    this.cacheManager = HFileReaderCacheManager.getInstance(
        blockCacheSize, cacheMaxWeightBytes, loadOnOpenCacheSize, cacheTtlMinutes);
  }

  @Override
  public synchronized void initializeMetadata() throws IOException {
    if (this.isMetadataInitialized) {
      return;
    }

    HFileReaderCacheManager.LoadOnOpenBlocks loadOnOpenBlocks = getOrComputeLoadOnOpenData();
    this.trailer = loadOnOpenBlocks.trailer;
    this.context = HFileContext.builder()
        .compressionCodec(trailer.getCompressionCodec())
        .build();
    this.dataBlockIndexEntryMap =
        readDataBlockIndex(loadOnOpenBlocks.rootDataIndexBlock, trailer.getDataIndexCount(), trailer.getNumDataIndexLevels());
    this.metaBlockIndexEntryMap =
        loadOnOpenBlocks.metaRootIndexBlock.readBlockIndex(trailer.getMetaIndexCount(), true);
    this.fileInfo = loadOnOpenBlocks.fileInfoBlock.readFileInfo();
    this.isMetadataInitialized = true;
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

  private HFileReaderCacheManager.LoadOnOpenBlocks getOrComputeLoadOnOpenData() throws IOException {
    return cacheManager.getOrComputeLoadOnOpenData(filePath, () -> {
      SeekableDataInputStream stream = getStream();
      long fileSize = lazyFileSize.get();
      HFileTrailer loadedTrailer = readTrailer(stream, fileSize);
      HFileContext loadOnOpenContext = HFileContext.builder()
          .compressionCodec(loadedTrailer.getCompressionCodec())
          .build();
      HFileBlockReader blockReader = new HFileBlockReader(loadOnOpenContext, stream,
          loadedTrailer.getLoadOnOpenDataOffset(), fileSize - HFileTrailer.getTrailerSize());
      HFileRootIndexBlock rootDataIndexBlock = (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
      HFileRootIndexBlock metaRootIndexBlock = (HFileRootIndexBlock) blockReader.nextBlock(HFileBlockType.ROOT_INDEX);
      HFileFileInfoBlock fileInfoBlock = (HFileFileInfoBlock) blockReader.nextBlock(HFileBlockType.FILE_INFO);
      return new HFileReaderCacheManager.LoadOnOpenBlocks(
          loadedTrailer, rootDataIndexBlock, metaRootIndexBlock, fileInfoBlock);
    });
  }

  private <T extends HFileBlock> T getOrComputeBlock(long offset,
                                                     int size,
                                                     HFileBlockType expectedBlockType,
                                                     Class<T> blockClass) throws IOException {
    return cacheManager.getOrComputeBlock(filePath, offset, size, blockClass, () -> {
      HFileBlockReader blockReader = new HFileBlockReader(context, getStream(), offset, offset + size);
      return blockReader.nextBlock(expectedBlockType);
    });
  }

  /**
   * Opens the underlying stream on first use. The lazy supplier can only surface an open failure
   * as a {@link HoodieIOException}, so it is unwrapped here to keep the reader's {@link IOException} contract.
   */
  private SeekableDataInputStream getStream() throws IOException {
    try {
      return lazyStream.get();
    } catch (HoodieIOException e) {
      if (e.getIOException() != null) {
        throw e.getIOException();
      }
      throw e;
    }
  }
}
