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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link HFile.Reader} to deal with pread.
 */
@InterfaceAudience.Private
public class HFilePreadReader extends HFileReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(HFileReaderImpl.class);

  public HFilePreadReader(ReaderContext context, HFileInfo fileInfo,
                          CacheConfig cacheConf, Configuration conf) throws IOException {
    super(context, fileInfo, cacheConf, conf);
    // Prefetch file blocks upon open if requested
    if (cacheConf.shouldPrefetchOnOpen()) {
      PrefetchExecutor.request(path, new Runnable() {
        @Override
        public void run() {
          long offset = 0;
          long end = 0;
          try {
            end = getTrailer().getLoadOnOpenDataOffset();
            if (LOG.isTraceEnabled()) {
              LOG.trace("Prefetch start " + getPathOffsetEndStr(path, offset, end));
            }
            // Don't use BlockIterator here, because it's designed to read load-on-open section.
            long onDiskSizeOfNextBlock = -1;
            while (offset < end) {
              if (Thread.interrupted()) {
                break;
              }
              // Perhaps we got our block from cache? Unlikely as this may be, if it happens, then
              // the internal-to-hfileblock thread local which holds the overread that gets the
              // next header, will not have happened...so, pass in the onDiskSize gotten from the
              // cached block. This 'optimization' triggers extremely rarely I'd say.
              HFileBlock block = readBlock(offset, onDiskSizeOfNextBlock, /* cacheBlock= */true,
                  /* pread= */true, false, false, null, null);
              try {
                onDiskSizeOfNextBlock = block.getNextBlockOnDiskSize();
                offset += block.getOnDiskSizeWithHeader();
              } finally {
                // Ideally here the readBlock won't find the block in cache. We call this
                // readBlock so that block data is read from FS and cached in BC. we must call
                // returnBlock here to decrease the reference count of block.
                block.release();
              }
            }
          } catch (IOException e) {
            // IOExceptions are probably due to region closes (relocation, etc.)
            if (LOG.isTraceEnabled()) {
              LOG.trace("Prefetch " + getPathOffsetEndStr(path, offset, end), e);
            }
          } catch (NullPointerException e) {
            LOG.warn("Stream moved/closed or prefetch cancelled?" +
                getPathOffsetEndStr(path, offset, end), e);
          } catch (Exception e) {
            // Other exceptions are interesting
            LOG.warn("Prefetch " + getPathOffsetEndStr(path, offset, end), e);
          } finally {
            PrefetchExecutor.complete(path);
          }
        }
      });
    }
  }

  private static String getPathOffsetEndStr(final Path path, final long offset, final long end) {
    return "path=" + path.toString() + ", offset=" + offset + ", end=" + end;
  }

  public void close(boolean evictOnClose) throws IOException {
    PrefetchExecutor.cancel(path);
    // Deallocate blocks in load-on-open section
    this.fileInfo.close();
    // Deallocate data blocks
    cacheConf.getBlockCache().ifPresent(cache -> {
      if (evictOnClose) {
        int numEvicted = cache.evictBlocksByHfileName(name);
        if (LOG.isTraceEnabled()) {
          LOG.trace("On close, file=" + name + " evicted=" + numEvicted + " block(s)");
        }
      }
    });
    fsBlockReader.closeStreams();
  }
}
