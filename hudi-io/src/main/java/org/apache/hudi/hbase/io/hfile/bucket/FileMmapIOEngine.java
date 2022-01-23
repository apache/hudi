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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hudi.hbase.io.hfile.Cacheable;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hudi.hbase.util.ByteBufferAllocator;
import org.apache.hudi.hbase.util.ByteBufferArray;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO engine that stores data to a file on the specified file system using memory mapping
 * mechanism
 */
@InterfaceAudience.Private
public abstract class FileMmapIOEngine extends PersistentIOEngine {
  static final Logger LOG = LoggerFactory.getLogger(FileMmapIOEngine.class);

  protected final String path;
  protected long size;
  protected ByteBufferArray bufferArray;
  private final FileChannel fileChannel;
  private RandomAccessFile raf = null;

  public FileMmapIOEngine(String filePath, long capacity) throws IOException {
    super(filePath);
    this.path = filePath;
    this.size = capacity;
    long fileSize = 0;
    try {
      raf = new RandomAccessFile(filePath, "rw");
      fileSize = roundUp(capacity, ByteBufferArray.DEFAULT_BUFFER_SIZE);
      File file = new File(filePath);
      // setLength() method will change file's last modified time. So if don't do
      // this check, wrong time will be used when calculating checksum.
      if (file.length() != fileSize) {
        raf.setLength(fileSize);
      }
      fileChannel = raf.getChannel();
      LOG.info("Allocating " + StringUtils.byteDesc(fileSize) + ", on the path:" + filePath);
    } catch (java.io.FileNotFoundException fex) {
      LOG.error("Can't create bucket cache file " + filePath, fex);
      throw fex;
    } catch (IOException ioex) {
      LOG.error(
          "Can't extend bucket cache file; insufficient space for " + StringUtils.byteDesc(fileSize),
          ioex);
      shutdown();
      throw ioex;
    }
    ByteBufferAllocator allocator = new ByteBufferAllocator() {
      AtomicInteger pos = new AtomicInteger(0);

      @Override
      public ByteBuffer allocate(long size) throws IOException {
        ByteBuffer buffer = fileChannel.map(java.nio.channels.FileChannel.MapMode.READ_WRITE,
            pos.getAndIncrement() * size, size);
        return buffer;
      }
    };
    bufferArray = new ByteBufferArray(fileSize, allocator);
  }

  private long roundUp(long n, long to) {
    return ((n + to - 1) / to) * to;
  }

  @Override
  public String toString() {
    return "ioengine=" + this.getClass().getSimpleName() + ", path=" + this.path + ", size="
        + String.format("%,d", this.size);
  }

  /**
   * File IO engine is always able to support persistent storage for the cache
   * @return true
   */
  @Override
  public boolean isPersistent() {
    // TODO : HBASE-21981 needed for persistence to really work
    return true;
  }

  @Override
  public abstract Cacheable read(BucketEntry be) throws IOException;

  /**
   * Transfers data from the given byte buffer to file
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the file where the first byte to be written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    bufferArray.write(offset, ByteBuff.wrap(srcBuffer));
  }

  @Override
  public void write(ByteBuff srcBuffer, long offset) throws IOException {
    bufferArray.write(offset, srcBuffer);
  }

  /**
   * Sync the data to file after writing
   * @throws IOException
   */
  @Override
  public void sync() throws IOException {
    if (fileChannel != null) {
      fileChannel.force(true);
    }
  }

  /**
   * Close the file
   */
  @Override
  public void shutdown() {
    try {
      fileChannel.close();
    } catch (IOException ex) {
      LOG.error("Can't shutdown cleanly", ex);
    }
    try {
      raf.close();
    } catch (IOException ex) {
      LOG.error("Can't shutdown cleanly", ex);
    }
  }
}
