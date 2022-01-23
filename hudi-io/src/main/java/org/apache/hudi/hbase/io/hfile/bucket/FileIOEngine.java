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
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hudi.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hudi.hbase.io.hfile.Cacheable;
import org.apache.hudi.hbase.nio.ByteBuff;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * IO engine that stores data to a file on the local file system.
 */
@InterfaceAudience.Private
public class FileIOEngine extends PersistentIOEngine {
  private static final Logger LOG = LoggerFactory.getLogger(FileIOEngine.class);
  public static final String FILE_DELIMITER = ",";
  private final FileChannel[] fileChannels;
  private final RandomAccessFile[] rafs;
  private final ReentrantLock[] channelLocks;

  private final long sizePerFile;
  private final long capacity;

  private FileReadAccessor readAccessor = new FileReadAccessor();
  private FileWriteAccessor writeAccessor = new FileWriteAccessor();

  public FileIOEngine(long capacity, boolean maintainPersistence, String... filePaths)
      throws IOException {
    super(filePaths);
    this.sizePerFile = capacity / filePaths.length;
    this.capacity = this.sizePerFile * filePaths.length;
    this.fileChannels = new FileChannel[filePaths.length];
    if (!maintainPersistence) {
      for (String filePath : filePaths) {
        File file = new File(filePath);
        if (file.exists()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("File " + filePath + " already exists. Deleting!!");
          }
          file.delete();
          // If deletion fails still we can manage with the writes
        }
      }
    }
    this.rafs = new RandomAccessFile[filePaths.length];
    this.channelLocks = new ReentrantLock[filePaths.length];
    for (int i = 0; i < filePaths.length; i++) {
      String filePath = filePaths[i];
      try {
        rafs[i] = new RandomAccessFile(filePath, "rw");
        long totalSpace = new File(filePath).getTotalSpace();
        if (totalSpace < sizePerFile) {
          // The next setting length will throw exception,logging this message
          // is just used for the detail reason of exception，
          String msg = "Only " + StringUtils.byteDesc(totalSpace)
              + " total space under " + filePath + ", not enough for requested "
              + StringUtils.byteDesc(sizePerFile);
          LOG.warn(msg);
        }
        File file = new File(filePath);
        // setLength() method will change file's last modified time. So if don't do
        // this check, wrong time will be used when calculating checksum.
        if (file.length() != sizePerFile) {
          rafs[i].setLength(sizePerFile);
        }
        fileChannels[i] = rafs[i].getChannel();
        channelLocks[i] = new ReentrantLock();
        LOG.info("Allocating cache " + StringUtils.byteDesc(sizePerFile)
            + ", on the path:" + filePath);
      } catch (IOException fex) {
        LOG.error("Failed allocating cache on " + filePath, fex);
        shutdown();
        throw fex;
      }
    }
  }

  @Override
  public String toString() {
    return "ioengine=" + this.getClass().getSimpleName() + ", paths="
        + Arrays.asList(filePaths) + ", capacity=" + String.format("%,d", this.capacity);
  }

  /**
   * File IO engine is always able to support persistent storage for the cache
   * @return true
   */
  @Override
  public boolean isPersistent() {
    return true;
  }

  /**
   * Transfers data from file to the given byte buffer
   * @param be an {@link BucketEntry} which maintains an (offset, len, refCnt)
   * @return the {@link Cacheable} with block data inside.
   * @throws IOException if any IO error happen.
   */
  @Override
  public Cacheable read(BucketEntry be) throws IOException {
    long offset = be.offset();
    int length = be.getLength();
    Preconditions.checkArgument(length >= 0, "Length of read can not be less than 0.");
    ByteBuff dstBuff = be.allocator.allocate(length);
    if (length != 0) {
      try {
        accessFile(readAccessor, dstBuff, offset);
        // The buffer created out of the fileChannel is formed by copying the data from the file
        // Hence in this case there is no shared memory that we point to. Even if the BucketCache
        // evicts this buffer from the file the data is already copied and there is no need to
        // ensure that the results are not corrupted before consuming them.
        if (dstBuff.limit() != length) {
          throw new IllegalArgumentIOException(
              "Only " + dstBuff.limit() + " bytes read, " + length + " expected");
        }
      } catch (IOException ioe) {
        dstBuff.release();
        throw ioe;
      }
    }
    dstBuff.rewind();
    return be.wrapAsCacheable(dstBuff);
  }

  void closeFileChannels() {
    for (FileChannel fileChannel: fileChannels) {
      try {
        fileChannel.close();
      } catch (IOException e) {
        LOG.warn("Failed to close FileChannel", e);
      }
    }
  }

  /**
   * Transfers data from the given byte buffer to file
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the file where the first byte to be written
   * @throws IOException
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    write(ByteBuff.wrap(srcBuffer), offset);
  }

  /**
   * Sync the data to file after writing
   * @throws IOException
   */
  @Override
  public void sync() throws IOException {
    for (int i = 0; i < fileChannels.length; i++) {
      try {
        if (fileChannels[i] != null) {
          fileChannels[i].force(true);
        }
      } catch (IOException ie) {
        LOG.warn("Failed syncing data to " + this.filePaths[i]);
        throw ie;
      }
    }
  }

  /**
   * Close the file
   */
  @Override
  public void shutdown() {
    for (int i = 0; i < filePaths.length; i++) {
      try {
        if (fileChannels[i] != null) {
          fileChannels[i].close();
        }
        if (rafs[i] != null) {
          rafs[i].close();
        }
      } catch (IOException ex) {
        LOG.error("Failed closing " + filePaths[i] + " when shudown the IOEngine", ex);
      }
    }
  }

  @Override
  public void write(ByteBuff srcBuff, long offset) throws IOException {
    if (!srcBuff.hasRemaining()) {
      return;
    }
    accessFile(writeAccessor, srcBuff, offset);
  }

  private void accessFile(FileAccessor accessor, ByteBuff buff,
                          long globalOffset) throws IOException {
    int startFileNum = getFileNum(globalOffset);
    int remainingAccessDataLen = buff.remaining();
    int endFileNum = getFileNum(globalOffset + remainingAccessDataLen - 1);
    int accessFileNum = startFileNum;
    long accessOffset = getAbsoluteOffsetInFile(accessFileNum, globalOffset);
    int bufLimit = buff.limit();
    while (true) {
      FileChannel fileChannel = fileChannels[accessFileNum];
      int accessLen = 0;
      if (endFileNum > accessFileNum) {
        // short the limit;
        buff.limit((int) (buff.limit() - remainingAccessDataLen + sizePerFile - accessOffset));
      }
      try {
        accessLen = accessor.access(fileChannel, buff, accessOffset);
      } catch (ClosedByInterruptException e) {
        throw e;
      } catch (ClosedChannelException e) {
        refreshFileConnection(accessFileNum, e);
        continue;
      }
      // recover the limit
      buff.limit(bufLimit);
      if (accessLen < remainingAccessDataLen) {
        remainingAccessDataLen -= accessLen;
        accessFileNum++;
        accessOffset = 0;
      } else {
        break;
      }
      if (accessFileNum >= fileChannels.length) {
        throw new IOException("Required data len " + StringUtils.byteDesc(buff.remaining())
            + " exceed the engine's capacity " + StringUtils.byteDesc(capacity) + " where offset="
            + globalOffset);
      }
    }
  }

  /**
   * Get the absolute offset in given file with the relative global offset.
   * @param fileNum
   * @param globalOffset
   * @return the absolute offset
   */
  private long getAbsoluteOffsetInFile(int fileNum, long globalOffset) {
    return globalOffset - fileNum * sizePerFile;
  }

  private int getFileNum(long offset) {
    if (offset < 0) {
      throw new IllegalArgumentException("Unexpected offset " + offset);
    }
    int fileNum = (int) (offset / sizePerFile);
    if (fileNum >= fileChannels.length) {
      throw new RuntimeException("Not expected offset " + offset
          + " where capacity=" + capacity);
    }
    return fileNum;
  }

  FileChannel[] getFileChannels() {
    return fileChannels;
  }

  void refreshFileConnection(int accessFileNum, IOException ioe) throws IOException {
    ReentrantLock channelLock = channelLocks[accessFileNum];
    channelLock.lock();
    try {
      FileChannel fileChannel = fileChannels[accessFileNum];
      if (fileChannel != null) {
        // Don't re-open a channel if we were waiting on another
        // thread to re-open the channel and it is now open.
        if (fileChannel.isOpen()) {
          return;
        }
        fileChannel.close();
      }
      LOG.warn("Caught ClosedChannelException accessing BucketCache, reopening file: "
          + filePaths[accessFileNum], ioe);
      rafs[accessFileNum] = new RandomAccessFile(filePaths[accessFileNum], "rw");
      fileChannels[accessFileNum] = rafs[accessFileNum].getChannel();
    } finally{
      channelLock.unlock();
    }
  }

  private interface FileAccessor {
    int access(FileChannel fileChannel, ByteBuff buff, long accessOffset)
        throws IOException;
  }

  private static class FileReadAccessor implements FileAccessor {
    @Override
    public int access(FileChannel fileChannel, ByteBuff buff,
                      long accessOffset) throws IOException {
      return buff.read(fileChannel, accessOffset);
    }
  }

  private static class FileWriteAccessor implements FileAccessor {
    @Override
    public int access(FileChannel fileChannel, ByteBuff buff,
                      long accessOffset) throws IOException {
      return buff.write(fileChannel, accessOffset);
    }
  }
}
