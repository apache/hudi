/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.storage.StorageSchemes;
import org.apache.hudi.common.table.log.HoodieLogFormat.Writer;
import org.apache.hudi.common.table.log.HoodieLogFormat.WriterBuilder;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HoodieLogFormatWriter can be used to append blocks to a log file Use HoodieLogFormat.WriterBuilder to construct.
 */
public class HoodieLogFormatWriter implements HoodieLogFormat.Writer {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieLogFormatWriter.class);

  private HoodieLogFile logFile;
  private final FileSystem fs;
  private final long sizeThreshold;
  private final Integer bufferSize;
  private final Short replication;
  private final String logWriteToken;
  private final String rolloverLogWriteToken;
  private FSDataOutputStream output;
  private static final String APPEND_UNAVAILABLE_EXCEPTION_MESSAGE = "not sufficiently replicated yet";

  /**
   * @param fs
   * @param logFile
   * @param bufferSize
   * @param replication
   * @param sizeThreshold
   */
  HoodieLogFormatWriter(FileSystem fs, HoodieLogFile logFile, Integer bufferSize, Short replication, Long sizeThreshold,
      String logWriteToken, String rolloverLogWriteToken) throws IOException, InterruptedException {
    this.fs = fs;
    this.logFile = logFile;
    this.sizeThreshold = sizeThreshold;
    this.bufferSize = bufferSize;
    this.replication = replication;
    this.logWriteToken = logWriteToken;
    this.rolloverLogWriteToken = rolloverLogWriteToken;
    Path path = logFile.getPath();
    if (fs.exists(path)) {
      boolean isAppendSupported = StorageSchemes.isAppendSupported(fs.getScheme());
      if (isAppendSupported) {
        LOG.info("{} exists. Appending to existing file", logFile);
        try {
          this.output = fs.append(path, bufferSize);
        } catch (RemoteException e) {
          LOG.warn("Remote Exception, attempting to handle or recover lease", e);
          handleAppendExceptionOrRecoverLease(path, e);
        } catch (IOException ioe) {
          if (ioe.getMessage().toLowerCase().contains("not supported")) {
            // may still happen if scheme is viewfs.
            isAppendSupported = false;
          } else {
            throw ioe;
          }
        }
      }
      if (!isAppendSupported) {
        this.logFile = logFile.rollOver(fs, rolloverLogWriteToken);
        LOG.info("Append not supported.. Rolling over to {}", logFile);
        createNewFile();
      }
    } else {
      LOG.info("{} does not exist. Create a new file", logFile);
      // Block size does not matter as we will always manually autoflush
      createNewFile();
    }
  }

  public FileSystem getFs() {
    return fs;
  }

  @Override
  public HoodieLogFile getLogFile() {
    return logFile;
  }

  public long getSizeThreshold() {
    return sizeThreshold;
  }

  @Override
  public Writer appendBlock(HoodieLogBlock block) throws IOException, InterruptedException {

    // Find current version
    HoodieLogFormat.LogFormatVersion currentLogFormatVersion =
        new HoodieLogFormatVersion(HoodieLogFormat.CURRENT_VERSION);
    long currentSize = this.output.size();

    // 1. Write the magic header for the start of the block
    this.output.write(HoodieLogFormat.MAGIC);

    // bytes for header
    byte[] headerBytes = HoodieLogBlock.getLogMetadataBytes(block.getLogBlockHeader());
    // content bytes
    byte[] content = block.getContentBytes();
    // bytes for footer
    byte[] footerBytes = HoodieLogBlock.getLogMetadataBytes(block.getLogBlockFooter());

    // 2. Write the total size of the block (excluding Magic)
    this.output.writeLong(getLogBlockLength(content.length, headerBytes.length, footerBytes.length));

    // 3. Write the version of this log block
    this.output.writeInt(currentLogFormatVersion.getVersion());
    // 4. Write the block type
    this.output.writeInt(block.getBlockType().ordinal());

    // 5. Write the headers for the log block
    this.output.write(headerBytes);
    // 6. Write the size of the content block
    this.output.writeLong(content.length);
    // 7. Write the contents of the data block
    this.output.write(content);
    // 8. Write the footers for the log block
    this.output.write(footerBytes);
    // 9. Write the total size of the log block (including magic) which is everything written
    // until now (for reverse pointer)
    this.output.writeLong(this.output.size() - currentSize);
    // Flush every block to disk
    flush();

    // roll over if size is past the threshold
    return rolloverIfNeeded();
  }

  /**
   * This method returns the total LogBlock Length which is the sum of 1. Number of bytes to write version 2. Number of
   * bytes to write ordinal 3. Length of the headers 4. Number of bytes used to write content length 5. Length of the
   * content 6. Length of the footers 7. Number of bytes to write totalLogBlockLength
   */
  private int getLogBlockLength(int contentLength, int headerLength, int footerLength) {
    return Integer.BYTES + // Number of bytes to write version
        Integer.BYTES + // Number of bytes to write ordinal
        headerLength + // Length of the headers
        Long.BYTES + // Number of bytes used to write content length
        contentLength + // Length of the content
        footerLength + // Length of the footers
        Long.BYTES; // bytes to write totalLogBlockLength at end of block (for reverse ptr)
  }

  private Writer rolloverIfNeeded() throws IOException, InterruptedException {
    // Roll over if the size is past the threshold
    if (getCurrentSize() > sizeThreshold) {
      // TODO - make an end marker which seals the old log file (no more appends possible to that
      // file).
      LOG.info("CurrentSize {} has reached threshold {}. Rolling over to the next version", getCurrentSize(), sizeThreshold);
      HoodieLogFile newLogFile = logFile.rollOver(fs, rolloverLogWriteToken);
      // close this writer and return the new writer
      close();
      return new HoodieLogFormatWriter(fs, newLogFile, bufferSize, replication, sizeThreshold, logWriteToken,
          rolloverLogWriteToken);
    }
    return this;
  }

  private void createNewFile() throws IOException {
    this.output =
        fs.create(this.logFile.getPath(), false, bufferSize, replication, WriterBuilder.DEFAULT_SIZE_THRESHOLD, null);
  }

  @Override
  public void close() throws IOException {
    flush();
    output.close();
    output = null;
  }

  private void flush() throws IOException {
    if (output == null) {
      return; // Presume closed
    }
    output.flush();
    // NOTE : the following API call makes sure that the data is flushed to disk on DataNodes (akin to POSIX fsync())
    // See more details here : https://issues.apache.org/jira/browse/HDFS-744
    output.hsync();
  }

  @Override
  public long getCurrentSize() throws IOException {
    if (output == null) {
      throw new IllegalStateException("Cannot get current size as the underlying stream has been closed already");
    }
    return output.getPos();
  }

  private void handleAppendExceptionOrRecoverLease(Path path, RemoteException e)
      throws IOException, InterruptedException {
    if (e.getMessage().contains(APPEND_UNAVAILABLE_EXCEPTION_MESSAGE)) {
      // This issue happens when all replicas for a file are down and/or being decommissioned.
      // The fs.append() API could append to the last block for a file. If the last block is full, a new block is
      // appended to. In a scenario when a lot of DN's are decommissioned, it can happen that DN's holding all
      // replicas for a block/file are decommissioned together. During this process, all these blocks will start to
      // get replicated to other active DataNodes but this process might take time (can be of the order of few
      // hours). During this time, if a fs.append() API is invoked for a file whose last block is eligible to be
      // appended to, then the NN will throw an exception saying that it couldn't find any active replica with the
      // last block. Find more information here : https://issues.apache.org/jira/browse/HDFS-6325
      LOG.warn("Failed to open an append stream to the log file. Opening a new log file..", e);
      // Rollover the current log file (since cannot get a stream handle) and create new one
      this.logFile = logFile.rollOver(fs, rolloverLogWriteToken);
      createNewFile();
    } else if (e.getClassName().contentEquals(AlreadyBeingCreatedException.class.getName())) {
      LOG.warn("Another task executor writing to the same log file({}. Rolling over", logFile);
      // Rollover the current log file (since cannot get a stream handle) and create new one
      this.logFile = logFile.rollOver(fs, rolloverLogWriteToken);
      createNewFile();
    } else if (e.getClassName().contentEquals(RecoveryInProgressException.class.getName())
        && (fs instanceof DistributedFileSystem)) {
      // this happens when either another task executor writing to this file died or
      // data node is going down. Note that we can only try to recover lease for a DistributedFileSystem.
      // ViewFileSystem unfortunately does not support this operation
      LOG.warn("Trying to recover log on path {}", path);
      if (FSUtils.recoverDFSFileLease((DistributedFileSystem) fs, path)) {
        LOG.warn("Recovered lease on path {}", path);
        // try again
        this.output = fs.append(path, bufferSize);
      } else {
        LOG.warn("Failed to recover lease on path {}", path);
        throw new HoodieException(e);
      }
    } else {
      throw new HoodieIOException("Failed to open an append stream ", e);
    }
  }

}
