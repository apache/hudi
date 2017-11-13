/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common.table.log;

import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Writer;
import com.uber.hoodie.common.table.log.HoodieLogFormat.WriterBuilder;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.exception.HoodieException;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * HoodieLogFormatWriter can be used to append blocks to a log file Use
 * HoodieLogFormat.WriterBuilder to construct
 */
public class HoodieLogFormatWriter implements HoodieLogFormat.Writer {

  private final static Logger log = LogManager.getLogger(HoodieLogFormatWriter.class);

  private final HoodieLogFile logFile;
  private final FileSystem fs;
  private final long sizeThreshold;
  private final Integer bufferSize;
  private final Short replication;
  private FSDataOutputStream output;

  /**
   *
   * @param fs
   * @param logFile
   * @param bufferSize
   * @param replication
   * @param sizeThreshold
   */
  HoodieLogFormatWriter(FileSystem fs, HoodieLogFile logFile, Integer bufferSize,
      Short replication, Long sizeThreshold)
      throws IOException, InterruptedException {
    this.fs = fs;
    this.logFile = logFile;
    this.sizeThreshold = sizeThreshold;
    this.bufferSize = bufferSize;
    this.replication = replication;

    Path path = logFile.getPath();
    if (fs.exists(path)) {
      log.info(logFile + " exists. Appending to existing file");
      try {
        this.output = fs.append(path, bufferSize);
      } catch (RemoteException e) {
        // this happens when either another task executor writing to this file died or data node is going down
        if (e.getClassName().equals(AlreadyBeingCreatedException.class.getName())
            && fs instanceof DistributedFileSystem) {
          log.warn("Trying to recover log on path " + path);
          if (FSUtils.recoverDFSFileLease((DistributedFileSystem) fs, path)) {
            log.warn("Recovered lease on path " + path);
            // try again
            this.output = fs.append(path, bufferSize);
          } else {
            log.warn("Failed to recover lease on path " + path);
            throw new HoodieException(e);
          }
        }
      }
    } else {
      log.info(logFile + " does not exist. Create a new file");
      // Block size does not matter as we will always manually autoflush
      this.output = fs.create(path, false, bufferSize, replication,
          WriterBuilder.DEFAULT_SIZE_THRESHOLD, null);
      // TODO - append a file level meta block
    }
  }

  public FileSystem getFs() {
    return fs;
  }

  public HoodieLogFile getLogFile() {
    return logFile;
  }

  public long getSizeThreshold() {
    return sizeThreshold;
  }

  @Override
  public Writer appendBlock(HoodieLogBlock block)
      throws IOException, InterruptedException {
    byte[] content = block.getBytes();
    // 1. write the magic header for the start of the block
    this.output.write(HoodieLogFormat.MAGIC);
    // 2. Write the block type
    this.output.writeInt(block.getBlockType().ordinal());
    // 3. Write the size of the block
    this.output.writeInt(content.length);
    // 4. Write the contents of the block
    this.output.write(content);

    // Flush every block to disk
    flush();

    // roll over if size is past the threshold
    return rolloverIfNeeded();
  }

  private Writer rolloverIfNeeded() throws IOException, InterruptedException {
    // Roll over if the size is past the threshold
    if (getCurrentSize() > sizeThreshold) {
      //TODO - make an end marker which seals the old log file (no more appends possible to that file).
      log.info("CurrentSize " + getCurrentSize() + " has reached threshold " + sizeThreshold
          + ". Rolling over to the next version");
      HoodieLogFile newLogFile = logFile.rollOver(fs);
      // close this writer and return the new writer
      close();
      return new HoodieLogFormatWriter(fs, newLogFile, bufferSize, replication, sizeThreshold);
    }
    return this;
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
    output.hflush();
  }

  public long getCurrentSize() throws IOException {
    if (output == null) {
      throw new IllegalStateException(
          "Cannot get current size as the underlying stream has been closed already");
    }
    return output.getPos();
  }

}
