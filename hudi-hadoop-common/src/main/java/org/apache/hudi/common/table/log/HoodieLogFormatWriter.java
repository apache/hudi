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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.HoodieLogFormat.WriterBuilder;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * HoodieLogFormatWriter can be used to append blocks to a log file Use HoodieLogFormat.WriterBuilder to construct.
 */
@Slf4j
public class HoodieLogFormatWriter implements HoodieLogFormat.Writer {

  @Getter
  private HoodieLogFile logFile;
  private FSDataOutputStream output;

  private final HoodieStorage storage;
  @Getter
  private final long sizeThreshold;
  private final Integer bufferSize;
  private final Short replication;
  private final String rolloverLogWriteToken;
  private final LogFileCreationCallback fileCreationHook;
  private boolean closed = false;
  private transient Thread shutdownThread = null;

  public HoodieLogFormatWriter(
      HoodieStorage storage,
      HoodieLogFile logFile,
      Integer bufferSize,
      Short replication,
      Long sizeThreshold,
      String rolloverLogWriteToken,
      LogFileCreationCallback fileCreationHook) {
    this.storage = storage;
    this.logFile = logFile;
    this.sizeThreshold = sizeThreshold;
    this.bufferSize = bufferSize != null ? bufferSize : storage.getDefaultBufferSize();
    this.replication = replication != null ? replication : storage.getDefaultReplication(logFile.getPath().getParent());
    this.rolloverLogWriteToken = rolloverLogWriteToken;
    this.fileCreationHook = fileCreationHook;
    addShutDownHook();
  }

  /**
   * Overrides the output stream, only for test purpose.
   */
  @VisibleForTesting
  public void withOutputStream(FSDataOutputStream output) {
    this.output = output;
  }

  /**
   * Lazily opens the output stream if needed for writing.
   * @return OutputStream for writing to current log file.
   * @throws IOException
   */
  private FSDataOutputStream getOutputStream() throws IOException {
    if (this.output == null) {
      boolean created = false;
      while (!created) {
        try {
          if (storage.exists(logFile.getPath())) {
            rollOver();
          }
          // Block size does not matter as we will always manually auto-flush
          createNewFile();
          log.info("Created a new log file: {}", logFile);
          created = true;
        } catch (FileAlreadyExistsException ignored) {
          log.info("File {} already exists, rolling over", logFile.getPath());
          rollOver();
        } catch (RemoteException re) {
          if (re.getClassName().contentEquals(AlreadyBeingCreatedException.class.getName())) {
            log.warn("Another task executor writing to the same log file({}), rolling over", logFile);
            // Rollover the current log file (since cannot get a stream handle) and create new one
            rollOver();
          } else {
            throw re;
          }
        }
      }
    }
    return output;
  }

  @Override
  public AppendResult appendBlock(HoodieLogBlock block) throws IOException, InterruptedException {
    return appendBlocks(Collections.singletonList(block));
  }

  @Override
  public AppendResult appendBlocks(List<HoodieLogBlock> blocks) throws IOException {
    // Find current version
    HoodieLogFormat.LogFormatVersion currentLogFormatVersion =
        new HoodieLogFormatVersion(HoodieLogFormat.CURRENT_VERSION);

    FSDataOutputStream originalOutputStream = getOutputStream();
    long startPos = originalOutputStream.getPos();
    long sizeWritten = 0;
    // HUDI-2655. here we wrap originalOutputStream to ensure huge blocks can be correctly written
    FSDataOutputStream outputStream = new FSDataOutputStream(originalOutputStream, new FileSystem.Statistics(storage.getScheme()), startPos);
    for (HoodieLogBlock block: blocks) {
      long startSize = outputStream.size();

      // 1. Write the magic header for the start of the block
      outputStream.write(HoodieLogFormat.MAGIC);

      // bytes for header
      byte[] headerBytes = HoodieLogBlock.getHeaderMetadataBytes(block.getLogBlockHeader());
      // content bytes
      ByteArrayOutputStream content = block.getContentBytes(storage);
      // bytes for footer
      byte[] footerBytes = HoodieLogBlock.getFooterMetadataBytes(block.getLogBlockFooter());

      // 2. Write the total size of the block (excluding Magic)
      outputStream.writeLong(getLogBlockLength(content.size(), headerBytes.length, footerBytes.length));

      // 3. Write the version of this log block
      outputStream.writeInt(currentLogFormatVersion.getVersion());
      // 4. Write the block type
      outputStream.writeInt(block.getBlockType().ordinal());

      // 5. Write the headers for the log block
      outputStream.write(headerBytes);
      // 6. Write the size of the content block
      outputStream.writeLong(content.size());
      // 7. Write the contents of the data block
      content.writeTo(outputStream);
      // 8. Write the footers for the log block
      outputStream.write(footerBytes);
      // 9. Write the total size of the log block (including magic) which is everything written
      // until now (for reverse pointer)
      // Update: this information is now used in determining if a block is corrupt by comparing to the
      //   block size in header. This change assumes that the block size will be the last data written
      //   to a block. Read will break if any data is written past this point for a block.
      outputStream.writeLong(outputStream.size() - startSize);

      // Fetch the size again, so it accounts also (9).

      // HUDI-2655. Check the size written to avoid log blocks whose size overflow.
      if (outputStream.size() == Integer.MAX_VALUE) {
        throw new HoodieIOException("Blocks appended may overflow. Please decrease log block size or log block amount");
      }
      sizeWritten +=  outputStream.size() - startSize;
    }
    // Flush all blocks to disk
    flush();

    AppendResult result = new AppendResult(logFile, startPos, sizeWritten);
    // roll over if size is past the threshold
    rolloverIfNeeded();
    return result;
  }

  /**
   * This method returns the total LogBlock Length which is the sum of 1. Number of bytes to write version 2. Number of
   * bytes to write ordinal 3. Length of the headers 4. Number of bytes used to write content length 5. Length of the
   * content 6. Length of the footers 7. Number of bytes to write totalLogBlockLength
   */
  private int getLogBlockLength(int contentLength, int headerLength, int footerLength) {
    return Integer.BYTES // Number of bytes to write version
        + Integer.BYTES // Number of bytes to write ordinal
        + headerLength // Length of the headers
        + Long.BYTES // Number of bytes used to write content length
        + contentLength // Length of the content
        + footerLength // Length of the footers
        + Long.BYTES; // bytes to write totalLogBlockLength at end of block (for reverse ptr)
  }

  private void rolloverIfNeeded() throws IOException {
    // Roll over if the size is past the threshold
    if (getCurrentSize() > sizeThreshold) {
      log.info("CurrentSize {} has reached threshold {}. Rolling over to the next version", getCurrentSize(), sizeThreshold);
      rollOver();
    }
  }

  private void rollOver() throws IOException {
    closeStream();
    this.logFile = logFile.rollOver(rolloverLogWriteToken);
    this.closed = false;
  }

  private void createNewFile() throws IOException {
    fileCreationHook.preFileCreation(this.logFile);
    this.output = new FSDataOutputStream(
        storage.create(this.logFile.getPath(), false, bufferSize, replication, WriterBuilder.DEFAULT_SIZE_THRESHOLD),
        new FileSystem.Statistics(storage.getScheme())
    );
  }

  @Override
  public void close() throws IOException {
    closeStream();
    // remove the shutdown hook after closing the stream to avoid memory leaks
    if (null != shutdownThread) {
      Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }
  }

  private void closeStream() throws IOException {
    if (output != null) {
      flush();
      output.close();
      output = null;
      closed = true;
    }
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
    if (closed) {
      throw new IllegalStateException("Cannot get current size as the underlying stream has been closed already");
    }

    if (output == null) {
      return 0;
    }
    return output.getPos();
  }

  /**
   * Close the output stream when the JVM exits.
   */
  private void addShutDownHook() {
    shutdownThread = new Thread() {
      public void run() {
        try {
          log.info("running HoodieLogFormatWriter shutdown hook to close output stream for log file: {}", logFile);
          closeStream();
        } catch (Exception e) {
          log.warn("unable to close output stream for log file: {}", logFile, e);
          // fail silently for any sort of exception
        }
      }
    };
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }
}
