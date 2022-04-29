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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.StorageSchemes;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.HoodieLogFormat.WriterBuilder;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.RecoveryInProgressException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * HoodieLogFormatWriter can be used to append blocks to a log file Use HoodieLogFormat.WriterBuilder to construct.
 */
public class HoodieLogFormatWriter implements HoodieLogFormat.Writer {

  private static final Logger LOG = LogManager.getLogger(HoodieLogFormatWriter.class);

  private HoodieLogFile logFile;
  private FSDataOutputStream output;

  private final FileSystem fs;
  private final long sizeThreshold;
  private final Integer bufferSize;
  private final Short replication;
  private final String rolloverLogWriteToken;
  private boolean closed = false;
  private transient Thread shutdownThread = null;

  private static final String APPEND_UNAVAILABLE_EXCEPTION_MESSAGE = "not sufficiently replicated yet";

  HoodieLogFormatWriter(FileSystem fs, HoodieLogFile logFile, Integer bufferSize, Short replication, Long sizeThreshold, String rolloverLogWriteToken) {
    this.fs = fs;
    this.logFile = logFile;
    this.sizeThreshold = sizeThreshold;
    this.bufferSize = bufferSize;
    this.replication = replication;
    this.rolloverLogWriteToken = rolloverLogWriteToken;
    addShutDownHook();
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

  /**
   * Lazily opens the output stream if needed for writing.
   * @return OutputStream for writing to current log file.
   * @throws IOException
   * @throws InterruptedException
   */
  private FSDataOutputStream getOutputStream() throws IOException, InterruptedException {
    if (this.output == null) {
      Path path = logFile.getPath();
      if (fs.exists(path)) {
        boolean isAppendSupported = StorageSchemes.isAppendSupported(fs.getScheme());
        if (isAppendSupported) {
          LOG.info(logFile + " exists. Appending to existing file");
          try {
            // open the path for append and record the offset
            this.output = fs.append(path, bufferSize);
          } catch (RemoteException e) {
            LOG.warn("Remote Exception, attempting to handle or recover lease", e);
            handleAppendExceptionOrRecoverLease(path, e);
          } catch (IOException ioe) {
            if (ioe.getMessage().toLowerCase().contains("not supported")) {
              // may still happen if scheme is viewfs.
              isAppendSupported = false;
            } else {
              /*
               * Before throwing an exception, close the outputstream,
               * to ensure that the lease on the log file is released.
               */
              close();
              throw ioe;
            }
          }
        }
        if (!isAppendSupported) {
          rollOver();
          createNewFile();
          LOG.info("Append not supported.. Rolling over to " + logFile);
        }
      } else {
        LOG.info(logFile + " does not exist. Create a new file");
        // Block size does not matter as we will always manually autoflush
        createNewFile();
      }
    }
    return output;
  }

  @Override
  public AppendResult appendBlock(HoodieLogBlock block) throws IOException, InterruptedException {
    return appendBlocks(Collections.singletonList(block));
  }

  @Override
  public AppendResult appendBlocks(List<HoodieLogBlock> blocks) throws IOException, InterruptedException {
    // Find current version
    HoodieLogFormat.LogFormatVersion currentLogFormatVersion =
        new HoodieLogFormatVersion(HoodieLogFormat.CURRENT_VERSION);

    FSDataOutputStream originalOutputStream = getOutputStream();
    long startPos = originalOutputStream.getPos();
    long sizeWritten = 0;
    // HUDI-2655. here we wrap originalOutputStream to ensure huge blocks can be correctly written
    FSDataOutputStream outputStream = new FSDataOutputStream(originalOutputStream, new FileSystem.Statistics(fs.getScheme()), startPos);
    for (HoodieLogBlock block: blocks) {
      long startSize = outputStream.size();

      // 1. Write the magic header for the start of the block
      outputStream.write(HoodieLogFormat.MAGIC);

      // bytes for header
      byte[] headerBytes = HoodieLogBlock.getLogMetadataBytes(block.getLogBlockHeader());
      // content bytes
      byte[] content = block.getContentBytes();
      // bytes for footer
      byte[] footerBytes = HoodieLogBlock.getLogMetadataBytes(block.getLogBlockFooter());

      // 2. Write the total size of the block (excluding Magic)
      outputStream.writeLong(getLogBlockLength(content.length, headerBytes.length, footerBytes.length));

      // 3. Write the version of this log block
      outputStream.writeInt(currentLogFormatVersion.getVersion());
      // 4. Write the block type
      outputStream.writeInt(block.getBlockType().ordinal());

      // 5. Write the headers for the log block
      outputStream.write(headerBytes);
      // 6. Write the size of the content block
      outputStream.writeLong(content.length);
      // 7. Write the contents of the data block
      outputStream.write(content);
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
    return Integer.BYTES + // Number of bytes to write version
        Integer.BYTES + // Number of bytes to write ordinal
        headerLength + // Length of the headers
        Long.BYTES + // Number of bytes used to write content length
        contentLength + // Length of the content
        footerLength + // Length of the footers
        Long.BYTES; // bytes to write totalLogBlockLength at end of block (for reverse ptr)
  }

  private void rolloverIfNeeded() throws IOException {
    // Roll over if the size is past the threshold
    if (getCurrentSize() > sizeThreshold) {
      LOG.info("CurrentSize " + getCurrentSize() + " has reached threshold " + sizeThreshold
          + ". Rolling over to the next version");
      rollOver();
    }
  }

  private void rollOver() throws IOException {
    closeStream();
    this.logFile = logFile.rollOver(fs, rolloverLogWriteToken);
    this.closed = false;
  }

  private void createNewFile() throws IOException {
    this.output =
        fs.create(this.logFile.getPath(), false, bufferSize, replication, WriterBuilder.DEFAULT_SIZE_THRESHOLD, null);
  }

  @Override
  public void close() throws IOException {
    if (null != shutdownThread) {
      Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }
    closeStream();
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
          if (output != null) {
            close();
          }
        } catch (Exception e) {
          LOG.warn("unable to close output stream for log file " + logFile, e);
          // fail silently for any sort of exception
        }
      }
    };
    Runtime.getRuntime().addShutdownHook(shutdownThread);
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
      rollOver();
      createNewFile();
    } else if (e.getClassName().contentEquals(AlreadyBeingCreatedException.class.getName())) {
      LOG.warn("Another task executor writing to the same log file(" + logFile + ". Rolling over");
      // Rollover the current log file (since cannot get a stream handle) and create new one
      rollOver();
      createNewFile();
    } else if (e.getClassName().contentEquals(RecoveryInProgressException.class.getName())
        && (fs instanceof DistributedFileSystem)) {
      // this happens when either another task executor writing to this file died or
      // data node is going down. Note that we can only try to recover lease for a DistributedFileSystem.
      // ViewFileSystem unfortunately does not support this operation
      LOG.warn("Trying to recover log on path " + path);
      if (FSUtils.recoverDFSFileLease((DistributedFileSystem) fs, path)) {
        LOG.warn("Recovered lease on path " + path);
        // try again
        this.output = fs.append(path, bufferSize);
      } else {
        final String msg = "Failed to recover lease on path " + path;
        LOG.warn(msg);
        throw new HoodieException(msg, e);
      }
    } else {
      // When fs.append() has failed and an exception is thrown, by closing the output stream
      // we shall force hdfs to release the lease on the log file. When Spark retries this task (with
      // new attemptId, say taskId.1) it will be able to acquire lease on the log file (as output stream was
      // closed properly by taskId.0).
      //
      // If closeStream() call were to fail throwing an exception, our best bet is to rollover to a new log file.
      try {
        closeStream();
        // output stream has been successfully closed and lease on the log file has been released,
        // before throwing an exception for the append failure.
        throw new HoodieIOException("Failed to append to the output stream ", e);
      } catch (Exception ce) {
        LOG.warn("Failed to close the output stream for " + fs.getClass().getName() + " on path " + path
            + ". Rolling over to a new log file.");
        rollOver();
        createNewFile();
      }
    }
  }
}
