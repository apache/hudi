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
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.FSUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * File Format for Hoodie Log Files. The File Format consists of blocks each seperated with a
 * OLD_MAGIC sync marker. A Block can either be a Data block, Command block or Delete Block. Data
 * Block - Contains log records serialized as Avro Binary Format Command Block - Specific commands
 * like RoLLBACK_PREVIOUS-BLOCK - Tombstone for the previously written block Delete Block - List of
 * keys to delete - tombstone for keys
 */
public interface HoodieLogFormat {

  /**
   * Magic 4 bytes we put at the start of every block in the log file. Sync marker. We could make
   * this file specific (generate a random 4 byte magic and stick it in the file header), but this I
   * think is suffice for now - PR
   */
  byte[] OLD_MAGIC = new byte[] {'H', 'U', 'D', 'I'};

  /**
   * Magic 6 bytes we put at the start of every block in the log file. This is added to maintain
   * backwards compatiblity due to lack of log format/block version in older log files. All new log
   * block will now write this OLD_MAGIC value
   */
  byte[] MAGIC = new byte[] {'#', 'H', 'U', 'D', 'I', '#'};

  /**
   * The current version of the log format. Anytime the log format changes this version needs to be
   * bumped and corresponding changes need to be made to {@link HoodieLogFormatVersion}
   */
  int currentVersion = 1;

  /**
   * Writer interface to allow appending block to this file format
   */
  interface Writer extends Closeable {

    /**
     * @return the path to this {@link HoodieLogFormat}
     */
    HoodieLogFile getLogFile();

    /**
     * Append Block returns a new Writer if the log is rolled
     */
    Writer appendBlock(HoodieLogBlock block) throws IOException, InterruptedException;

    long getCurrentSize() throws IOException;
  }

  /**
   * Reader interface which is an Iterator of HoodieLogBlock
   */
  interface Reader extends Closeable, Iterator<HoodieLogBlock> {

    /**
     * @return the path to this {@link HoodieLogFormat}
     */
    HoodieLogFile getLogFile();
  }


  /**
   * Builder class to construct the default log format writer
   */
  class WriterBuilder {

    private static final Logger log = LogManager.getLogger(WriterBuilder.class);
    // Default max log file size 512 MB
    public static final long DEFAULT_SIZE_THRESHOLD = 512 * 1024 * 1024L;

    // Buffer size
    private Integer bufferSize;
    // Replication for the log file
    private Short replication;
    // FileSystem
    private FileSystem fs;
    // Size threshold for the log file. Useful when used with a rolling log appender
    private Long sizeThreshold;
    // Log File extension. Could be .avro.delta or .avro.commits etc
    private String fileExtension;
    // File Id
    private String logFileId;
    // File Commit Time stamp
    private String commitTime;
    // version number for this log file. If not specified, then the current version will be
    // computed by inspecting the file system
    private Integer logVersion;
    // Location of the directory containing the log
    private Path parentPath;

    public WriterBuilder withBufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public WriterBuilder withReplication(short replication) {
      this.replication = replication;
      return this;
    }

    public WriterBuilder withFs(FileSystem fs) {
      this.fs = fs;
      return this;
    }

    public WriterBuilder withSizeThreshold(long sizeThreshold) {
      this.sizeThreshold = sizeThreshold;
      return this;
    }

    public WriterBuilder withFileExtension(String logFileExtension) {
      this.fileExtension = logFileExtension;
      return this;
    }

    public WriterBuilder withFileId(String fileId) {
      this.logFileId = fileId;
      return this;
    }

    public WriterBuilder overBaseCommit(String baseCommit) {
      this.commitTime = baseCommit;
      return this;
    }

    public WriterBuilder withLogVersion(int version) {
      this.logVersion = version;
      return this;
    }

    public WriterBuilder onParentPath(Path parentPath) {
      this.parentPath = parentPath;
      return this;
    }

    public Writer build() throws IOException, InterruptedException {
      log.info("Building HoodieLogFormat Writer");
      if (fs == null) {
        throw new IllegalArgumentException("fs is not specified");
      }
      if (logFileId == null) {
        throw new IllegalArgumentException("FileID is not specified");
      }
      if (commitTime == null) {
        throw new IllegalArgumentException("BaseCommitTime is not specified");
      }
      if (fileExtension == null) {
        throw new IllegalArgumentException("File extension is not specified");
      }
      if (parentPath == null) {
        throw new IllegalArgumentException("Log file parent location is not specified");
      }
      if (logVersion == null) {
        log.info("Computing the next log version for " + logFileId + " in " + parentPath);
        logVersion =
            FSUtils.getCurrentLogVersion(fs, parentPath, logFileId, fileExtension, commitTime);
        log.info(
            "Computed the next log version for " + logFileId + " in " + parentPath + " as "
                + logVersion);
      }

      Path logPath = new Path(parentPath,
          FSUtils.makeLogFileName(logFileId, fileExtension, commitTime, logVersion));
      log.info("HoodieLogFile on path " + logPath);
      HoodieLogFile logFile = new HoodieLogFile(logPath);

      if (bufferSize == null) {
        bufferSize = FSUtils.getDefaultBufferSize(fs);
      }
      if (replication == null) {
        replication = FSUtils.getDefaultReplication(fs, parentPath);
      }
      if (sizeThreshold == null) {
        sizeThreshold = DEFAULT_SIZE_THRESHOLD;
      }
      return new HoodieLogFormatWriter(fs, logFile, bufferSize, replication, sizeThreshold);
    }

  }

  static WriterBuilder newWriterBuilder() {
    return new WriterBuilder();
  }

  static HoodieLogFormat.Reader newReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema)
      throws IOException {
    return new HoodieLogFileReader(fs, logFile, readerSchema, HoodieLogFileReader.DEFAULT_BUFFER_SIZE, false, false);
  }

  /**
   * A set of feature flags associated with a log format. Versions are changed when the log format
   * changes. TODO(na) - Implement policies around major/minor versions
   */
  abstract class LogFormatVersion {

    private final int version;

    LogFormatVersion(int version) {
      this.version = version;
    }

    public int getVersion() {
      return version;
    }

    public abstract boolean hasMagicHeader();

    public abstract boolean hasContent();

    public abstract boolean hasContentLength();

    public abstract boolean hasOrdinal();

    public abstract boolean hasHeader();

    public abstract boolean hasFooter();

    public abstract boolean hasLogBlockLength();
  }
}
