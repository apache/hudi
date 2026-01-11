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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * File Format for Hoodie Log Files. The File Format consists of blocks each separated with a MAGIC sync marker. A Block
 * can either be a Data block, Command block or Delete Block. Data Block - Contains log records serialized as Avro
 * Binary Format Command Block - Specific commands like ROLLBACK_PREVIOUS-BLOCK - Tombstone for the previously written
 * block Delete Block - List of keys to delete - tombstone for keys
 */
public interface HoodieLogFormat {

  /**
   * Magic 6 bytes we put at the start of every block in the log file.
   */
  byte[] MAGIC = new byte[] {'#', 'H', 'U', 'D', 'I', '#'};

  /**
   * The current version of the log format. Anytime the log format changes this version needs to be bumped and
   * corresponding changes need to be made to {@link HoodieLogFormatVersion}
   */
  int CURRENT_VERSION = 1;

  String UNKNOWN_WRITE_TOKEN = "1-0-1";

  String DEFAULT_WRITE_TOKEN = "0-0-0";

  /**
   * Abstract base class for appending blocks to the Hoodie log format.
   * Subclasses provide specific implementations for writing to different storage layers.
   */
  @Getter
  @Slf4j
  abstract class Writer implements Closeable {

    // Default max log file size 512 MB
    public static final long DEFAULT_SIZE_THRESHOLD = 512 * 1024 * 1024L;

    // Buffer size
    protected Integer bufferSize;
    // FileSystem
    protected HoodieStorage storage;
    // Size threshold for the log file. Useful when used with a rolling log appender
    protected Long sizeThreshold;
    // Log File extension. Could be .avro.delta or .avro.commits etc
    protected String fileExtension;
    // File Id
    protected String logFileId;
    // File Commit Time stamp
    protected String instantTime;
    // version number for this log file. If not specified, then the current version will be
    // computed by inspecting the file system
    protected Integer logVersion;
    // file len of this log file
    protected Long fileLen = 0L;
    // Location of the directory containing the log
    protected StoragePath parentPath;
    // Log File Write Token
    protected String logWriteToken;
    // optional file suffix
    protected String suffix;
    // file creation hook
    protected LogFileCreationCallback fileCreationCallback;
    protected HoodieLogFile logFile;

    protected HoodieTableVersion tableVersion;

    /**
     * Base constructor that performs the core Hudi Log logic.
     */
    protected Writer(
        Integer bufferSize,
        HoodieStorage storage,
        StoragePath parentPath,
        String logFileId,
        String fileExtension,
        String instantTime,
        Integer logVersion,
        String logWriteToken,
        String suffix,
        Long fileLen,
        Long sizeThreshold,
        LogFileCreationCallback fileCreationCallback,
        HoodieTableVersion tableVersion) throws IOException {
      log.info("Building HoodieLogFormat.Writer");

      // Validation
      ValidationUtils.checkArgument(storage != null, "Storage is not specified");
      ValidationUtils.checkArgument(logFileId != null, "FileID is not specified");
      ValidationUtils.checkArgument(instantTime != null, "Instant time is not specified");
      ValidationUtils.checkArgument(fileExtension != null, "File extension is not specified");
      ValidationUtils.checkArgument(parentPath != null, "Log file parent location is not specified");

      this.bufferSize = bufferSize != null ? bufferSize : storage.getDefaultBufferSize();
      this.storage = storage;
      this.parentPath = parentPath;
      this.logFileId = logFileId;
      this.fileExtension = fileExtension;
      this.instantTime = instantTime;
      this.logVersion = logVersion;
      this.logWriteToken = logWriteToken;
      this.suffix = suffix;

      // Defaults and logic
      this.fileLen = fileLen != null ? fileLen : 0L;
      this.sizeThreshold = sizeThreshold != null ? sizeThreshold : DEFAULT_SIZE_THRESHOLD;
      // Does nothing by default
      this.fileCreationCallback = fileCreationCallback != null ? fileCreationCallback : new LogFileCreationCallback() {};
      this.tableVersion = tableVersion != null ? tableVersion : HoodieTableVersion.current();

      // Log version computation
      if (this.logVersion == null) {
        log.info("Computing next log version for {} in {}", logFileId, parentPath);
        boolean useBaseVersion = this.tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) && this.logWriteToken != null;

        if (useBaseVersion) {
          this.logVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
        } else {
          // Compute from storage (expensive)
          Option<Pair<Integer, String>> versionAndToken = FSUtils.getLatestLogVersion(this.storage, this.parentPath, this.logFileId, this.fileExtension, this.instantTime);
          if (versionAndToken.isPresent()) {
            this.logVersion = versionAndToken.get().getKey();
            this.logWriteToken = versionAndToken.get().getValue();
          } else {
            this.logVersion = HoodieLogFile.LOGFILE_BASE_VERSION;
            this.logWriteToken = UNKNOWN_WRITE_TOKEN;
          }
        }
      }

      if (this.logWriteToken == null) {
        this.logWriteToken = UNKNOWN_WRITE_TOKEN;
      }

      if (this.suffix != null) {
        // A little hacky to simplify the file name concatenation:
        // patch the write token with an optional suffix
        // instead of adding a new extension
        this.logWriteToken = this.logWriteToken + this.suffix;
      }

      // Initialise logFile
      StoragePath logPath = new StoragePath(parentPath,
          FSUtils.makeLogFileName(this.logFileId, this.fileExtension, this.instantTime, this.logVersion, this.logWriteToken));
      log.info("HoodieLogFile on path {}", logPath);
      this.logFile = new HoodieLogFile(logPath, this.fileLen);
    }

    /**
     * Append Block to a log file.
     * @return {@link AppendResult} containing result of the append.
     */
    public abstract AppendResult appendBlock(HoodieLogBlock block) throws IOException, InterruptedException;

    /**
     * Appends the list of blocks to a logfile.
     * @return {@link AppendResult} containing result of the append.
     */
    public abstract AppendResult appendBlocks(List<HoodieLogBlock> blocks) throws IOException, InterruptedException;

    public abstract long getCurrentSize() throws IOException;
  }

  /**
   * Reader interface which is an Iterator of HoodieLogBlock.
   */
  interface Reader extends Closeable, Iterator<HoodieLogBlock> {

    /**
     * @return the path to this {@link HoodieLogFormat}
     */
    HoodieLogFile getLogFile();

    /**
     * Read log file in reverse order and check if prev block is present.
     * 
     * @return {@code true} if previous block is present, {@code false} otherwise.
     */
    boolean hasPrev();

    /**
     * Read log file in reverse order and return prev block if present.
     * 
     * @return {@link HoodieLogBlock} the previous block
     * @throws IOException
     */
    HoodieLogBlock prev() throws IOException;
  }

  static HoodieLogFormat.Reader newReader(HoodieStorage storage, HoodieLogFile logFile, HoodieSchema readerSchema)
      throws IOException {
    return new HoodieLogFileReader(storage, logFile, readerSchema, HoodieLogFileReader.DEFAULT_BUFFER_SIZE);
  }

  static HoodieLogFormat.Reader newReader(HoodieStorage storage, HoodieLogFile logFile, HoodieSchema readerSchema, boolean reverseReader) throws IOException {
    return new HoodieLogFileReader(storage, logFile, readerSchema, HoodieLogFileReader.DEFAULT_BUFFER_SIZE, reverseReader);
  }

  /**
   * A set of feature flags associated with a log format. Versions are changed when the log format changes. TODO(na) -
   * Implement policies around major/minor versions
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
