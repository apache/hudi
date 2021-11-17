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

import org.apache.hudi.common.fs.BoundedFsDataInputStream;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.SchemeAwareFSDataInputStream;
import org.apache.hudi.common.fs.TimedFSDataInputStream;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCDCDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieCorruptBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieHFileDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.table.log.block.HoodieParquetDataBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.CorruptedLogFileException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Scans a log file and provides block level iterator on the log file Loads the entire block contents in memory Can emit
 * either a DataBlock, CommandBlock, DeleteBlock or CorruptBlock (if one is found).
 */
public class HoodieLogFileReader implements HoodieLogFormat.Reader {

  public static final int DEFAULT_BUFFER_SIZE = 16 * 1024 * 1024; // 16 MB
  private static final int BLOCK_SCAN_READ_BUFFER_SIZE = 1024 * 1024; // 1 MB
  private static final Logger LOG = LogManager.getLogger(HoodieLogFileReader.class);

  private final Configuration hadoopConf;
  private final FSDataInputStream inputStream;
  private final HoodieLogFile logFile;
  private final byte[] magicBuffer = new byte[6];
  private final Schema readerSchema;
  private InternalSchema internalSchema;
  private final String keyField;
  private boolean readBlockLazily;
  private long reverseLogFilePosition;
  private long lastReverseLogFilePosition;
  private boolean reverseReader;
  private boolean enableRecordLookups;
  private boolean closed = false;
  private transient Thread shutdownThread = null;

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily) throws IOException {
    this(fs, logFile, readerSchema, bufferSize, readBlockLazily, false);
  }

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily, boolean reverseReader) throws IOException {
    this(fs, logFile, readerSchema, bufferSize, readBlockLazily, reverseReader, false,
        HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily, boolean reverseReader, boolean enableRecordLookups,
                             String keyField) throws IOException {
    this(fs, logFile, readerSchema, bufferSize, readBlockLazily, reverseReader, enableRecordLookups, keyField, InternalSchema.getEmptyInternalSchema());
  }

  public HoodieLogFileReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                             boolean readBlockLazily, boolean reverseReader, boolean enableRecordLookups,
                             String keyField, InternalSchema internalSchema) throws IOException {
    this.hadoopConf = fs.getConf();
    // NOTE: We repackage {@code HoodieLogFile} here to make sure that the provided path
    //       is prefixed with an appropriate scheme given that we're not propagating the FS
    //       further
    this.logFile = new HoodieLogFile(FSUtils.makeQualified(fs, logFile.getPath()), logFile.getFileSize());
    this.inputStream = getFSDataInputStream(fs, this.logFile, bufferSize);
    this.readerSchema = readerSchema;
    this.readBlockLazily = readBlockLazily;
    this.reverseReader = reverseReader;
    this.enableRecordLookups = enableRecordLookups;
    this.keyField = keyField;
    this.internalSchema = internalSchema == null ? InternalSchema.getEmptyInternalSchema() : internalSchema;
    if (this.reverseReader) {
      this.reverseLogFilePosition = this.lastReverseLogFilePosition = this.logFile.getFileSize();
    }

    addShutDownHook();
  }

  @Override
  public HoodieLogFile getLogFile() {
    return logFile;
  }

  /**
   * Close the inputstream if not closed when the JVM exits.
   */
  private void addShutDownHook() {
    shutdownThread = new Thread(() -> {
      try {
        close();
      } catch (Exception e) {
        LOG.warn("unable to close input stream for log file " + logFile, e);
        // fail silently for any sort of exception
      }
    });
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  // TODO : convert content and block length to long by using ByteBuffer, raw byte [] allows
  // for max of Integer size
  private HoodieLogBlock readBlock() throws IOException {
    int blockSize;
    long blockStartPos = inputStream.getPos();
    try {
      // 1 Read the total size of the block
      blockSize = (int) inputStream.readLong();
    } catch (EOFException | CorruptedLogFileException e) {
      // An exception reading any of the above indicates a corrupt block
      // Create a corrupt block by finding the next MAGIC marker or EOF
      return createCorruptBlock();
    }

    // We may have had a crash which could have written this block partially
    // Skip blockSize in the stream and we should either find a sync marker (start of the next
    // block) or EOF. If we did not find either of it, then this block is a corrupted block.
    boolean isCorrupted = isBlockCorrupt(blockStartPos, blocksize);
    if (isCorrupted) {
      inputStream.seek(blockStartPos);
      return createCorruptBlock();
    }

    // 2. Read the version for this log format
    HoodieLogFormat.LogFormatVersion nextBlockVersion = readVersion();

    // 3. Read the block type for a log block
    HoodieLogBlockType blockType = tryReadBlockType(nextBlockVersion);

    // 4. Read the header for a log block, if present

    Map<HeaderMetadataType, String> header =
        nextBlockVersion.hasHeader() ? HoodieLogBlock.getLogMetadata(inputStream) : null;

    // 5. Read the content length for the content
    // Fallback to full-block size if no content-length
    // TODO replace w/ hasContentLength
    int contentLength =
        nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION ? (int) inputStream.readLong() : blockSize;

    // 6. Read the content or skip content based on IO vs Memory trade-off by client
    long contentPosition = inputStream.getPos();
    boolean shouldReadLazily = readBlockLazily && nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION;
    Option<byte[]> content = HoodieLogBlock.tryReadContent(inputStream, contentLength, shouldReadLazily);

    // 7. Read footer if any
    Map<HeaderMetadataType, String> footer =
        nextBlockVersion.hasFooter() ? HoodieLogBlock.getLogMetadata(inputStream) : null;

    // 8. Read log block length, if present. This acts as a reverse pointer when traversing a
    // log file in reverse
    if (nextBlockVersion.hasLogBlockLength()) {
      inputStream.readLong();
    }

    // 9. Read the log block end position in the log file
    long blockEndPos = inputStream.getPos();

    HoodieLogBlock.HoodieLogBlockContentLocation logBlockContentLoc =
        new HoodieLogBlock.HoodieLogBlockContentLocation(hadoopConf, logFile, contentPosition, contentLength, blockEndPos);

    switch (Objects.requireNonNull(blockType)) {
      case AVRO_DATA_BLOCK:
        if (nextBlockVersion.getVersion() == HoodieLogFormatVersion.DEFAULT_VERSION) {
          return HoodieAvroDataBlock.getBlock(content.get(), readerSchema, internalSchema);
        } else {
          return new HoodieAvroDataBlock(inputStream, content, readBlockLazily, logBlockContentLoc,
              Option.ofNullable(readerSchema), header, footer, keyField, internalSchema);
        }

      case HFILE_DATA_BLOCK:
        checkState(nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION,
            String.format("HFile block could not be of version (%d)", HoodieLogFormatVersion.DEFAULT_VERSION));

        return new HoodieHFileDataBlock(inputStream, content, readBlockLazily, logBlockContentLoc,
            Option.ofNullable(readerSchema), header, footer, enableRecordLookups, logFile.getPath());

      case PARQUET_DATA_BLOCK:
        checkState(nextBlockVersion.getVersion() != HoodieLogFormatVersion.DEFAULT_VERSION,
            String.format("Parquet block could not be of version (%d)", HoodieLogFormatVersion.DEFAULT_VERSION));

        return new HoodieParquetDataBlock(inputStream, content, readBlockLazily, logBlockContentLoc,
             Option.ofNullable(readerSchema), header, footer, keyField);

      case DELETE_BLOCK:
        return new HoodieDeleteBlock(content, inputStream, readBlockLazily, Option.of(logBlockContentLoc), header, footer);

      case COMMAND_BLOCK:
        return new HoodieCommandBlock(content, inputStream, readBlockLazily, Option.of(logBlockContentLoc), header, footer);

      case CDC_DATA_BLOCK:
        return new HoodieCDCDataBlock(inputStream, content, readBlockLazily, logBlockContentLoc, readerSchema, header, keyField);

      default:
        throw new HoodieNotSupportedException("Unsupported Block " + blockType);
    }
  }

  @Nullable
  private HoodieLogBlockType tryReadBlockType(HoodieLogFormat.LogFormatVersion blockVersion) throws IOException {
    if (blockVersion.getVersion() == HoodieLogFormatVersion.DEFAULT_VERSION) {
      return null;
    }

    int type = inputStream.readInt();
    checkArgument(type < HoodieLogBlockType.values().length, "Invalid block byte type found " + type);
    return HoodieLogBlockType.values()[type];
  }

  private HoodieLogBlock createCorruptBlock() throws IOException {
    LOG.info("Log " + logFile + " has a corrupted block at " + inputStream.getPos());
    long currentPos = inputStream.getPos();
    long nextBlockOffset = scanForNextAvailableBlockOffset();
    // Rewind to the initial start and read corrupted bytes till the nextBlockOffset
    inputStream.seek(currentPos);
    LOG.info("Next available block in " + logFile + " starts at " + nextBlockOffset);
    int corruptedBlockSize = (int) (nextBlockOffset - currentPos);
    long contentPosition = inputStream.getPos();
    Option<byte[]> corruptedBytes = HoodieLogBlock.tryReadContent(inputStream, corruptedBlockSize, readBlockLazily);
    HoodieLogBlock.HoodieLogBlockContentLocation logBlockContentLoc =
        new HoodieLogBlock.HoodieLogBlockContentLocation(hadoopConf, logFile, contentPosition, corruptedBlockSize, nextBlockOffset);
    return new HoodieCorruptBlock(corruptedBytes, inputStream, readBlockLazily, Option.of(logBlockContentLoc), new HashMap<>(), new HashMap<>());
  }

  private boolean isBlockCorrupt(int blocksize) throws IOException {
    long currentPos = inputStream.getPos();
    long blockSizeFromFooter;
    
    try {
      // check if the blocksize mentioned in the footer is the same as the header;
      // by seeking and checking the length of a long.  We do not seek `currentPos + blocksize`
      // which can be the file size for the last block in the file, causing EOFException
      // for some FSDataInputStream implementation
      inputStream.seek(currentPos + blocksize - Long.BYTES);
      // Block size in the footer includes the magic header, which the header does not include.
      // So we have to shorten the footer block size by the size of magic hash
      blockSizeFromFooter = inputStream.readLong() - magicBuffer.length;
    } catch (EOFException e) {
      LOG.info("Found corrupted block in file " + logFile + " with block size(" + blocksize + ") running past EOF");
      // this is corrupt
      // This seek is required because contract of seek() is different for naked DFSInputStream vs BufferedFSInputStream
      // release-3.1.0-RC1/DFSInputStream.java#L1455
      // release-3.1.0-RC1/BufferedFSInputStream.java#L73
      return true;
    }

    if (blocksize != blockSizeFromFooter) {
      LOG.info("Found corrupted block in file " + logFile + ". Header block size(" + blocksize
          + ") did not match the footer block size(" + blockSizeFromFooter + ")");
      inputStream.seek(currentPos);
      return true;
    }

    try {
      readMagic();
      // all good - either we found the sync marker or EOF. Reset position and continue
      return false;
    } catch (CorruptedLogFileException e) {
      // This is a corrupted block
      LOG.info("Found corrupted block in file " + logFile + ". No magic hash found right after footer block size entry");
      return true;
    } finally {
      inputStream.seek(currentPos);
    }
  }

  private long scanForNextAvailableBlockOffset() throws IOException {
    // Make buffer large enough to scan through the file as quick as possible especially if it is on S3/GCS.
    byte[] dataBuf = new byte[BLOCK_SCAN_READ_BUFFER_SIZE];
    boolean eof = false;
    while (true) {
      long currentPos = inputStream.getPos();
      try {
        Arrays.fill(dataBuf, (byte) 0);
        inputStream.readFully(dataBuf, 0, dataBuf.length);
      } catch (EOFException e) {
        eof = true;
      }
      long pos = Bytes.indexOf(dataBuf, HoodieLogFormat.MAGIC);
      if (pos >= 0) {
        return currentPos + pos;
      }
      if (eof) {
        return inputStream.getPos();
      }
      inputStream.seek(currentPos + dataBuf.length - HoodieLogFormat.MAGIC.length);
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      this.inputStream.close();
      if (null != shutdownThread) {
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
      }
      closed = true;
    }
  }

  /*
   * hasNext is not idempotent. TODO - Fix this. It is okay for now - PR
   */
  @Override
  public boolean hasNext() {
    try {
      return readMagic();
    } catch (IOException e) {
      throw new HoodieIOException("IOException when reading logfile " + logFile, e);
    }
  }

  /**
   * Read log format version from log file.
   */
  private HoodieLogFormat.LogFormatVersion readVersion() throws IOException {
    return new HoodieLogFormatVersion(inputStream.readInt());
  }

  private boolean readMagic() throws IOException {
    try {
      boolean hasMagic = hasNextMagic();
      if (!hasMagic) {
        throw new CorruptedLogFileException(
            logFile + " could not be read. Did not find the magic bytes at the start of the block");
      }
      return hasMagic;
    } catch (EOFException e) {
      // We have reached the EOF
      return false;
    }
  }

  private boolean hasNextMagic() throws IOException {
    // 1. Read magic header from the start of the block
    inputStream.readFully(magicBuffer, 0, 6);
    return Arrays.equals(magicBuffer, HoodieLogFormat.MAGIC);
  }

  @Override
  public HoodieLogBlock next() {
    try {
      // hasNext() must be called before next()
      return readBlock();
    } catch (IOException io) {
      throw new HoodieIOException("IOException when reading logblock from log file " + logFile, io);
    }
  }

  /**
   * hasPrev is not idempotent.
   */
  @Override
  public boolean hasPrev() {
    try {
      if (!this.reverseReader) {
        throw new HoodieNotSupportedException("Reverse log reader has not been enabled");
      }
      reverseLogFilePosition = lastReverseLogFilePosition;
      reverseLogFilePosition -= Long.BYTES;
      lastReverseLogFilePosition = reverseLogFilePosition;
      inputStream.seek(reverseLogFilePosition);
    } catch (Exception e) {
      // Either reached EOF while reading backwards or an exception
      return false;
    }
    return true;
  }

  /**
   * This is a reverse iterator Note: At any point, an instance of HoodieLogFileReader should either iterate reverse
   * (prev) or forward (next). Doing both in the same instance is not supported WARNING : Every call to prev() should be
   * preceded with hasPrev()
   */
  @Override
  public HoodieLogBlock prev() throws IOException {

    if (!this.reverseReader) {
      throw new HoodieNotSupportedException("Reverse log reader has not been enabled");
    }
    long blockSize = inputStream.readLong();
    long blockEndPos = inputStream.getPos();
    // blocksize should read everything about a block including the length as well
    try {
      inputStream.seek(reverseLogFilePosition - blockSize);
    } catch (Exception e) {
      // this could be a corrupt block
      inputStream.seek(blockEndPos);
      throw new CorruptedLogFileException("Found possible corrupted block, cannot read log file in reverse, "
          + "fallback to forward reading of logfile");
    }
    boolean hasNext = hasNext();
    reverseLogFilePosition -= blockSize;
    lastReverseLogFilePosition = reverseLogFilePosition;
    return next();
  }

  /**
   * Reverse pointer, does not read the block. Return the current position of the log file (in reverse) If the pointer
   * (inputstream) is moved in any way, it is the job of the client of this class to seek/reset it back to the file
   * position returned from the method to expect correct results
   */
  public long moveToPrev() throws IOException {

    if (!this.reverseReader) {
      throw new HoodieNotSupportedException("Reverse log reader has not been enabled");
    }
    inputStream.seek(lastReverseLogFilePosition);
    long blockSize = inputStream.readLong();
    // blocksize should be everything about a block including the length as well
    inputStream.seek(reverseLogFilePosition - blockSize);
    reverseLogFilePosition -= blockSize;
    lastReverseLogFilePosition = reverseLogFilePosition;
    return reverseLogFilePosition;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove not supported for HoodieLogFileReader");
  }

  /**
   * Fetch the right {@link FSDataInputStream} to be used by wrapping with required input streams.
   * @param fs instance of {@link FileSystem} in use.
   * @param bufferSize buffer size to be used.
   * @return the right {@link FSDataInputStream} as required.
   */
  private static FSDataInputStream getFSDataInputStream(FileSystem fs,
                                                        HoodieLogFile logFile,
                                                        int bufferSize) throws IOException {
    FSDataInputStream fsDataInputStream = fs.open(logFile.getPath(), bufferSize);

    if (FSUtils.isGCSFileSystem(fs)) {
      // in GCS FS, we might need to interceptor seek offsets as we might get EOF exception
      return new SchemeAwareFSDataInputStream(getFSDataInputStreamForGCS(fsDataInputStream, logFile, bufferSize), true);
    }

    if (FSUtils.isCHDFileSystem(fs)) {
      return new BoundedFsDataInputStream(fs, logFile.getPath(), fsDataInputStream);
    }

    if (fsDataInputStream.getWrappedStream() instanceof FSInputStream) {
      return new TimedFSDataInputStream(logFile.getPath(), new FSDataInputStream(
          new BufferedFSInputStream((FSInputStream) fsDataInputStream.getWrappedStream(), bufferSize)));
    }

    // fsDataInputStream.getWrappedStream() maybe a BufferedFSInputStream
    // need to wrap in another BufferedFSInputStream the make bufferSize work?
    return fsDataInputStream;
  }

  /**
   * GCS FileSystem needs some special handling for seek and hence this method assists to fetch the right {@link FSDataInputStream} to be
   * used by wrapping with required input streams.
   * @param fsDataInputStream original instance of {@link FSDataInputStream}.
   * @param bufferSize buffer size to be used.
   * @return the right {@link FSDataInputStream} as required.
   */
  private static FSDataInputStream getFSDataInputStreamForGCS(FSDataInputStream fsDataInputStream,
                                                              HoodieLogFile logFile,
                                                              int bufferSize) {
    // incase of GCS FS, there are two flows.
    // a. fsDataInputStream.getWrappedStream() instanceof FSInputStream
    // b. fsDataInputStream.getWrappedStream() not an instanceof FSInputStream, but an instance of FSDataInputStream.
    // (a) is handled in the first if block and (b) is handled in the second if block. If not, we fallback to original fsDataInputStream
    if (fsDataInputStream.getWrappedStream() instanceof FSInputStream) {
      return new TimedFSDataInputStream(logFile.getPath(), new FSDataInputStream(
          new BufferedFSInputStream((FSInputStream) fsDataInputStream.getWrappedStream(), bufferSize)));
    }

    if (fsDataInputStream.getWrappedStream() instanceof FSDataInputStream
        && ((FSDataInputStream) fsDataInputStream.getWrappedStream()).getWrappedStream() instanceof FSInputStream) {
      FSInputStream inputStream = (FSInputStream)((FSDataInputStream) fsDataInputStream.getWrappedStream()).getWrappedStream();
      return new TimedFSDataInputStream(logFile.getPath(),
          new FSDataInputStream(new BufferedFSInputStream(inputStream, bufferSize)));
    }

    return fsDataInputStream;
  }
}
