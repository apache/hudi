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

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieCorruptBlock;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import com.uber.hoodie.exception.CorruptedLogFileException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Scans a log file and provides block level iterator on the log file Loads the entire block
 * contents in memory Can emit either a DataBlock, CommandBlock, DeleteBlock or CorruptBlock (if one
 * is found)
 */
public class HoodieLogFormatReader implements HoodieLogFormat.Reader {

  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private final static Logger log = LogManager.getLogger(HoodieLogFormatReader.class);

  private final FSDataInputStream inputStream;
  private final HoodieLogFile logFile;
  private static final byte[] magicBuffer = new byte[4];
  private static final byte[] magicBufferV2 = new byte[6];
  private final Schema readerSchema;
  private HoodieLogBlock nextBlock = null;
  private LogBlockVersion nextBlockVersion;
  private boolean ioIntensiveReaderSupport; // DEFAULT has to be false
  private long reverseLogFilePosition;
  private long lastReverseLogFilePosition;

  HoodieLogFormatReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize,
                        boolean ioIntensiveReaderSupport) throws IOException {
    this.inputStream = fs.open(logFile.getPath(), bufferSize);
    this.logFile = logFile;
    this.readerSchema = readerSchema;
    this.ioIntensiveReaderSupport = ioIntensiveReaderSupport;
    this.reverseLogFilePosition = this.lastReverseLogFilePosition = fs.getFileStatus(logFile.getPath()).getLen();
  }

  HoodieLogFormatReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema,
      boolean ioIntensiveReaderSupport) throws IOException {
    this(fs, logFile, readerSchema, DEFAULT_BUFFER_SIZE, ioIntensiveReaderSupport);
  }

  HoodieLogFormatReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema) throws IOException {
    this(fs, logFile, readerSchema, DEFAULT_BUFFER_SIZE, false);
  }

  @Override
  public HoodieLogFile getLogFile() {
    return logFile;
  }

  private HoodieLogBlock readBlock() throws IOException {

    int blocksize = -1; int ordinal = -1;
    HoodieLogBlockType blockType = null;
    Map<HeaderMetadataType, String> header = null;

    try {

      if (isOldMagic()) {
        // 1 Read the block type for a log block
        ordinal = inputStream.readInt();

        Preconditions.checkArgument(ordinal < HoodieLogBlockType.values().length,
            "Invalid block byte ordinal found " + ordinal);
        blockType = HoodieLogBlockType.values()[ordinal];

        // 2 Read the total size of the block
        blocksize = inputStream.readInt();
      } else {
        // 1 Read the total size of the block
        blocksize = inputStream.readInt();
      }

    } catch(Exception e) {
      // An exception reading any of the above indicates a corrupt block
      // Create a corrupt block by finding the next MAGIC marker or EOF
      return createCorruptBlock();
    }

    // We may have had a crash which could have written this block partially
    // Skip blocksize in the stream and we should either find a sync marker (start of the next block) or EOF
    // If we did not find either of it, then this block is a corrupted block.
    boolean isCorrupted = isBlockCorrupt(blocksize);
    if (isCorrupted) {
      return createCorruptBlock();
    }

    // 2. Read the version for this log format
    this.nextBlockVersion = readVersion();

    // 3. Read the block type for a log block
    if (nextBlockVersion.getVersion() != HoodieLogBlockVersion.DEFAULT_VERSION) {
      ordinal = inputStream.readInt();

      Preconditions.checkArgument(ordinal < HoodieLogBlockType.values().length,
          "Invalid block byte ordinal found " + ordinal);
      blockType = HoodieLogBlockType.values()[ordinal];
    }

    // 4. Read the header for a log block, if present
    if (nextBlockVersion.hasHeader()) {
      header = HoodieLogBlock.getLogMetadata(inputStream);
    }

    int contentLength = blocksize;
    // 5. Read the content length for the content
    if (nextBlockVersion.getVersion() != HoodieLogBlockVersion.DEFAULT_VERSION) {
      contentLength = inputStream.readInt();
    }

    // 4. Read the content or skip content based on IO vs Memory trade-off by client
    // TODO - have a max block size and reuse this buffer in the ByteBuffer (hard to guess max block size for now)
    long contentPosition = inputStream.getPos();
    byte [] content = null;
    if (!ioIntensiveReaderSupport) {
      // Read the contents in memory
      content = new byte[contentLength];
      inputStream.readFully(content, 0, contentLength);
    } else {
      // Seek to the end of the content block
      inputStream.seek(inputStream.getPos() + contentLength);
    }

    // 5. Read footer if any
    Map<HeaderMetadataType, String> footer = null;
    if (nextBlockVersion.hasFooter()) {
      footer = HoodieLogBlock.getLogMetadata(inputStream);
    }

    // 6. Read log block length, if present. This acts as a reverse pointer when traversing a log file in reverse
    long logBlockLength = 0;
    if (nextBlockVersion.hasLogBlockLength()) {
      logBlockLength = inputStream.readLong();
    }

    log.info("Length of the log block " + logBlockLength);

    switch (blockType) {
      // based on type read the block
      case AVRO_DATA_BLOCK:
        if (nextBlockVersion.getVersion() == HoodieLogBlockVersion.DEFAULT_VERSION) {
          return HoodieAvroDataBlock.fromBytes(content, readerSchema);
        } else if (ioIntensiveReaderSupport){
          return HoodieAvroDataBlock.getBlock(logFile, contentPosition, contentLength, readerSchema, header, footer);
        } else {
          return HoodieAvroDataBlock.getBlock(content, readerSchema, header, footer);
        }
      case DELETE_BLOCK:
        if (ioIntensiveReaderSupport) {
          return HoodieDeleteBlock.getBlock(logFile, contentPosition, contentLength, header, footer);
        } else {
          return HoodieDeleteBlock.getBlock(content, header, footer);
        }
      case COMMAND_BLOCK:
        if (ioIntensiveReaderSupport) {
          return HoodieCommandBlock.getBlock(logFile, contentPosition, contentLength, header, footer);
        } else{
          return HoodieCommandBlock.getBlock(content, header, footer);
        }
      default:
        throw new HoodieNotSupportedException("Unsupported Block " + blockType);
    }
  }

  private HoodieLogBlock createCorruptBlock() throws IOException {
    log.info("Log " + logFile + " has a corrupted block at " + inputStream.getPos());
    long currentPos = inputStream.getPos();
    long nextBlockOffset = scanForNextAvailableBlockOffset();
    // Rewind to the initial start and read corrupted bytes till the nextBlockOffset
    inputStream.seek(currentPos);
    log.info("Next available block in " + logFile + " starts at " + nextBlockOffset);
    int corruptedBlockSize = (int) (nextBlockOffset - currentPos);
    if (ioIntensiveReaderSupport) {
      byte[] content = new byte[corruptedBlockSize];
      inputStream.readFully(content, 0, corruptedBlockSize);
      return HoodieCorruptBlock.getBlock(content, new HashMap<>(), new HashMap<>());
    } else {
      inputStream.seek(currentPos + corruptedBlockSize);
      return HoodieCorruptBlock.getBlock(logFile, currentPos, corruptedBlockSize, new HashMap<>(), new HashMap<>());
    }
  }

  private boolean isBlockCorrupt(int blocksize) throws IOException {
    long currentPos = inputStream.getPos();
    try {
      inputStream.seek(currentPos + blocksize);
    } catch (EOFException e) {
      // this is corrupt
      return true;
    }

    try {
      readMagic();
      // all good - either we found the sync marker or EOF. Reset position and continue
      return false;
    } catch (CorruptedLogFileException e) {
      // This is a corrupted block
      return true;
    } finally {
      inputStream.seek(currentPos);
    }
  }

  private long scanForNextAvailableBlockOffset() throws IOException {
    while (true) {
      long currentPos = inputStream.getPos();
      try {
        boolean isEOF = readMagic();
        return isEOF ? inputStream.getPos() : currentPos;
      } catch (CorruptedLogFileException e) {
        // No luck - advance and try again
        inputStream.seek(currentPos + 1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    this.inputStream.close();
  }

  @Override
  /**
   * hasNext is not idempotent. TODO - Fix this. It is okay for now - PR
   */
  public boolean hasNext() {
    try {
      boolean isEOF = readMagic();
      if (isEOF) {
        return false;
      }
      this.nextBlock = readBlock();
      return nextBlock != null;
    } catch (IOException e) {
      throw new HoodieIOException("IOException when reading logfile " + logFile, e);
    }
  }

  /**
   * Read log format version from log file, if present
   * For old log files written with Magic header MAGIC and without version, return DEFAULT_VERSION
   * @throws IOException
   */
  private LogBlockVersion readVersion() throws IOException {
    // If not old log file format (written with Magic header MAGIC), then read log version
    if (Arrays.equals(magicBuffer, HoodieLogFormat.MAGIC)) {
      java.util.Arrays.fill(magicBuffer, (byte) 0);
      return new HoodieLogBlockVersion(HoodieLogBlockVersion.DEFAULT_VERSION);
    }
    return new HoodieLogBlockVersion(inputStream.readInt());
  }

  private boolean isOldMagic() {
    return Arrays.equals(magicBuffer, HoodieLogFormat.MAGIC);
  }


  private boolean readMagic() throws IOException {
    try {
      long pos = inputStream.getPos();
      // 1. Read magic header from the start of the block
      inputStream.readFully(magicBufferV2, 0, 6);
      if (!Arrays.equals(magicBufferV2, HoodieLogFormat.MAGIC_V2)) {
        inputStream.seek(pos);
        // 1. Read old magic header from the start of the block
        // (for backwards compatibility of older log files written without log version)
        inputStream.readFully(magicBuffer, 0, 4);
        if (!Arrays.equals(magicBuffer, HoodieLogFormat.MAGIC)) {
          throw new CorruptedLogFileException(
              logFile + "could not be read. Did not find the magic bytes at the start of the block");
        }
      }
      return false;
    } catch (EOFException e) {
      // We have reached the EOF
      return true;
    }
  }

  @Override
  public HoodieLogBlock next() {
    if (nextBlock == null) {
      // may be hasNext is not called
      hasNext();
    }
    return nextBlock;
  }

  /**
   * hasPrev is not idempotent
   * @return
   */
  public boolean hasPrev() {
    try {
      reverseLogFilePosition = lastReverseLogFilePosition;
      reverseLogFilePosition -= Long.BYTES;
      lastReverseLogFilePosition = reverseLogFilePosition;
      inputStream.seek(reverseLogFilePosition);
    } catch(Exception e) {
      // Either reached EOF while reading backwards or an exception
      return false;
    }
    return true;
  }

  /**
   * This is a reverse iterator
   * Note: At any point, an instance of HoodieLogFormatReader should either iterate reverse (prev)
   * or forward (next). Doing both in the same instance is not supported
   * WARNING : Every call to prev() should be preceded with hasPrev()
   * @return
   * @throws IOException
   */
  public HoodieLogBlock prev() throws IOException {

    long blockSize = inputStream.readLong();
    long blockEndPos = inputStream.getPos();
    // blocksize should read everything about a block including the length as well
    try {
      inputStream.seek(reverseLogFilePosition - blockSize);
    } catch(Exception e) {
      // this could be a corrupt block
      inputStream.seek(blockEndPos);
      throw new CorruptedLogFileException("Found possible corrupted block, cannot read log file in reverse, " +
          "fallback to forward reading of logfile");
    }
    boolean hasNext = hasNext();
    reverseLogFilePosition -= blockSize;
    lastReverseLogFilePosition = reverseLogFilePosition;
    return this.nextBlock;
  }

  /**
   * Reverse pointer, does not read the block. Return the current position of the log file (in reverse)
   * If the pointer (inputstream) is moved in any way, it is the job of the client of this class to
   * seek/reset it back to the file position returned from the method to expect correct results
   * @return
   * @throws IOException
   */
  public long moveToPrev() throws IOException {

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
    throw new UnsupportedOperationException("Remove not supported for HoodieLogFormatReader");
  }
}
