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
import com.uber.hoodie.exception.CorruptedLogFileException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Scans a log file and provides block level iterator on the log file
 * Loads the entire block contents in memory
 * Can emit either a DataBlock, CommandBlock, DeleteBlock or CorruptBlock (if one is found)
 */
public class HoodieLogFormatReader implements HoodieLogFormat.Reader {
  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private final static Logger log = LogManager.getLogger(HoodieLogFormatReader.class);

  private final FSDataInputStream inputStream;
  private final HoodieLogFile logFile;
  private static final byte[] magicBuffer = new byte[4];
  private final Schema readerSchema;
  private HoodieLogBlock nextBlock = null;
  private boolean readMetadata = true;

  HoodieLogFormatReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, int bufferSize, boolean readMetadata) throws IOException {
    this.inputStream = fs.open(logFile.getPath(), bufferSize);
    this.logFile = logFile;
    this.readerSchema = readerSchema;
    this.readMetadata = readMetadata;
  }

  HoodieLogFormatReader(FileSystem fs, HoodieLogFile logFile, Schema readerSchema, boolean readMetadata) throws IOException {
    this(fs, logFile, readerSchema, DEFAULT_BUFFER_SIZE, readMetadata);
  }

  @Override
  public HoodieLogFile getLogFile() {
    return logFile;
  }

  private HoodieLogBlock readBlock() throws IOException {
    // 2. Read the block type
    int ordinal = inputStream.readInt();
    Preconditions.checkArgument(ordinal < HoodieLogBlockType.values().length,
        "Invalid block byte ordinal found " + ordinal);
    HoodieLogBlockType blockType = HoodieLogBlockType.values()[ordinal];

    // 3. Read the size of the block
    int blocksize = inputStream.readInt();

    // We may have had a crash which could have written this block partially
    // Skip blocksize in the stream and we should either find a sync marker (start of the next block) or EOF
    // If we did not find either of it, then this block is a corrupted block.
    boolean isCorrupted = isBlockCorrupt(blocksize);
    if(isCorrupted) {
      return createCorruptBlock();
    }

    // 4. Read the content
    // TODO - have a max block size and reuse this buffer in the ByteBuffer (hard to guess max block size for now)
    byte[] content = new byte[blocksize];
    inputStream.readFully(content, 0, blocksize);

    switch (blockType) {
      // based on type read the block
      case AVRO_DATA_BLOCK:
        return HoodieAvroDataBlock.fromBytes(content, readerSchema, readMetadata);
      case DELETE_BLOCK:
        return HoodieDeleteBlock.fromBytes(content, readMetadata);
      case COMMAND_BLOCK:
        return HoodieCommandBlock.fromBytes(content, readMetadata);
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
    byte[] content = new byte[corruptedBlockSize];
    inputStream.readFully(content, 0, corruptedBlockSize);
    return HoodieCorruptBlock.fromBytes(content, corruptedBlockSize, true);
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
    while(true) {
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

  private boolean readMagic() throws IOException {
    try {
      // 1. Read magic header from the start of the block
      inputStream.readFully(magicBuffer, 0, 4);
      if (!Arrays.equals(magicBuffer, HoodieLogFormat.MAGIC)) {
        throw new CorruptedLogFileException(
            logFile + "could not be read. Did not find the magic bytes at the start of the block");
      }
      return false;
    } catch (EOFException e) {
      // We have reached the EOF
      return true;
    }
  }

  @Override
  public HoodieLogBlock next() {
    if(nextBlock == null) {
      // may be hasNext is not called
      hasNext();
    }
    return nextBlock;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove not supported for HoodieLogFormatReader");
  }
}
