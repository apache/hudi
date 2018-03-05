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
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieCommandBlock;
import com.uber.hoodie.common.table.log.block.HoodieDeleteBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Deque;
import java.util.List;

public class HoodieLogFormatReaderWrapper implements HoodieLogFormat.Reader {

  // Store the last instant log blocks (needed to implement rollback)
  private final List<HoodieLogFile> logFiles;
  private HoodieLogFormatReader currentReader;
  private final FileSystem fs;
  private final Schema readerSchema;
  private final boolean ioIntensiveReader;

  HoodieLogFormatReaderWrapper(FileSystem fs, List<HoodieLogFile> logFiles,
                                  Schema readerSchema, boolean ioIntensiveReader) throws IOException {
    this.logFiles = logFiles;
    this.fs = fs;
    this.readerSchema = readerSchema;
    this.ioIntensiveReader = ioIntensiveReader;
    if(logFiles.size() > 0) {
      this.currentReader = new HoodieLogFormatReader(fs, logFiles.remove(0), readerSchema, ioIntensiveReader);
    }
  }

  HoodieLogFormatReaderWrapper(FileSystem fs, List<HoodieLogFile> logFiles,
                               Schema readerSchema) throws IOException {
    this(fs, logFiles, readerSchema, false);
  }

  @Override
  public void close() throws IOException {
    if (currentReader != null) {
      currentReader.close();
    }
  }

  @Override
  public boolean hasNext() {

    if(currentReader == null) {
      return false;
    }
    else if (currentReader.hasNext()) {
      return true;
    }
    else if (logFiles.size() > 0) {
      try {
        HoodieLogFile nextLogFile = logFiles.remove(0);
        this.currentReader = new HoodieLogFormatReader(fs, nextLogFile, readerSchema, ioIntensiveReader);
      } catch (IOException io) {
        throw new HoodieIOException("unable to initialize read with log file ", io);
      }
      return this.currentReader.hasNext();
    }
    return false;
  }

  @Override
  public HoodieLogBlock next() {
    HoodieLogBlock block = currentReader.next();
    return block;
  }

  @Override
  public HoodieLogFile getLogFile() {
    return currentReader.getLogFile();
  }

  @Override
  public void remove() {
  }

  public HoodieLogFormat.Reader getLazyReader(Deque<HoodieLogBlock> currentLogBlocks) throws IOException {
      return new HoodieLogBlocksLazyReader(fs, currentLogBlocks, this.ioIntensiveReader);
  }

  static class HoodieLogBlocksLazyReader implements HoodieLogFormat.Reader {

    private final static Logger log = LogManager.getLogger(HoodieLogBlocksLazyReader.class);
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    private FSDataInputStream currentInputStream;
    private HoodieLogFile currentLogFile;
    private final Deque<HoodieLogBlock> hoodieLogBlocks;
    private final FileSystem fs;
    private final boolean ioIntensiveReader;

    HoodieLogBlocksLazyReader(FileSystem fs, Deque<HoodieLogBlock> hoodieLogBlocks, boolean ioIntensiveReader) throws IOException {
      this.ioIntensiveReader = ioIntensiveReader;
      this.hoodieLogBlocks = hoodieLogBlocks;
      this.fs = fs;
      init(fs, hoodieLogBlocks.peekLast());
    }

    private void init(FileSystem fs, HoodieLogBlock hoodieLogBlock) throws IOException {
      if (ioIntensiveReader) {
        this.currentInputStream = fs.open(hoodieLogBlock.getBlockContentLocation()
            .get().getLogFile().getPath(), DEFAULT_BUFFER_SIZE);
        this.currentLogFile = hoodieLogBlock.getBlockContentLocation().get().getLogFile();
      }
    }

    @Override
    public HoodieLogFile getLogFile() {
      return currentLogFile;
    }

    @Override
    public void close() throws IOException {
      if (currentInputStream != null) {
        currentInputStream.close();
      }
    }

    @Override
    public boolean hasNext() {
      return hoodieLogBlocks.size() > 0;
    }

    @Override
    public HoodieLogBlock next() {
      try {
        if (ioIntensiveReader) {
          return inflateLogBlock(hoodieLogBlocks.peekLast());
        } else {
          return hoodieLogBlocks.peekLast();
        }
      } catch(IOException io) {
        throw new HoodieIOException("Unable to inflate log block",io);
      }
    }

    @Override
    public void remove() {
      hoodieLogBlocks.pollLast();
    }


    private HoodieLogBlock inflateLogBlock(HoodieLogBlock block) throws IOException {

      HoodieLogBlock.HoodieLogBlockContentLocation logBlockContentLocation = block.getBlockContentLocation().get();
      long position = logBlockContentLocation.getContentPositionInLogFile();
      int blockSize = (int) logBlockContentLocation.getBlockSize();
      HoodieLogFile blockLogFile = logBlockContentLocation.getLogFile();
      if (!this.getLogFile().equals(blockLogFile)) {
        close();
        this.currentInputStream = fs.open(logBlockContentLocation.getLogFile().getPath(), DEFAULT_BUFFER_SIZE);
      }
      byte[] content = new byte[blockSize];
      currentInputStream.seek(position);
      currentInputStream.readFully(content, 0, blockSize);
      switch (block.getBlockType()) {
        // based on type read the block
        case AVRO_DATA_BLOCK:
          return HoodieAvroDataBlock.getBlock(content,
              ((HoodieAvroDataBlock)block).getSchema(), block.getLogBlockHeader(), block.getLogBlockFooter());
        case DELETE_BLOCK:
          return HoodieDeleteBlock.getBlock(content, block.getLogBlockHeader(), block.getLogBlockFooter());
        case COMMAND_BLOCK:
          return HoodieCommandBlock.getBlock(content, block.getLogBlockHeader(), block.getLogBlockFooter());
        default:
          throw new HoodieNotSupportedException("Unsupported Block " + block.getBlockType());
      }
    }
  }
}
