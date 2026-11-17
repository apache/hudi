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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.io.util.FileIOUtils;
import org.apache.hudi.storage.StoragePath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Reads the instant that wrote a log file from its first block's {@code INSTANT_TIME} header (the
 * file name carries the slice's base commit time, not the writing delta commit).
 *
 * <p>The same read reports whether that block is a rollback command block, detected by block type
 * or by a {@code COMMAND_BLOCK_TYPE} header. Its instant header is whatever instant was last on the
 * timeline when written, so it looks like an orphan's, and deleting it resurrects rolled-back data.
 * Reading both signals here guarantees they come from the same block, and keeps the choice between
 * {@code NO_LOG_BLOCK}, {@code UNREADABLE} and {@code NO_INSTANT_HEADER} in one place.
 */
public final class LogFileInstantReader {

  private static final Logger LOG = LoggerFactory.getLogger(LogFileInstantReader.class);

  private LogFileInstantReader() {
  }

  /** A resolved instant or the reason it is unresolved, plus the type of block that was read. */
  public static final class Result {

    private final String instantTime;
    private final HoodieLogBlock.HoodieLogBlockType blockType;
    private final MismatchRecord.InstantUnresolvedReason reason;
    private final boolean commandBlock;

    private Result(String instantTime, HoodieLogBlock.HoodieLogBlockType blockType,
                   MismatchRecord.InstantUnresolvedReason reason, boolean commandBlock) {
      this.instantTime = instantTime;
      this.blockType = blockType;
      this.reason = reason;
      this.commandBlock = commandBlock;
    }

    public static Result of(String instantTime, HoodieLogBlock.HoodieLogBlockType blockType) {
      return new Result(instantTime, blockType, null,
          blockType == HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK);
    }

    public static Result unresolved(MismatchRecord.InstantUnresolvedReason reason) {
      return new Result(null, null, reason, false);
    }

    /** A block was read but carried no instant; its type is kept because it is what refuses a delete. */
    public static Result unresolved(MismatchRecord.InstantUnresolvedReason reason,
                                    HoodieLogBlock.HoodieLogBlockType blockType) {
      return new Result(null, blockType, reason,
          blockType == HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK);
    }

    /**
     * The result for a block that was read; it is a command block if either the block type or the
     * {@code COMMAND_BLOCK_TYPE} header says so. Only rollback code writes that header, so the two
     * detections fail independently.
     */
    public static Result fromHeader(Map<HoodieLogBlock.HeaderMetadataType, String> header,
                                    HoodieLogBlock.HoodieLogBlockType blockType) {
      boolean isCommandBlock =
          blockType == HoodieLogBlock.HoodieLogBlockType.COMMAND_BLOCK
              || (header != null
                  && header.containsKey(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE));
      String instantTime =
          header == null ? null : header.get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME);
      if (instantTime == null) {
        return new Result(null, blockType,
            MismatchRecord.InstantUnresolvedReason.NO_INSTANT_HEADER, isCommandBlock);
      }
      return new Result(instantTime, blockType, null, isCommandBlock);
    }

    /** Null when unresolved. */
    public String getInstantTime() {
      return instantTime;
    }

    /**
     * The type of the first block, or {@code null} when no block was read. Callers must treat
     * {@code null} as unknown, never as "not a command block".
     */
    public HoodieLogBlock.HoodieLogBlockType getBlockType() {
      return blockType;
    }

    /** Null when resolved. */
    public MismatchRecord.InstantUnresolvedReason getReason() {
      return reason;
    }

    public boolean isResolved() {
      return instantTime != null;
    }

    /** Whether this file is a rollback marker and must not be adjudicated at all. */
    public boolean isCommandBlock() {
      return commandBlock;
    }
  }

  /**
   * Reads the first block's header. Never throws: a read failure is returned as an unresolved
   * {@link Result}, since it is not evidence that the table is broken.
   *
   * <p>No reader schema is resolved first on purpose: schema resolution only succeeds on data
   * blocks, so a file holding only a rollback command block would come back {@code UNREADABLE} and
   * the command-block refusal could never fire. A block header can be read with a null schema.
   */
  public static Result read(HoodieTableMetaClient metaClient, StoragePath logFilePath) {
    HoodieLogFile logFile = new HoodieLogFile(logFilePath);
    HoodieLogFormat.Reader reader = null;
    try {
      reader = HoodieLogFormat.newReader(metaClient, logFile, null, false);
      if (!reader.hasNext()) {
        return Result.unresolved(MismatchRecord.InstantUnresolvedReason.NO_LOG_BLOCK);
      }
      HoodieLogBlock block = reader.next();
      return Result.fromHeader(block.getLogBlockHeader(), block.getBlockType());
    } catch (Exception e) {
      LOG.warn("Could not read an instant out of log file {}; recording it as unresolved",
          logFile.getPath(), e);
      return Result.unresolved(MismatchRecord.InstantUnresolvedReason.UNREADABLE);
    } finally {
      FileIOUtils.closeQuietly(reader);
    }
  }
}
