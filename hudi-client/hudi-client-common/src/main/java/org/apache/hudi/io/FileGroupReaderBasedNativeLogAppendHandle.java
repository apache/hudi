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

package org.apache.hudi.io;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.schema.internal.utils.SerDeHelper;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;

/**
 * File-group-reader based log-compaction append handle for native MOR log files.
 */
@NotThreadSafe
public class FileGroupReaderBasedNativeLogAppendHandle<T, I, K, O> extends HoodieNativeLogAppendHandle<T, I, K, O> {
  private final HoodieReaderContext<T> readerContext;
  private final CompactionOperation operation;
  private HoodieReadStats readStats;

  public FileGroupReaderBasedNativeLogAppendHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                                   CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                                   HoodieReaderContext<T> readerContext) {
    super(config, instantTime, hoodieTable, operation.getPartitionPath(), operation.getFileId(), taskContextSupplier);
    this.operation = operation;
    this.readerContext = readerContext;
  }

  @Override
  public void doAppend() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    TypedProperties props = TypedProperties.copy(config.getProps());
    long maxMemoryPerCompaction = MergeUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
    Stream<HoodieLogFile> logFiles = operation.getDeltaFileNames().stream().map(logFileName ->
        new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
            config.getBasePath(), operation.getPartitionPath()), logFileName)));
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>builder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(instantTime)
        .withPartitionPath(partitionPath)
        .withLogFiles(logFiles)
        .withBaseFileOption(Option.empty())
        .withDataSchema(writeSchemaWithMetaFields)
        .withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchemaOpt(internalSchemaOption)
        .withProps(props)
        .withEmitDelete(true)
        .withShouldUseRecordPosition(usePosition)
        .withSortOutput(hoodieTable.requireSortedRecords())
        // instead of using config.enableOptimizedLogBlocksScan(), we set to true as log compaction blocks only supported in scanV2
        .build()) {
      recordItr = new CloseableMappingIterator<>(fileGroupReader.getLogRecordsOnly(), record -> {
        HoodieRecord<T> hoodieRecord = readerContext.getRecordContext().constructHoodieRecord(record);
        hoodieRecord.setCurrentLocation(newRecordLocation);
        return hoodieRecord;
      });
      header.put(HoodieLogBlock.HeaderMetadataType.COMPACTED_BLOCK_TIMES,
          StringUtils.join(fileGroupReader.getValidBlockInstants(), ","));
      super.doAppend();
      this.readStats = fileGroupReader.getReadStats();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize file group reader for " + fileId, e);
    }
  }

  @Override
  public List<WriteStatus> close() {
    try {
      List<WriteStatus> writeStatuses = super.close();
      // Native log compaction can produce more than one physical output file, for example a data log
      // and a delete log, so keep all write statuses and annotate each output file with compaction identity.
      for (WriteStatus status : writeStatuses) {
        status.getStat().setPartitionPath(operation.getPartitionPath());
        status.getStat().setPrevCommit(operation.getBaseInstantTime());
      }
      // The read stats describe the source file group scanned for this compaction, not each native
      // output file. Set them once so commit-level compaction metrics are not double-counted.
      if (!writeStatuses.isEmpty()) {
        updateCompactionReadStats(writeStatuses.get(0));
      }
      return writeStatuses;
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to close " + this.getClass().getSimpleName(), e);
    }
  }

  private void updateCompactionReadStats(WriteStatus status) {
    status.getStat().setTotalLogReadTimeMs(readStats.getTotalLogReadTimeMs());
    status.getStat().setTotalUpdatedRecordsCompacted(readStats.getTotalUpdatedRecordsCompacted());
    status.getStat().setTotalLogFilesCompacted(readStats.getTotalLogFilesCompacted());
    status.getStat().setTotalLogRecords(readStats.getTotalLogRecords());
    status.getStat().setTotalLogBlocks(readStats.getTotalLogBlocks());
    status.getStat().setTotalCorruptLogBlock(readStats.getTotalCorruptLogBlock());
    status.getStat().setTotalRollbackBlocks(readStats.getTotalRollbackBlocks());
    status.getStat().setTotalLogSizeCompacted(readStats.getTotalLogSizeCompacted());

    if (status.getStat().getRuntimeStats() != null) {
      status.getStat().getRuntimeStats().setTotalScanTime(readStats.getTotalLogReadTimeMs());
    }
  }
}
