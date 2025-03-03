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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;
import static org.apache.hudi.common.model.HoodieFileFormat.HFILE;

/**
 * A merge handle implementation based on the {@link HoodieFileGroupReader}.
 * <p>
 * This merge handle is used for compaction, which passes a file slice from the
 * compaction operation of a single file group to a file group reader, get an iterator of
 * the records, and writes the records to a new base file.
 */
@NotThreadSafe
public class FileGroupReaderBasedMergeHandle<T, I, K, O> extends HoodieDefaultMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(FileGroupReaderBasedMergeHandle.class);

  private final HoodieReaderContext<T> readerContext;
  private final FileSlice fileSlice;
  private final CompactionOperation operation;
  private final String maxInstantTime;
  private HoodieReadStats readStats;
  private final HoodieRecord.HoodieRecordType recordType;

  public FileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                         FileSlice fileSlice, CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                         HoodieReaderContext<T> readerContext, String maxInstantTime,
                                         HoodieRecord.HoodieRecordType enginRecordType) {
    super(config, instantTime, hoodieTable, Collections.emptyMap(), operation.getPartitionPath(), operation.getFileId(),
        operation.getBaseFile(config.getBasePath(), operation.getPartitionPath()).orElse(null), taskContextSupplier, keyGeneratorOpt);
    this.maxInstantTime = maxInstantTime;
    this.keyToNewRecords = Collections.emptyMap();
    this.readerContext = readerContext;
    this.conf = conf;
    Option<HoodieBaseFile> baseFileOpt =
        operation.getBaseFile(config.getBasePath(), operation.getPartitionPath());
    List<HoodieLogFile> logFiles = operation.getDeltaFileNames().stream().map(p ->
            new HoodieLogFile(new StoragePath(FSUtils.constructAbsolutePath(
                config.getBasePath(), operation.getPartitionPath()), p)))
        .collect(Collectors.toList());
    this.fileSlice = new FileSlice(
        operation.getFileGroupId(),
        operation.getBaseInstantTime(),
        baseFileOpt.isPresent() ? baseFileOpt.get() : null,
        logFiles);
    this.preserveMetadata = true;
    setAdditionalMetrics(operation);
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  private void validateAndSetAndKeyGenProps(Option<BaseKeyGenerator> keyGeneratorOpt, boolean populateMetaFields) {
    ValidationUtils.checkArgument(populateMetaFields == !keyGeneratorOpt.isPresent());
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  @Override
  protected HoodieRecord.HoodieRecordType getRecordType() {
    return HoodieRecord.HoodieRecordType.SPARK;
  }

  protected void setAdditionalMetrics(CompactionOperation operation) {
    writeStatus.getStat().setTotalLogSizeCompacted(
        operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
  }

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file.
   */
  public void write() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    TypedProperties props = TypedProperties.copy(config.getProps());
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, config);
    props.put(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(maxMemoryPerCompaction));
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = HoodieFileGroupReader.<T>newBuilder().withReaderContext(readerContext).withHoodieTableMetaClient(hoodieTable.getMetaClient())
        .withLatestCommitTime(maxInstantTime).withFileSlice(fileSlice).withDataSchema(writeSchemaWithMetaFields).withRequestedSchema(writeSchemaWithMetaFields)
        .withInternalSchema(internalSchemaOption).withProps(props).withShouldUseRecordPosition(usePosition).withSortOutput(hoodieTable.requireSortedRecords()).build()) {
      // Reads the records from the file slice
      try (ClosableIterator<HoodieRecord<T>> recordIterator = fileGroupReader.getClosableHoodieRecordIterator()) {
        while (recordIterator.hasNext()) {
          HoodieRecord<T> record = recordIterator.next();
          record.setCurrentLocation(newRecordLocation);
          record.setNewLocation(newRecordLocation);
          Option<Map<String, String>> recordMetadata = record.getMetadata();
          if (!partitionPath.equals(record.getPartitionPath())) {
            HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
                + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
            writeStatus.markFailure(record, failureEx, recordMetadata);
            continue;
          }
          // Writes the record
          try {
            writeToFile(record.getKey(), record, writeSchemaWithMetaFields,
                config.getPayloadConfig().getProps(), preserveMetadata);
            writeStatus.markSuccess(record, recordMetadata);
          } catch (Exception e) {
            LOG.error("Error writing record {}", record, e);
            writeStatus.markFailure(record, e, recordMetadata);
          }
        }

        // The stats of inserts, updates, and deletes are updated once at the end
        // These will be set in the write stat when closing the merge handle
        this.readStats = fileGroupReader.getStats();
        this.insertRecordsWritten = readStats.getNumInserts();
        this.updatedRecordsWritten = readStats.getNumUpdates();
        this.recordsDeleted = readStats.getNumDeletes();
        this.recordsWritten = readStats.getNumInserts() + readStats.getNumUpdates();
      }
    } catch (IOException e) {
      throw new HoodieUpsertException("Failed to compact file slice: " + fileSlice, e);
    }
  }

  @Override
  protected void writeIncomingRecords() {
    // no operation.
  }

  @Override
  public List<WriteStatus> close() {
    try {
      super.close();
      writeStatus.getStat().setTotalLogReadTimeMs(readStats.getTotalLogReadTimeMs());
      writeStatus.getStat().setTotalUpdatedRecordsCompacted(readStats.getTotalUpdatedRecordsCompacted());
      writeStatus.getStat().setTotalLogFilesCompacted(readStats.getTotalLogFilesCompacted());
      writeStatus.getStat().setTotalLogRecords(readStats.getTotalLogRecords());
      writeStatus.getStat().setTotalLogBlocks(readStats.getTotalLogBlocks());
      writeStatus.getStat().setTotalCorruptLogBlock(readStats.getTotalCorruptLogBlock());
      writeStatus.getStat().setTotalRollbackBlocks(readStats.getTotalRollbackBlocks());
      writeStatus.getStat().setTotalLogSizeCompacted(operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());

      if (writeStatus.getStat().getRuntimeStats() != null) {
        writeStatus.getStat().getRuntimeStats().setTotalScanTime(readStats.getTotalLogReadTimeMs());
      }
      return Collections.singletonList(writeStatus);
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to close " + this.getClass().getSimpleName(), e);
    }
  }
}
