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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.MetadataValues;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieFileGroupReader.HoodieFileGroupReaderIterator;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS;

/**
 * A merge handle implementation based on the {@link HoodieFileGroupReader}.
 * <p>
 * This merge handle is used for compaction on Spark, which passes a file slice from the
 * compaction operation of a single file group to a file group reader, get an iterator of
 * the records, and writes the records to a new base file.
 */
@NotThreadSafe
public class HoodieSparkFileGroupReaderBasedMergeHandle<T, I, K, O> extends HoodieMergeHandle<T, I, K, O> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkFileGroupReaderBasedMergeHandle.class);

  protected HoodieReaderContext readerContext;
  protected FileSlice fileSlice;
  protected Configuration conf;
  protected HoodieReadStats readStats;

  public HoodieSparkFileGroupReaderBasedMergeHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                                                    CompactionOperation operation, TaskContextSupplier taskContextSupplier,
                                                    Option<BaseKeyGenerator> keyGeneratorOpt,
                                                    HoodieReaderContext readerContext, Configuration conf) {
    super(config, instantTime, operation.getPartitionPath(), operation.getFileId(), hoodieTable, taskContextSupplier);
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
    init(operation, this.partitionPath, baseFileOpt);
    validateAndSetAndKeyGenProps(keyGeneratorOpt, config.populateMetaFields());
  }

  private void validateAndSetAndKeyGenProps(Option<BaseKeyGenerator> keyGeneratorOpt, boolean populateMetaFields) {
    ValidationUtils.checkArgument(populateMetaFields == !keyGeneratorOpt.isPresent());
    this.keyGeneratorOpt = keyGeneratorOpt;
  }

  private void init(CompactionOperation operation, String partitionPath, Option<HoodieBaseFile> baseFileToMerge) {
    LOG.info("partitionPath:" + partitionPath + ", fileId to be merged:" + fileId);
    this.baseFileToMerge = baseFileToMerge.orElse(null);
    this.writtenRecordKeys = new HashSet<>();
    writeStatus.setStat(new HoodieWriteStat());
    writeStatus.getStat().setTotalLogSizeCompacted(
        operation.getMetrics().get(CompactionStrategy.TOTAL_LOG_FILE_SIZE).longValue());
    try {
      Option<String> latestValidFilePath = Option.empty();
      if (baseFileToMerge.isPresent()) {
        latestValidFilePath = Option.of(baseFileToMerge.get().getFileName());
        writeStatus.getStat().setPrevCommit(baseFileToMerge.get().getCommitTime());
        // At the moment, we only support SI for overwrite with latest payload. So, we don't need to embed entire file slice here.
        // HUDI-8518 will be taken up to fix it for any payload during which we might require entire file slice to be set here.
        // Already AppendHandle adds all logs file from current file slice to HoodieDeltaWriteStat.
        writeStatus.getStat().setPrevBaseFile(latestValidFilePath.get());
      } else {
        writeStatus.getStat().setPrevCommit(HoodieWriteStat.NULL_COMMIT);
      }

      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(storage, instantTime,
          new StoragePath(config.getBasePath()),
          FSUtils.constructAbsolutePath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave();

      String newFileName = FSUtils.makeBaseFileName(instantTime, writeToken, fileId, hoodieTable.getBaseFileExtension());
      makeOldAndNewFilePaths(partitionPath,
          latestValidFilePath.isPresent() ? latestValidFilePath.get() : null, newFileName);

      LOG.info(String.format(
          "Merging data from file group %s, to a new base file %s", fileId, newFilePath.toString()));
      // file name is same for all records, in this bunch
      writeStatus.setFileId(fileId);
      writeStatus.setPartitionPath(partitionPath);
      writeStatus.getStat().setPartitionPath(partitionPath);
      writeStatus.getStat().setFileId(fileId);
      setWriteStatusPath();

      // Create Marker file,
      // uses name of `newFilePath` instead of `newFileName`
      // in case the sub-class may roll over the file handle name.
      createMarkerFile(partitionPath, newFilePath.getName());

      // Create the writer for writing the new version file
      fileWriter = HoodieFileWriterFactory.getFileWriter(instantTime, newFilePath, hoodieTable.getStorage(),
          config, writeSchemaWithMetaFields, taskContextSupplier, HoodieRecord.HoodieRecordType.SPARK);
    } catch (IOException io) {
      LOG.error("Error in update task at commit " + instantTime, io);
      writeStatus.setGlobalError(io);
      throw new HoodieUpsertException("Failed to initialize HoodieUpdateHandle for FileId: " + fileId + " on commit "
          + instantTime + " on path " + hoodieTable.getMetaClient().getBasePath(), io);
    }
  }

  /**
   * Reads the file slice of a compaction operation using a file group reader,
   * by getting an iterator of the records; then writes the records to a new base file
   * using Spark parquet writer.
   */
  public void write() {
    boolean usePosition = config.getBooleanOrDefault(MERGE_USE_RECORD_POSITIONS);
    Option<InternalSchema> internalSchemaOption = Option.empty();
    if (!StringUtils.isNullOrEmpty(config.getInternalSchema())) {
      internalSchemaOption = SerDeHelper.fromJson(config.getInternalSchema());
    }
    // Initializes file group reader
    try (HoodieFileGroupReader<T> fileGroupReader = new HoodieFileGroupReader<>(
        readerContext,
        storage.newInstance(hoodieTable.getMetaClient().getBasePath(), new HadoopStorageConfiguration(conf)),
        hoodieTable.getMetaClient().getBasePath().toString(),
        instantTime,
        fileSlice,
        writeSchemaWithMetaFields,
        writeSchemaWithMetaFields,
        internalSchemaOption,
        hoodieTable.getMetaClient(),
        hoodieTable.getMetaClient().getTableConfig().getProps(),
        0,
        // The compaction plan does not contain the length of the bootstrap data or skeleton
        // files, so we set -1 as the file length for the file group reader to fetch the
        // length from the file system
        -1,
        usePosition)) {
      fileGroupReader.initRecordIterators();
      // Reads the records from the file slice
      try (HoodieFileGroupReaderIterator<InternalRow> recordIterator
               = (HoodieFileGroupReaderIterator<InternalRow>) fileGroupReader.getClosableIterator()) {
        StructType sparkSchema = AvroConversionUtils.convertAvroSchemaToStructType(writeSchemaWithMetaFields);
        while (recordIterator.hasNext()) {
          // Constructs Spark record for the Spark Parquet file writer
          InternalRow row = recordIterator.next();
          HoodieKey recordKey = new HoodieKey(
              row.getString(HoodieRecord.RECORD_KEY_META_FIELD_ORD),
              row.getString(HoodieRecord.PARTITION_PATH_META_FIELD_ORD));
          HoodieSparkRecord record = new HoodieSparkRecord(recordKey, row, sparkSchema, false);
          Option recordMetadata = record.getMetadata();
          if (!partitionPath.equals(record.getPartitionPath())) {
            HoodieUpsertException failureEx = new HoodieUpsertException("mismatched partition path, record partition: "
                + record.getPartitionPath() + " but trying to insert into partition: " + partitionPath);
            writeStatus.markFailure(record, failureEx, recordMetadata);
            continue;
          }
          // Writes the record
          try {
            writeToFile(recordKey, record, writeSchemaWithMetaFields,
                config.getPayloadConfig().getProps(), preserveMetadata);
            writeStatus.markSuccess(record, recordMetadata);
          } catch (Exception e) {
            LOG.error("Error writing record  " + record, e);
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

  /**
   * Writes a single record to the new file.
   *
   * @param key                          record key
   * @param record                       the record of {@link HoodieSparkRecord}
   * @param schema                       record schema
   * @param prop                         table properties
   * @param shouldPreserveRecordMetadata should preserve meta fields or not
   *
   * @throws IOException
   */
  protected void writeToFile(HoodieKey key, HoodieSparkRecord record, Schema schema, Properties prop, boolean shouldPreserveRecordMetadata)
      throws IOException {
    // NOTE: `FILENAME_METADATA_FIELD` has to be rewritten to correctly point to the
    //       file holding this record even in cases when overall metadata is preserved
    MetadataValues metadataValues = new MetadataValues().setFileName(newFilePath.getName());
    HoodieRecord populatedRecord = record.prependMetaFields(schema, writeSchemaWithMetaFields, metadataValues, prop);

    if (shouldPreserveRecordMetadata) {
      fileWriter.write(key.getRecordKey(), populatedRecord, writeSchemaWithMetaFields);
    } else {
      fileWriter.writeWithMetadata(key, populatedRecord, writeSchemaWithMetaFields);
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

      if (writeStatus.getStat().getRuntimeStats() != null) {
        writeStatus.getStat().getRuntimeStats().setTotalScanTime(readStats.getTotalLogReadTimeMs());
      }
      return Collections.singletonList(writeStatus);
    } catch (Exception e) {
      throw new HoodieUpsertException("Failed to close HoodieSparkFileGroupReaderBasedMergeHandle", e);
    }
  }
}
