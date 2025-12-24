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

package org.apache.hudi.io.storage.row;

import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.client.model.HoodieRowData;
import org.apache.hudi.client.model.HoodieRowDataCreation;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.model.MultiplePartialUpdateUnit;
import org.apache.hudi.common.storage.HoodieStorageStrategy;
import org.apache.hudi.common.storage.HoodieStorageStrategyFactory;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketStrategist;
import org.apache.hudi.index.bucket.BucketStrategistFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copy From HoodieRowDataCreateHandle
 * 区别在于独立控制了文件名称 getLSMFileName()
 * bucketNumber_UUID_columnFamilyNumber_levelNumber_partitionId-stageId-attemptId.parquet
 *
 */
public class LSMHoodieRowDataCreateHandle implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(LSMHoodieRowDataCreateHandle.class);
  private static final AtomicLong SEQGEN = new AtomicLong(1);

  private final String instantTime;
  private final int taskPartitionId;
  private final long taskId;
  private final long taskEpochId;
  private final HoodieTable table;
  private final HoodieWriteConfig writeConfig;
  protected final HoodieRowDataFileWriter fileWriter;
  private final String partitionPath;
  private final Path path;
  private final String fileId;
  private final boolean preserveHoodieMetadata;
  private final FileSystem fs;
  protected final HoodieInternalWriteStatus writeStatus;
  private final HoodieTimer currTimer;
  private final Map<Integer, LogicalType> indexToFieldType;
  private final RowType rowType;
  private final int version;
  private final int levelNumber;
  private final HoodieTableConfig tableConfig;
  private final HoodieRecordMerger recordMerger;
  private final TypedProperties payloadProperties;
  private final Schema writeSchema;
  private final boolean isUpdateEventTime;
  private final HoodieStorageStrategy hoodieStorageStrategy;
  private String lastRecordKey = null;
  private HoodieFlinkRecord lastFlinkRecord = null;

  public LSMHoodieRowDataCreateHandle(HoodieTable table, HoodieWriteConfig writeConfig, String partitionPath, String fileId,
                                      String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                      RowType rowType, boolean preserveHoodieMetadata, int levelNumber, int version) {
    if (levelNumber == 1 && writeConfig.getParquetBlockSize() == 32 * 1024 * 1024) {
      writeConfig.setValue(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), HoodieStorageConfig.PARQUET_BLOCK_SIZE.defaultValue());
    }
    this.version = version;
    this.levelNumber = levelNumber;
    this.hoodieStorageStrategy = HoodieStorageStrategyFactory.getInstant(table.getMetaClient());
    Option<BucketStrategist> bucketStrategist;
    if (writeConfig.getIndexType().equals(HoodieIndex.IndexType.BUCKET) && writeConfig.isBucketIndexAtPartitionLevel()) {
      bucketStrategist = Option.of(BucketStrategistFactory.getInstant(writeConfig, table.getMetaClient().getFs()));
    } else {
      bucketStrategist = Option.empty();
    }
    this.partitionPath = partitionPath;
    this.table = table;
    this.tableConfig = table.getMetaClient().getTableConfig();
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.taskPartitionId = taskPartitionId;
    this.taskId = taskId;
    this.taskEpochId = taskEpochId;
    this.fileId = fileId;
    this.preserveHoodieMetadata = preserveHoodieMetadata;
    this.currTimer = HoodieTimer.start();
    this.fs = table.getMetaClient().getFs();
    String fileName = getLSMFileName();
    this.path = makeNewPath(partitionPath, fileName);
    this.rowType = preserveHoodieMetadata ? rowType : addMetadataFields(rowType, writeConfig.allowOperationMetadataField());
    this.writeSchema = preserveHoodieMetadata ? AvroSchemaCache.intern(AvroSchemaConverter.convertToSchema(rowType)) :
        AvroSchemaCache.intern(AvroSchemaConverter.convertToSchema(addRecordKeyAndSeqIdMetaFields(rowType)));
    this.indexToFieldType = new HashMap<>();
    parseEventTimeField(this.rowType, writeConfig);
    this.isUpdateEventTime = supportUpdateEventTime();
    this.writeStatus = new HoodieInternalWriteStatus(!table.getIndex().isImplicitWithStorage(),
        writeConfig.getWriteStatusFailureFraction());
    writeStatus.setPartitionPath(partitionPath);
    writeStatus.setFileId(fileId);
    writeStatus.setStat(new HoodieWriteStat());
    try {
      HoodiePartitionMetadata partitionMetadata =
          new HoodiePartitionMetadata(
              fs,
              instantTime,
              new Path(writeConfig.getBasePath()),
              hoodieStorageStrategy.storageLocation(partitionPath, instantTime),
              table.getPartitionMetafileFormat(),
              hoodieStorageStrategy,
              bucketStrategist.map(strategist -> strategist.computeBucketNumber(partitionPath)));
      partitionMetadata.trySave(taskPartitionId);
      createMarkerFile(partitionPath, fileName);
      this.fileWriter = createNewFileWriter(path, table, writeConfig, this.rowType);
      this.recordMerger = writeConfig.getRecordMerger();
      this.payloadProperties = writeConfig.getPayloadConfig().getProps();
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
    }
    LOG.info("New handle created for partition :" + partitionPath + " with fileId " + fileId);
  }

  /**
   * Writes an {@link RowData} to the underlying {@link HoodieRowDataFileWriter}.
   * Before writing, value for meta columns are computed as required
   * and wrapped in {@link HoodieRowData}. {@link HoodieRowData} is what gets written to HoodieRowDataFileWriter.
   *
   * @param recordKey     The record key
   * @param partitionPath The partition path
   * @param record        instance of {@link RowData} that needs to be written to the fileWriter.
   * @throws IOException
   */
  public void write(String recordKey, String partitionPath, RowData record, boolean needDedupe) throws IOException {
    if (needDedupe) {
      RowData currentRow = copyRow(record);
      HoodieFlinkRecord currentFlinkRecord = new HoodieFlinkRecord(currentRow);
      if (lastRecordKey != null && lastRecordKey.equals(recordKey)) {
        // 相同key，合并记录
        Option<Pair<HoodieRecord, Schema>> res = recordMerger.merge(lastFlinkRecord, writeSchema,
            currentFlinkRecord, writeSchema, payloadProperties);
        if (res.isPresent()) {
          lastFlinkRecord = (HoodieFlinkRecord) res.get().getLeft();
        } else {
          lastFlinkRecord = null;
        }
      } else {
        // 不同key，先写出上一条合并后的记录(如果存在)
        if (lastFlinkRecord != null) {
          writeRow(lastRecordKey, partitionPath, lastFlinkRecord.getData());
        }
        // 更新为当前记录
        lastRecordKey = recordKey;
        lastFlinkRecord = currentFlinkRecord;
      }
    } else {
      writeRow(recordKey, partitionPath, record);
    }
  }

  private void writeRow(String recordKey, String partitionPath, RowData record) {
    try {
      RowData rowData;
      String commitInstant = preserveHoodieMetadata
          ? record.getString(HoodieRecord.COMMIT_TIME_METADATA_FIELD_ORD).toString()
          : instantTime;
      if (preserveHoodieMetadata) {
        String seqId = record.getString(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD_ORD).toString();
        rowData = HoodieRowDataCreation.create(commitInstant, seqId, recordKey, partitionPath, path.getName(),
            record, writeConfig.allowOperationMetadataField(), preserveHoodieMetadata);
      } else {
        rowData = HoodieRowDataCreation.createLSMHoodieRowData(commitInstant, partitionPath,
            path.getName(), record, writeConfig.allowOperationMetadataField());
      }
      try {
        fileWriter.writeRow(recordKey, rowData);
        if (isUpdateEventTime) {
          // set min/max
          writeStatus.markSuccess(recordKey, String.valueOf(getValueFromRowData(rowData)));
        } else {
          writeStatus.markSuccess(recordKey);
        }
      } catch (Throwable t) {
        LOG.error("Failed to write : key is " + recordKey + ", data is " + rowData, t);
        writeStatus.markFailure(recordKey, t);
        if (!writeConfig.getIgnoreWriteFailed()) {
          throw new HoodieException(t.getMessage(), t);
        }
      }
    } catch (Throwable ge) {
      writeStatus.setGlobalError(ge);
      throw ge;
    }
  }

  private RowData copyRow(RowData rowData) {
    // no need to copy here
    //    return rowData.copy();
    return rowData;
  }

  private void flush() {
    if (lastFlinkRecord != null) {
      writeRow(lastRecordKey, partitionPath, lastFlinkRecord.getData());
      lastFlinkRecord = null;
      lastRecordKey = null;
    }
  }

  /**
   * Parse the eventtime Field and store the mapping between field index and type.
   */
  private void parseEventTimeField(RowType rowType, HoodieWriteConfig writeConfig) {
    String eventTimeField = writeConfig.getPayloadConfig().getString(HoodiePayloadConfig.EVENT_TIME_FIELD.key());
    if (StringUtils.isNullOrEmpty(eventTimeField)) {
      return;
    }

    if (eventTimeField.contains("|") || eventTimeField.contains(":")) {
      MultiplePartialUpdateUnit multiplePartialUpdateUnit = new MultiplePartialUpdateUnit(eventTimeField);
      multiplePartialUpdateUnit.getAllOrderingFields().forEach(field ->
          storeFieldIndexAndType(rowType, field)
      );
    } else {
      storeFieldIndexAndType(rowType, eventTimeField);
    }
  }

  private void storeFieldIndexAndType(RowType rowType, String fieldName) {
    int fieldIndex = rowType.getFieldIndex(fieldName);
    if (fieldIndex == -1) {
      return;
    }
    LogicalType fieldType = rowType.getTypeAt(fieldIndex);
    this.indexToFieldType.put(fieldIndex, fieldType);
  }

  private Object getValueFromRowData(RowData rowData) {
    String minEventTimeValue = null;
    for (Map.Entry<Integer, LogicalType> entry : this.indexToFieldType.entrySet()) {
      int fieldIndex = entry.getKey();
      LogicalType fieldType = entry.getValue();

      if (rowData.isNullAt(fieldIndex)) {
        continue;
      }

      LogicalTypeRoot typeRoot = fieldType.getTypeRoot();
      String currentValue = null;
      if (typeRoot == LogicalTypeRoot.BIGINT) {
        currentValue = String.valueOf(rowData.getLong(fieldIndex));
      } else if (typeRoot == LogicalTypeRoot.VARCHAR) {
        currentValue = String.valueOf(rowData.getString(fieldIndex));
      } else {
        continue;
      }

      if (minEventTimeValue == null || currentValue.compareTo(minEventTimeValue) < 0) {
        minEventTimeValue = currentValue;
      }
    }
    return minEventTimeValue;
  }

  /**
   * Eventtime only supports bigint and string types.
   */
  private boolean supportUpdateEventTime() {
    for (Map.Entry<Integer, LogicalType> entry : this.indexToFieldType.entrySet()) {
      int fieldIndex = entry.getKey();
      LogicalTypeRoot typeRoot = entry.getValue().getTypeRoot();
      if (fieldIndex == -1) {
        return false;
      }
      if (typeRoot != LogicalTypeRoot.BIGINT && typeRoot != LogicalTypeRoot.VARCHAR) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns {@code true} if this handle can take in more writes. else {@code false}.
   */
  public boolean canWrite() {
    return fileWriter.canWrite();
  }

  /**
   * Closes the {@link LSMHoodieRowDataCreateHandle} and returns an instance of {@link HoodieInternalWriteStatus} containing the stats and
   * status of the writes to this handle.
   *
   * @return the {@link HoodieInternalWriteStatus} containing the stats and status of the writes to this handle.
   * @throws IOException
   */
  public HoodieInternalWriteStatus close() throws IOException {
    flush();
    fileWriter.close();
    HoodieWriteStat stat = writeStatus.getStat();
    stat.setPartitionPath(partitionPath);
    stat.setNumWrites(writeStatus.getTotalRecords());
    stat.setNumDeletes(0);
    stat.setNumInserts(writeStatus.getTotalRecords());
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(fileId);
    stat.setPath(hoodieStorageStrategy.getRelativePath(path));
    long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), path);
    stat.setTotalWriteBytes(fileSizeInBytes);
    stat.setFileSizeInBytes(fileSizeInBytes);
    stat.setTotalWriteErrors(writeStatus.getFailedRowsSize());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(currTimer.endTimer());
    stat.setRuntimeStats(runtimeStats);
    return writeStatus;
  }

  public String getFileName() {
    return path.getName();
  }

  private Path makeNewPath(String partitionPath, String fileName) {
    Path path = hoodieStorageStrategy.storageLocation(partitionPath, instantTime);
    try {
      if (!fs.exists(path)) {
        fs.mkdirs(path); // create a new partition as needed.
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    return new Path(path.toString(), fileName);
  }

  /**
   * Creates an empty marker file corresponding to storage writer path.
   *
   * @param partitionPath Partition path
   */
  private void createMarkerFile(String partitionPath, String dataFileName) {
    WriteMarkers writeMarkers = WriteMarkersFactory.get(writeConfig.getMarkersType(), table, instantTime);
    writeMarkers.create(partitionPath, dataFileName, IOType.CREATE);
  }

  private String getWriteToken() {
    return taskPartitionId + "-" + taskId + "-" + taskEpochId;
  }

  protected HoodieRowDataFileWriter createNewFileWriter(
      Path path, HoodieTable hoodieTable, HoodieWriteConfig config, RowType rowType)
      throws IOException {
    return HoodieRowDataFileWriterFactory.getRowDataFileWriter(
        path, hoodieTable, config, rowType);
  }

  private String getLSMFileName() {
    return FSUtils.makeLSMFileName(fileId, instantTime, String.valueOf(version), String.valueOf(levelNumber),
        getWriteToken(), tableConfig.getBaseFileFormat().getFileExtension());
  }

  public static RowType addMetadataFields(RowType rowType, boolean withOperationField) {
    List<RowType.RowField> mergedFields = new ArrayList<>();

    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();
    RowType.RowField commitTimeField =
        new RowType.RowField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, metadataFieldType, "commit time");
    RowType.RowField commitSeqnoField =
        new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType, "commit seqno");
    RowType.RowField recordKeyField =
        new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType, "record key");
    RowType.RowField partitionPathField =
        new RowType.RowField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, metadataFieldType, "partition path");
    RowType.RowField fileNameField =
        new RowType.RowField(HoodieRecord.FILENAME_METADATA_FIELD, metadataFieldType, "field name");

    mergedFields.add(commitTimeField);
    mergedFields.add(commitSeqnoField);
    mergedFields.add(recordKeyField);
    mergedFields.add(partitionPathField);
    mergedFields.add(fileNameField);

    if (withOperationField) {
      RowType.RowField operationField =
          new RowType.RowField(HoodieRecord.OPERATION_METADATA_FIELD, metadataFieldType, "operation");
      mergedFields.add(operationField);
    }

    mergedFields.addAll(rowType.getFields());

    return new RowType(false, mergedFields);
  }

  public static RowType addRecordKeyAndSeqIdMetaFields(RowType rowType) {
    List<RowType.RowField> mergedFields = new ArrayList<>();

    LogicalType metadataFieldType = DataTypes.STRING().getLogicalType();
    RowType.RowField recordKeyField =
        new RowType.RowField(HoodieRecord.RECORD_KEY_METADATA_FIELD, metadataFieldType, "record key");
    RowType.RowField commitSeqnoField =
        new RowType.RowField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, metadataFieldType, "commit seqno");

    mergedFields.add(recordKeyField);
    mergedFields.add(commitSeqnoField);
    mergedFields.addAll(rowType.getFields());

    return new RowType(false, mergedFields);
  }
}
