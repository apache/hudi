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

package org.apache.hudi.io.v2;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.AppendResult;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.FlinkAppendHandle;
import org.apache.hudi.io.MiniBatchHandle;
import org.apache.hudi.io.log.block.HoodieFlinkAvroDataBlock;
import org.apache.hudi.io.log.block.HoodieFlinkParquetDataBlock;
import org.apache.hudi.io.storage.ColumnRangeMetadataProvider;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_COMPRESSION_RATIO_FRACTION;
import static org.apache.hudi.common.config.HoodieStorageConfig.PARQUET_DICTIONARY_ENABLED;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/**
 * A write handle that supports creating a log file and writing records based on record Iterator.
 * The differences from {@code FlinkAppendHandle} are:
 *
 * <p> 1. {@code RowDataLogHandle} does not convert RowData into Avro record before writing.
 * <p> 2. {@code RowDataLogHandle} writes Parquet data block by default.
 *
 * <p>The back-up writer may roll over to a new log file if there already exists a log file for the
 * given file group and instant.
 */
@Slf4j
public class RowDataLogWriteHandle<T, I, K, O>
    extends FlinkAppendHandle<T, I, K, O> implements MiniBatchHandle {

  public RowDataLogWriteHandle(
      HoodieWriteConfig config,
      String instantTime,
      HoodieTable<T, I, K, O> hoodieTable,
      Iterator<HoodieRecord<T>> recordItr,
      String fileId,
      String partitionPath,
      BucketType bucketType,
      TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, bucketType, recordItr, taskContextSupplier);
  }

  @Override
  protected SizeEstimator<HoodieRecord> getSizeEstimator() {
    return new FlinkRecordSizeEstimator();
  }

  /**
   * Flink writer does not support record-position for update/delete currently, will be supported later, see HUDI-9192.
   */
  @Override
  protected Option<String> getBaseFileInstantTimeOfPositions() {
    return Option.empty();
  }

  @Override
  protected void processAppendResult(AppendResult result, Option<HoodieLogBlock> dataBlock) {
    if (getLogBlockType() == HoodieLogBlockType.AVRO_DATA_BLOCK) {
      super.processAppendResult(result, dataBlock);
      return;
    }
    HoodieDeltaWriteStat stat = (HoodieDeltaWriteStat) this.writeStatus.getStat();
    updateWriteStatus(result, stat);

    // for parquet data block, we can get column stats from parquet footer directly.
    if (config.isMetadataColumnStatsIndexEnabled()) {
      HoodieIndexVersion indexVersion = HoodieTableMetadataUtil.existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, hoodieTable.getMetaClient());
      Set<String> columnsToIndexSet = new HashSet<>(HoodieTableMetadataUtil
          .getColumnsToIndex(hoodieTable.getMetaClient().getTableConfig(),
              config.getMetadataConfig(), Lazy.eagerly(Option.of(writeSchemaWithMetaFields)),
              Option.of(HoodieRecord.HoodieRecordType.FLINK), indexVersion).keySet());

      Map<String, HoodieColumnRangeMetadata<Comparable>> columnRangeMetadata;
      if (dataBlock.isEmpty()) {
        // only delete block exists
        columnRangeMetadata = new HashMap<>();
        columnsToIndexSet.forEach(col -> columnRangeMetadata.put(col, HoodieColumnRangeMetadata.createEmpty(stat.getPath(), col, indexVersion)));
      } else {
        ValidationUtils.checkArgument(dataBlock.get() instanceof ColumnRangeMetadataProvider,
            "Log block for Flink ingestion should always be an instance of ColumnRangeMetadataProvider for collecting column stats efficiently.");
        columnRangeMetadata =
            ((ColumnRangeMetadataProvider) dataBlock.get()).getColumnRangeMeta(stat.getPath(), indexVersion).entrySet().stream()
                .filter(e -> columnsToIndexSet.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
      stat.putRecordsStats(columnRangeMetadata);
    }
    resetWriteCounts();
    assert stat.getRuntimeStats() != null;
    log.info("WriteHandle for partitionPath {} filePath {}, took {} ms.",
        partitionPath, stat.getPath(), stat.getRuntimeStats().getTotalUpsertTime());
    timer.startTimer();
  }

  /**
   * Build a data block based on the hoodie record iterator, and block header.
   *
   * @param writeConfig hoodie write config
   * @param logDataBlockFormat type of the data block
   * @param records hoodie record list used to build the data block
   * @param keyField name of key field
   * @param header header for the data block
   * @return data block
   */
  @Override
  protected HoodieLogBlock getDataBlock(
      HoodieWriteConfig writeConfig,
      HoodieLogBlockType logDataBlockFormat,
      List<HoodieRecord> records,
      Map<HeaderMetadataType, String> header,
      String keyField) {
    switch (logDataBlockFormat) {
      case PARQUET_DATA_BLOCK:
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put(PARQUET_COMPRESSION_CODEC_NAME.key(), writeConfig.getParquetCompressionCodec());
        paramsMap.put(PARQUET_COMPRESSION_RATIO_FRACTION.key(), String.valueOf(writeConfig.getParquetCompressionRatio()));
        paramsMap.put(PARQUET_DICTIONARY_ENABLED.key(), String.valueOf(writeConfig.parquetDictionaryEnabled()));
        return new HoodieFlinkParquetDataBlock(
            records,
            header,
            keyField,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetCompressionRatio(),
            writeConfig.parquetDictionaryEnabled());
      case AVRO_DATA_BLOCK:
        return new HoodieFlinkAvroDataBlock(records, header, keyField);
      default:
        throw new HoodieException("Data block format " + logDataBlockFormat + " is not implemented for Flink RowData append handle.");
    }
  }
}
