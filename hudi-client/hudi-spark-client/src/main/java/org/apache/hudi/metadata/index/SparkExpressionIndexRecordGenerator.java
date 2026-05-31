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

package org.apache.hudi.metadata.index;

import org.apache.hudi.client.utils.SparkMetadataWriterUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieSchemaNotFoundException;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.model.FileInfoAndPartition;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getProjectedSchemaForExpressionIndex;

/**
 * Spark implementation of {@link ExpressionIndexRecordGenerator}.
 */
public class SparkExpressionIndexRecordGenerator implements ExpressionIndexRecordGenerator {
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataWriteConfig;

  public SparkExpressionIndexRecordGenerator(HoodieEngineContext engineContext,
                                             HoodieWriteConfig dataWriteConfig) {
    this.engineContext = engineContext;
    this.dataWriteConfig = dataWriteConfig;
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  public HoodieData<HoodieRecord> buildInitialization(
      List<FileInfoAndPartition> filesToIndex,
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient,
      int parallelism, HoodieSchema tableSchema,
      HoodieSchema readerSchema,
      StorageConfiguration<?> storageConf,
      String instantTime) {
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      throw new HoodieNotSupportedException("Hudi tables prior to version 8 do not support expression index.");
    }
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata = SparkMetadataWriterUtils.getExprIndexRecords(
        filesToIndex, indexDefinition, metaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
        Option.of(rangeMetadata ->
            HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(rangeMetadata, true, Option.of(indexDefinition.getIndexName()))));
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS)) {
      exprIndexRecords = exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get());
    }
    return exprIndexRecords;
  }

  @Override
  public HoodieData<HoodieRecord> buildUpdate(
      HoodieTableMetaClient dataMetaClient,
      HoodieTableMetadata tableMetadata,
      HoodieCommitMetadata commitMetadata,
      String indexPartition,
      String instantTime) {
    HoodieIndexDefinition indexDefinition = HoodieTableMetadataUtil.getHoodieIndexDefinition(indexPartition, dataMetaClient);
    boolean isExprIndexUsingColumnStats = indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS);
    Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>> partitionRecordsFunctionOpt = Option.empty();
    if (isExprIndexUsingColumnStats) {
      // Fetch column range metadata for affected partitions in the commit
      HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> exprIndexPartitionStatUpdates =
          SparkMetadataWriterUtils.getExpressionIndexPartitionStatsForExistingFiles(
                  commitMetadata, indexPartition, engineContext, tableMetadata, dataMetaClient, dataWriteConfig.getMetadataConfig(),
                  Option.of(dataWriteConfig.getRecordMerger().getRecordType()), instantTime, dataWriteConfig)
              .flatMapValues(List::iterator);
      // The function below merges the column range metadata from the updated data with latest column range metadata of affected partition computed above
      partitionRecordsFunctionOpt = Option.of(rangeMetadata ->
          HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(exprIndexPartitionStatUpdates.union(rangeMetadata), true, Option.of(indexDefinition.getIndexName())));
    }

    // Step 1: Generate partition name, file path and size triplets from the newly created files in the commit metadata
    List<FileInfoAndPartition> filesToIndex = new ArrayList<>();
    commitMetadata.getPartitionToWriteStats().forEach((dataPartition, writeStats) -> writeStats.forEach(writeStat -> filesToIndex.add(
        FileInfoAndPartition.of(writeStat.getPartitionPath(), new StoragePath(dataMetaClient.getBasePath(), writeStat.getPath()).toString(), writeStat.getFileSizeInBytes()))));
    int parallelism = Math.min(filesToIndex.size(), dataWriteConfig.getMetadataConfig().getExpressionIndexParallelism());
    HoodieSchema tableSchema = null;
    try {
      tableSchema = new TableSchemaResolver(dataMetaClient).getTableSchema();
    } catch (Exception e) {
      throw new HoodieSchemaNotFoundException("No schema found for table at " + dataMetaClient.getBasePath());
    }
    HoodieSchema readerSchema = getProjectedSchemaForExpressionIndex(indexDefinition, dataMetaClient, tableSchema);
    // Step 2: Compute the expression index column stat and partition stat records for these newly created files
    // partitionRecordsFunctionOpt - Function used to generate partition stats. These stats are generated only for expression index created using column stats
    //
    // In the partitionRecordsFunctionOpt function we merge the expression index records from the new files created in the commit metadata
    // with the expression index records from the unmodified files to get the new partition stat records
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata =
        SparkMetadataWriterUtils.getExprIndexRecords(filesToIndex, indexDefinition, dataMetaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
            partitionRecordsFunctionOpt);
    return expressionIndexComputationMetadata.getPartitionStatRecordsOpt().isPresent()
        ? expressionIndexComputationMetadata.getExpressionIndexRecords().union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get())
        : expressionIndexComputationMetadata.getExpressionIndexRecords();
  }
}
