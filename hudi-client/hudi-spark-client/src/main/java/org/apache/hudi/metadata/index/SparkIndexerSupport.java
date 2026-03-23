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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.model.FileInfoAndPartition;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/**
 * Spark implementation of {@link EngineIndexerSupport}.
 */
public class SparkIndexerSupport implements EngineIndexerSupport {
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataWriteConfig;

  public SparkIndexerSupport(HoodieEngineContext engineContext,
                             HoodieWriteConfig dataWriteConfig) {
    this.engineContext = engineContext;
    this.dataWriteConfig = dataWriteConfig;
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  public HoodieData<HoodieRecord> generateExpressionIndexRecords(
      List<FileInfoAndPartition> filesToIndex,
      HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient,
      int parallelism,
      HoodieSchema tableSchema,
      HoodieSchema readerSchema,
      StorageConfiguration<?> storageConf,
      String instantTime,
      Option<PartitionStatsRecordsFunction> partitionRecordsFunctionOpt) {
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      throw new HoodieNotSupportedException("Hudi tables prior to version 8 do not support expression index.");
    }
    // SparkMetadataWriterUtils accepts the generic Function type, while the public engine
    // support API uses a named capability type to avoid exposing the full generic signature.
    Option<Function<HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>>, HoodieData<HoodieRecord>>>
        partitionRecordsFunction = partitionRecordsFunctionOpt.map(function -> function);
    // Keep Spark-only computation metadata local to Spark support; common indexers only see
    // the final HoodieRecord stream for the metadata table.
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata = SparkMetadataWriterUtils.getExprIndexRecords(
        filesToIndex, indexDefinition, metaClient, parallelism, tableSchema, readerSchema, instantTime, engineContext, dataWriteConfig,
        partitionRecordsFunction);
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (PARTITION_NAME_COLUMN_STATS.equals(indexDefinition.getIndexType()) && expressionIndexComputationMetadata.getPartitionStatRecordsOpt().isPresent()) {
      exprIndexRecords = exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOpt().get());
    }
    return exprIndexRecords;
  }

  @Override
  public HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> loadExpressionIndexPartitionStats(
      HoodieTableMetaClient dataMetaClient,
      HoodieTableMetadata tableMetadata,
      HoodieCommitMetadata commitMetadata,
      HoodieIndexDefinition indexDefinition,
      String indexPartition,
      String instantTime) {
    return SparkMetadataWriterUtils.getExpressionIndexPartitionStatsForExistingFiles(
            commitMetadata, indexPartition, engineContext, tableMetadata, dataMetaClient, dataWriteConfig.getMetadataConfig(),
            Option.of(dataWriteConfig.getRecordMerger().getRecordType()), instantTime, dataWriteConfig)
        .flatMapValues(List::iterator);
  }
}
