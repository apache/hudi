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
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

public class SparkExpressionIndexRecordGenerator implements ExpressionIndexRecordGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExpressionIndexRecordGenerator.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieWriteConfig metadataTableWriteConfig;

  public SparkExpressionIndexRecordGenerator(HoodieEngineContext engineContext,
                                             HoodieWriteConfig dataTableWriteConfig,
                                             HoodieWriteConfig metadataTableWriteConfig) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.metadataTableWriteConfig = metadataTableWriteConfig;
  }

  @Override
  public EngineType getEngineType() {
    return EngineType.SPARK;
  }

  @Override
  public HoodieData<HoodieRecord> generate(
      List<FileToIndex> filesToIndex, HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient, int parallelism, Schema readerSchema, StorageConfiguration<?> storageConf,
      String instantTime) {
    if (metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
      throw new HoodieNotSupportedException("Hudi tables prior to version 8 do not support expression index.");
    }

    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata =
        SparkMetadataWriterUtils.getExprIndexRecords(filesToIndex, indexDefinition,
            metaClient, parallelism, readerSchema, instantTime, engineContext, dataTableWriteConfig,
            metadataTableWriteConfig,
            Option.of(rangeMetadata ->
                collectAndProcessExprIndexPartitionStatRecords(
                    rangeMetadata, true, Option.of(indexDefinition.getIndexName()))));
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS)) {
      exprIndexRecords =
          exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOption().get());
    }
    return exprIndexRecords;
  }

  public static HoodieData<HoodieRecord> collectAndProcessExprIndexPartitionStatRecords(HoodiePairData<String, HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata,
                                                                                        boolean isTightBound, Option<String> indexPartitionOpt) {
    // Step 1: Group by partition name
    HoodiePairData<String, Iterable<HoodieColumnRangeMetadata<Comparable>>> columnMetadataMap = fileColumnMetadata.groupByKey();
    // Step 2: Aggregate Column Ranges
    return columnMetadataMap.map(entry -> {
      String partitionName = entry.getKey();
      Iterable<HoodieColumnRangeMetadata<Comparable>> iterable = entry.getValue();
      final HoodieColumnRangeMetadata<Comparable>[] finalMetadata = new HoodieColumnRangeMetadata[] {null};
      iterable.forEach(e -> {
        HoodieColumnRangeMetadata<Comparable> rangeMetadata = HoodieColumnRangeMetadata.create(
            partitionName, e.getColumnName(), e.getMinValue(), e.getMaxValue(),
            e.getNullCount(), e.getValueCount(), e.getTotalSize(), e.getTotalUncompressedSize());
        finalMetadata[0] = HoodieColumnRangeMetadata.merge(finalMetadata[0], rangeMetadata);
      });
      return HoodieMetadataPayload.createPartitionStatsRecords(partitionName, Collections.singletonList(finalMetadata[0]), false, isTightBound, indexPartitionOpt)
          .collect(Collectors.toList());
    }).flatMap(List::iterator);
  }
}
