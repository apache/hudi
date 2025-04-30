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

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexer;
import org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexer;
import org.apache.hudi.metadata.index.expression.ExpressionIndexer;
import org.apache.hudi.metadata.index.files.FilesIndexer;
import org.apache.hudi.metadata.index.partitionstats.PartitionStatsIndexer;
import org.apache.hudi.metadata.index.record.RecordIndexer;
import org.apache.hudi.metadata.index.secondary.SecondaryIndexer;
import org.apache.hudi.table.HoodieTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.MetadataPartitionType.getValidValues;

public class IndexerFactory {
  public static Indexer getIndexBuilder(MetadataPartitionType partitionType,
                                        EngineType engineType,
                                        HoodieEngineContext engineContext,
                                        HoodieWriteConfig dataTableWriteConfig,
                                        HoodieTableMetaClient dataTableMetaClient,
                                        HoodieTable table,
                                        ExpressionIndexRecordGenerator indexHelper) {
    switch (partitionType) {
      case BLOOM_FILTERS:
        return new BloomFiltersIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case COLUMN_STATS:
        return new ColumnStatsIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case EXPRESSION_INDEX:
        return new ExpressionIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient, indexHelper);
      case FILES:
        return new FilesIndexer(engineContext);
      case PARTITION_STATS:
        return new PartitionStatsIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient, indexHelper);
      case RECORD_INDEX:
        return new RecordIndexer(
            engineType, engineContext, dataTableWriteConfig, dataTableMetaClient, table);
      case SECONDARY_INDEX:
        return new SecondaryIndexer(
            engineType, engineContext, dataTableWriteConfig, dataTableMetaClient, indexHelper);
      default:
        throw new HoodieNotSupportedException(
            "Unsupported metadata partition type for indexing: " + partitionType);
    }
  }

  /**
   * Returns the list of metadata partition types enabled based on the metadata config and table config.
   */
  // TODO(yihua): remove MetadataPartitionType#getEnabledIndexBuilderMap
  public static Map<MetadataPartitionType, Indexer> getEnabledIndexBuilderMap(
      EngineType engineType,
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient metaClient,
      HoodieTable table,
      ExpressionIndexRecordGenerator indexHelper) {
    if (!dataTableWriteConfig.getMetadataConfig().isEnabled()) {
      return Collections.emptyMap();
    }
    return Arrays.stream(getValidValues())
        .filter(partitionType -> partitionType.isMetadataPartitionSupported(metaClient)
            && (partitionType.isMetadataPartitionEnabled(dataTableWriteConfig.getMetadataConfig())
            || (partitionType.shouldUpdateIfAvailable(dataTableWriteConfig.getMetadataConfig())
            && partitionType.isMetadataPartitionAvailable(metaClient))))
        .collect(Collectors.toMap(
            Function.identity(), type -> IndexerFactory.getIndexBuilder(
                type, engineType, engineContext, dataTableWriteConfig, metaClient, table, indexHelper)));
  }
}
