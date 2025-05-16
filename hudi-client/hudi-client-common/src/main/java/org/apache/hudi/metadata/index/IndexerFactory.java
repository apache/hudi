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
import org.apache.hudi.util.Lazy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.MetadataPartitionType.getValidValues;

public class IndexerFactory {
  public static Indexer getIndexer(MetadataPartitionType partitionType,
                                   HoodieEngineContext engineContext,
                                   HoodieWriteConfig dataTableWriteConfig,
                                   HoodieTableMetaClient dataTableMetaClient,
                                   Lazy<HoodieTable> table,
                                   ExpressionIndexRecordGenerator expressionIndexRecordGenerator) {
    switch (partitionType) {
      case BLOOM_FILTERS:
        return new BloomFiltersIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case COLUMN_STATS:
        return new ColumnStatsIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case EXPRESSION_INDEX:
        return new ExpressionIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient, expressionIndexRecordGenerator);
      case FILES:
        return new FilesIndexer(engineContext);
      case PARTITION_STATS:
        return new PartitionStatsIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case RECORD_INDEX:
        return new RecordIndexer(
            engineContext, dataTableWriteConfig, dataTableMetaClient, table);
      case SECONDARY_INDEX:
        return new SecondaryIndexer(
            engineContext, expressionIndexRecordGenerator.getEngineType(), dataTableWriteConfig, dataTableMetaClient);
      default:
        throw new HoodieNotSupportedException(
            "Unsupported metadata partition type for indexing: " + partitionType);
    }
  }

  /**
   * @param engineContext                  {@link HoodieEngineContext} instance
   * @param dataTableWriteConfig           write config for the data table
   * @param dataTableMetaClient            meta client of the data table
   * @param table                          lazy {@link HoodieTable} instance presenting the data table
   * @param expressionIndexRecordGenerator record generator for the expression index
   * @return the map of metadata partition type to the indexer for the enabled metadata
   * partition types based on the metadata config and table config.
   */
  // TODO(yihua): remove MetadataPartitionType#getEnabledIndexBuilderMap
  public static Map<MetadataPartitionType, Indexer> getEnabledIndexerMap(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient,
      Lazy<HoodieTable> table,
      ExpressionIndexRecordGenerator expressionIndexRecordGenerator) {
    if (!dataTableWriteConfig.getMetadataConfig().isEnabled()) {
      return Collections.emptyMap();
    }
    return Collections.unmodifiableMap(Arrays.stream(
            getValidValues(dataTableMetaClient.getTableConfig().getTableVersion()))
        .filter(partitionType -> partitionType.isMetadataPartitionSupported(dataTableMetaClient)
            && (partitionType.isMetadataPartitionEnabled(dataTableWriteConfig.getMetadataConfig())
            || partitionType.isMetadataPartitionAvailable(dataTableMetaClient)))
        .collect(Collectors.toMap(
            Function.identity(), type -> IndexerFactory.getIndexer(
                type, engineContext, dataTableWriteConfig, dataTableMetaClient, table, expressionIndexRecordGenerator))));
  }
}
