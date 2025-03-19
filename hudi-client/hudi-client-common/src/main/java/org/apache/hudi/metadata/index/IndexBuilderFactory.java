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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.bloomfilters.BloomFiltersIndexBuilder;
import org.apache.hudi.metadata.index.columnstats.ColumnStatsIndexBuilder;
import org.apache.hudi.metadata.index.expression.ExpressionIndexBuilder;
import org.apache.hudi.metadata.index.files.FilesIndexBuilder;
import org.apache.hudi.metadata.index.partitionstats.PartitionStatsIndexBuilder;
import org.apache.hudi.metadata.index.record.RecordIndexBuilder;
import org.apache.hudi.metadata.index.secondary.SecondaryIndexBuilder;
import org.apache.hudi.table.HoodieTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieMetadataConfig.ENABLE;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.metadata.MetadataPartitionType.getValidValues;

public class IndexBuilderFactory {
  public static IndexBuilder getIndexBuilder(MetadataPartitionType partitionType,
                                             HoodieEngineContext engineContext,
                                             HoodieWriteConfig dataTableWriteConfig,
                                             HoodieTableMetaClient dataTableMetaClient,
                                             HoodieTable table,
                                             EngineIndexHelper indexHelper) {
    switch (partitionType) {
      case BLOOM_FILTERS:
        return new BloomFiltersIndexBuilder(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case COLUMN_STATS:
        return new ColumnStatsIndexBuilder(
            engineContext, dataTableWriteConfig, dataTableMetaClient);
      case EXPRESSION_INDEX:
        return new ExpressionIndexBuilder(
            engineContext, dataTableWriteConfig, dataTableMetaClient, indexHelper);
      case FILES:
        return new FilesIndexBuilder(engineContext);
      case PARTITION_STATS:
        return new PartitionStatsIndexBuilder(
            engineContext, dataTableWriteConfig, dataTableMetaClient, indexHelper);
      case RECORD_INDEX:
        return new RecordIndexBuilder(
            engineContext, dataTableWriteConfig, dataTableMetaClient, table);
      case SECONDARY_INDEX:
        return new SecondaryIndexBuilder();
      default:
        throw new HoodieNotSupportedException(
            "Unsupported metadata partition type for index building: " + partitionType);
    }
  }

  /**
   * Returns the list of metadata partition types enabled based on the metadata config and table config.
   */
  // TODO(yihua): remove MetadataPartitionType#getEnabledIndexBuilderMap
  public static Map<MetadataPartitionType, IndexBuilder> getEnabledIndexBuilderMap(
      HoodieEngineContext engineContext,
      HoodieWriteConfig writeConfig,
      HoodieTableMetaClient metaClient,
      HoodieTable table,
      EngineIndexHelper indexHelper) {
    // TODO(yihua): use HoodieWriteConfig APIs directly
    TypedProperties props = writeConfig.getProps();
    if (!getBooleanWithAltKeys(props, ENABLE)) {
      return Collections.emptyMap();
    }
    return Arrays.stream(getValidValues())
        .filter(partitionType -> partitionType.isMetadataPartitionEnabled(props)
            || partitionType.isMetadataPartitionAvailable(metaClient))
        .collect(Collectors.toMap(
            Function.identity(), type -> IndexBuilderFactory.getIndexBuilder(
                type, engineContext, writeConfig, metaClient, table, indexHelper)));
  }
}
