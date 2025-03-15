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

package org.apache.hudi.metadata.index.columnstats;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.index.IndexBuilder;
import org.apache.hudi.util.Lazy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ColumnStatsIndexBuilder implements IndexBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnStatsIndexBuilder.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieTableMetaClient dataTableMetaClient;
  private final Lazy<List<String>> columnsToIndex;

  public ColumnStatsIndexBuilder(HoodieEngineContext engineContext,
                                 HoodieWriteConfig dataTableWriteConfig,
                                 HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
    this.columnsToIndex = Lazy.lazily(() ->
        new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(dataTableMetaClient.getTableConfig(),
            dataTableWriteConfig.getMetadataConfig(),
            Lazy.lazily(() -> HoodieTableMetadataUtil.tryResolveSchemaForTable(dataTableMetaClient)),
            true,
            Option.of(dataTableWriteConfig.getRecordMerger().getRecordType())).keySet()));
  }

  @Override
  public Pair<Integer, HoodieData<HoodieRecord>> createRecordsFromExistingFiles(
      List<HoodieTableMetadataUtil.DirectoryInfo> partitionInfoList,
      Map<String, Map<String, Long>> partitionToFilesMap,
      String createInstantTime,
      HoodieTableFileSystemView fsView,
      HoodieBackedTableMetadata metadata,
      String instantTimeForPartition) throws IOException {

    final int fileGroupCount = dataTableWriteConfig.getMetadataConfig().getColumnStatsIndexFileGroupCount();
    if (columnsToIndex.get().isEmpty()) {
      // this can only happen if meta fields are disabled and cols to index is not explicitly overridden.
      return Pair.of(fileGroupCount, engineContext.emptyHoodieData());
    }

    LOG.info("Indexing {} columns for column stats index", columnsToIndex.get().size());

    // during initialization, we need stats for base and log files.
    HoodieData<HoodieRecord> records = HoodieTableMetadataUtil.convertFilesToColumnStatsRecords(
        engineContext, Collections.emptyMap(), partitionToFilesMap, dataTableMetaClient,
        dataTableWriteConfig.getMetadataConfig(),
        dataTableWriteConfig.getColumnStatsIndexParallelism(),
        dataTableWriteConfig.getMetadataConfig().getMaxReaderBufferSize(),
        columnsToIndex.get());

    return Pair.of(fileGroupCount, records);
  }
}
