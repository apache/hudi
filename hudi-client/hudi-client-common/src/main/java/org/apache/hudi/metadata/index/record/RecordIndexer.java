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

package org.apache.hudi.metadata.index.record;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.index.model.DataPartitionAndRecords;
import org.apache.hudi.metadata.model.FileInfo;
import org.apache.hudi.metadata.model.FileSliceAndPartition;
import org.apache.hudi.metadata.index.model.IndexPartitionInitialization;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.record.HoodieRecordIndex;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.createRecordIndexDefinition;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;

/**
 * Implementation of the global {@link MetadataPartitionType#RECORD_INDEX} index
 */
@Slf4j
public class RecordIndexer extends BaseRecordIndexer {

  public RecordIndexer(HoodieEngineContext engineContext, HoodieWriteConfig dataTableWriteConfig,
                          HoodieTableMetaClient dataTableMetaClient) {
    super(engineContext, dataTableWriteConfig, dataTableMetaClient);
  }

  @Override
  public List<IndexPartitionInitialization> buildInitialization(String dataTableInstantTime, String instantTimeForPartition, Map<String, List<FileInfo>> partitionToAllFilesMap,
                                                                Lazy<List<FileSliceAndPartition>> lazyPartitionFileSlices) throws IOException {
    createRecordIndexDefinition(dataTableMetaClient, Collections.singletonMap(HoodieRecordIndex.IS_PARTITIONED_OPTION, "false"));
    DataPartitionAndRecords dataPartitionAndRecords = initializeRecordIndexPartition(
        lazyPartitionFileSlices.get(), dataTableWriteConfig.getMetadataConfig().getRecordIndexMaxParallelism());
    return Collections.singletonList(IndexPartitionInitialization.of(RECORD_INDEX.getPartitionPath(), dataPartitionAndRecords));
  }
}
