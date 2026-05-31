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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.index.model.IndexPartitionAndRecords;
import org.apache.hudi.metadata.model.FileInfo;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of {@link Indexer} that handles common metadata-partition bootstrap flow,
 * including file-group initialization, commit, and partition state update.
 */
@Slf4j
public abstract class BaseIndexer implements Indexer {
  protected final HoodieEngineContext engineContext;
  protected final HoodieWriteConfig dataTableWriteConfig;
  protected final HoodieTableMetaClient dataTableMetaClient;

  protected BaseIndexer(
      HoodieEngineContext engineContext,
      HoodieWriteConfig dataTableWriteConfig,
      HoodieTableMetaClient dataTableMetaClient) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.dataTableMetaClient = dataTableMetaClient;
  }

  /**
   * Hook invoked after the bootstrap bulk commit for an index partition succeeds.
   * <p>
   * The default implementation marks the index partition as available in the data table config.
   * Subclasses can override this to perform index-specific follow-up work (for example, index-definition
   * registration or post-commit validation).
   *
   * @param metadataMetaClient metadata table meta client used during initialization
   * @param records records committed during index partition initialization
   * @param fileGroupCount number of file groups created for the index partition
   * @param relativePartitionPath metadata table relative partition path being initialized
   */
  @Override
  public void postInitialization(HoodieTableMetaClient metadataMetaClient, HoodieData<HoodieRecord> records, int fileGroupCount, String relativePartitionPath) {
    dataTableMetaClient.getTableConfig().setMetadataPartitionState(dataTableMetaClient, relativePartitionPath, true);
  }

  @Override
  public List<IndexPartitionAndRecords> buildRestore(String instantTime, List<String> deletedPartitions, Map<String, List<FileInfo>> filesAdded, Map<String, List<String>> filesDeleted) {
    return Collections.emptyList();
  }
}
