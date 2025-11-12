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

package org.apache.hudi.client;

import org.apache.hudi.common.model.ActionType;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

/**
 * Utils to assist with updating columns to index with col stats.
 */
public class HoodieColumnStatsIndexUtils {

  /**
   * Updates the list of columns to index with col stats partition in MDT.
   * @param dataTable {@link HoodieTable} of interest.
   * @param config {@link HoodieWriteConfig} of interest.
   * @param commitMetadata commit metadata of interest.
   * @param commitActionType commit action type to include interested actions.
   * @param updateColStatsFunc function to assist with updating columns to index.
   */
  @VisibleForTesting
  public static void updateColsToIndex(HoodieTable dataTable,
                                       HoodieWriteConfig config,
                                       HoodieCommitMetadata commitMetadata,
                                       String commitActionType,
                                       Functions.Function2<HoodieTableMetaClient, List<String>, Void> updateColStatsFunc) {
    if (config.isMetadataTableEnabled()                            // this is a data table
        && config.getMetadataConfig().isColumnStatsIndexEnabled()  // the col_stats is enabled
        && ActionType.isCommitActionType(commitActionType)) {      // with interested actions
      dataTable.getMetaClient().reloadTableConfig();
      try {
        // update data table's table config for list of columns indexed.
        List<String> columnsToIndex = new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(commitMetadata, dataTable.getMetaClient(), config.getMetadataConfig(),
            Option.of(config.getRecordMerger().getRecordType())).keySet());
        // if col stats is getting updated, lets also update list of columns indexed if changed.
        updateColStatsFunc.apply(dataTable.getMetaClient(), columnsToIndex);
      } catch (Exception e) {
        throw new HoodieException("Updating data table config to latest set of columns indexed with col stats failed ", e);
      }
    }
  }

  /**
   * Deletes col stats index definition for the given table of interest.
   * @param dataTableMetaClient {@link HoodieTableMetaClient} instance for the data table.
   */
  @VisibleForTesting
  public static void deleteColumnStatsIndexDefinition(HoodieTableMetaClient dataTableMetaClient) {
    dataTableMetaClient.deleteIndexDefinition(PARTITION_NAME_COLUMN_STATS);
  }
}
