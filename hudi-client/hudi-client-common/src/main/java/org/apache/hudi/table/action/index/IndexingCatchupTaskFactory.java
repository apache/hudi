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

package org.apache.hudi.table.action.index;

import org.apache.hudi.avro.model.HoodieIndexPartitionInfo;
import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import java.util.List;
import java.util.Set;

public class IndexingCatchupTaskFactory {

  public static IndexingCatchupTask createCatchupTask(List<HoodieIndexPartitionInfo> indexPartitionInfos,
                                                      HoodieTableMetadataWriter metadataWriter,
                                                      List<HoodieInstant> instantsToIndex,
                                                      Set<String> metadataCompletedInstants,
                                                      HoodieTable table,
                                                      HoodieTableMetaClient metadataMetaClient,
                                                      String currentCaughtupInstant,
                                                      TransactionManager transactionManager,
                                                      HoodieEngineContext engineContext,
                                                      HoodieHeartbeatClient heartbeatClient) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    boolean hasRecordLevelIndexing = indexPartitionInfos.stream()
        .anyMatch(partitionInfo -> partitionInfo.getMetadataPartitionPath().equals(MetadataPartitionType.RECORD_INDEX.getPartitionPath()));
    if (hasRecordLevelIndexing) {
      return new RecordBasedIndexingCatchupTask(
          metadataWriter,
          instantsToIndex,
          metadataCompletedInstants,
          metaClient,
          metadataMetaClient,
          currentCaughtupInstant,
          transactionManager,
          engineContext,
          table,
          heartbeatClient);
    } else {
      return new WriteStatBasedIndexingCatchupTask(
          metadataWriter,
          instantsToIndex,
          metadataCompletedInstants,
          metaClient,
          metadataMetaClient,
          currentCaughtupInstant,
          transactionManager,
          engineContext,
          table,
          heartbeatClient);
    }
  }
}
