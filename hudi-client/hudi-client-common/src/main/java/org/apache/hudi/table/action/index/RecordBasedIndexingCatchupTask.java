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

import org.apache.hudi.client.heartbeat.HoodieHeartbeatClient;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Indexing catchup task for record level indexing.
 */
public class RecordBasedIndexingCatchupTask extends AbstractIndexingCatchupTask {

  public RecordBasedIndexingCatchupTask(HoodieTableMetadataWriter metadataWriter,
                                        List<HoodieInstant> instantsToIndex,
                                        Set<String> metadataCompletedInstants,
                                        HoodieTableMetaClient metaClient,
                                        HoodieTableMetaClient metadataMetaClient,
                                        String currentCaughtupInstant,
                                        TransactionManager transactionManager,
                                        HoodieEngineContext engineContext,
                                        HoodieTable table,
                                        HoodieHeartbeatClient heartbeatClient) {
    super(metadataWriter, instantsToIndex, metadataCompletedInstants, metaClient, metadataMetaClient, transactionManager, currentCaughtupInstant, engineContext, table, heartbeatClient);
  }

  @Override
  public void updateIndexForWriteAction(HoodieInstant instant) throws IOException {
    HoodieCommitMetadata commitMetadata =
        metaClient.getActiveTimeline().readCommitMetadata(instant);
    metadataWriter.update(commitMetadata, instant.requestedTime());
  }
}
