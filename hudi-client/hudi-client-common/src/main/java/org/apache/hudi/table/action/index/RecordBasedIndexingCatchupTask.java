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

import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import java.io.IOException;
import java.util.ArrayList;
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
                                        HoodieEngineContext engineContext) {
    super(metadataWriter, instantsToIndex, metadataCompletedInstants, metaClient, metadataMetaClient, transactionManager, currentCaughtupInstant, engineContext);
  }

  @Override
  public void updateIndexForWriteAction(HoodieInstant instant) throws IOException {
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
        metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    HoodieData<HoodieRecord> records = readRecordKeysFromFileSlices(instant);
    metadataWriter.update(commitMetadata, records, instant.getTimestamp());
  }

  private HoodieData<HoodieRecord> readRecordKeysFromFileSlices(HoodieInstant instant) throws IOException {
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    HoodieTableMetadata metadata = HoodieTableMetadata.create(
        engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), false);
    HoodieMetadataFileSystemView fsView = new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline().filter(i -> i.equals(instant)), metadata);
    // Collect the list of latest file slices present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    fsView.loadAllPartitions();
    final List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    for (String partition : partitions) {
      fsView.getLatestFileSlices(partition).forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs)));
    }

    return HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        false,
        metadataConfig.getRecordIndexMaxParallelism(),
        this.getClass().getSimpleName(),
        metaClient,
        EngineType.SPARK);
  }
}
