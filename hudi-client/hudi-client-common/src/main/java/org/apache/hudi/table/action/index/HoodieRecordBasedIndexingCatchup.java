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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.transaction.TransactionManager;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.RESTORE_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.ROLLBACK_ACTION;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.readRecordKeysFromBaseFiles;

/**
 * Indexing catchup task for record level indexing.
 */
public class HoodieRecordBasedIndexingCatchup extends BaseIndexingCatchupTask {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieRecordBasedIndexingCatchup.class);

  public HoodieRecordBasedIndexingCatchup(HoodieTableMetadataWriter metadataWriter,
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
  public void run() {
    for (HoodieInstant instant : instantsToIndex) {
      // metadata index already updated for this instant
      instant = getHoodieInstant(instant);
      if (instant == null) {
        continue;
      }
      // if instant completed, ensure that there was metadata commit, else update metadata for this completed instant
      if (COMPLETED.equals(instant.getState())) {
        String instantTime = instant.getTimestamp();
        Option<HoodieInstant> metadataInstant = metadataMetaClient.reloadActiveTimeline()
            .filterCompletedInstants().filter(i -> i.getTimestamp().equals(instantTime)).firstInstant();
        if (metadataInstant.isPresent()) {
          currentCaughtupInstant = instantTime;
          continue;
        }
        try {
          // we need take a lock here as inflight writer could also try to update the timeline
          transactionManager.beginTransaction(Option.of(instant), Option.empty());
          LOG.info("Updating metadata table for instant: " + instant);
          switch (instant.getAction()) {
            case HoodieTimeline.COMMIT_ACTION:
            case HoodieTimeline.DELTA_COMMIT_ACTION:
            case HoodieTimeline.REPLACE_COMMIT_ACTION:
              HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                  metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
              HoodieData<HoodieRecord> records = readRecordsFromDataFiles(instant);
              metadataWriter.update(commitMetadata, records, instant.getTimestamp());
              break;
            case CLEAN_ACTION:
              HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(metaClient, instant);
              metadataWriter.update(cleanMetadata, instant.getTimestamp());
              break;
            case RESTORE_ACTION:
              HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
                  metaClient.getActiveTimeline().getInstantDetails(instant).get());
              metadataWriter.update(restoreMetadata, instant.getTimestamp());
              break;
            case ROLLBACK_ACTION:
              HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
                  metaClient.getActiveTimeline().getInstantDetails(instant).get());
              metadataWriter.update(rollbackMetadata, instant.getTimestamp());
              break;
            default:
              throw new IllegalStateException("Unexpected value: " + instant.getAction());
          }
        } catch (IOException e) {
          throw new HoodieIndexException(String.format("Could not update metadata partition for instant: %s", instant), e);
        } finally {
          transactionManager.endTransaction(Option.of(instant));
        }
      }
    }
  }

  private HoodieData<HoodieRecord> readRecordsFromDataFiles(HoodieInstant instant) throws IOException {
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .build();
    HoodieTableMetadata metadata = HoodieTableMetadata.create(engineContext, metadataConfig, metaClient.getBasePathV2().toString(), false);
    HoodieMetadataFileSystemView fsView = new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline().filter(i -> i.equals(instant)), metadata);
    // Collect the list of latest base files present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    fsView.loadAllPartitions();
    final List<Pair<String, HoodieBaseFile>> partitionBaseFilePairs = new ArrayList<>();
    for (String partition : partitions) {
      partitionBaseFilePairs.addAll(fsView.getLatestBaseFiles(partition)
          .map(basefile -> Pair.of(partition, basefile)).collect(Collectors.toList()));
    }

    return readRecordKeysFromBaseFiles(
        engineContext,
        partitionBaseFilePairs,
        false,
        metadataConfig.getRecordIndexMaxParallelism(),
        metaClient.getBasePathV2().toString(),
        engineContext.getHadoopConf(),
        this.getClass().getSimpleName());
  }
}
