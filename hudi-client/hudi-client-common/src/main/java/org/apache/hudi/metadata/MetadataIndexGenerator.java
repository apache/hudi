/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.metadata;

import org.apache.hudi.client.SecondaryIndexStats;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.metadata.MetadataPartitionType.SECONDARY_INDEX;

/**
 * For now this is a placeholder to generate all MDT records in one place.
 * Once <a href="https://github.com/apache/hudi/pull/13226">Refactor MDT update logic with Indexer</a> is landed,
 * we will leverage the new abstraction to generate MDT records.
 */
public class MetadataIndexGenerator implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataIndexGenerator.class);

  /**
   * MDT record transformation utility. This function is expected to be invoked from a map Partition call, where one spark task will receive
   * one WriteStatus as input and the output contains prepared MDT table records for all eligible partitions that can operate on one
   * WriteStatus instance only.
   */
  static class WriteStatusBasedMetadataIndexMapper implements SerializableFunction<WriteStatus, Iterator<HoodieRecord>> {
    private final List<MetadataPartitionType> enabledPartitionTypes;
    private final HoodieWriteConfig dataWriteConfig;

    public WriteStatusBasedMetadataIndexMapper(List<MetadataPartitionType> enabledPartitionTypes, HoodieWriteConfig dataWriteConfig) {
      this.enabledPartitionTypes = enabledPartitionTypes;
      this.dataWriteConfig = dataWriteConfig;
    }

    @Override
    public Iterator<HoodieRecord> apply(WriteStatus writeStatus) throws Exception {
      List<HoodieRecord> allRecords = new ArrayList<>();
      if (enabledPartitionTypes.contains(RECORD_INDEX)) {
        allRecords.addAll(processWriteStatusForRLI(writeStatus, dataWriteConfig));
      }
      if (enabledPartitionTypes.contains(SECONDARY_INDEX)) {
        allRecords.addAll(processWriteStatusForSecondaryIndex(writeStatus));
      }
      // yet to add support for more partitions.
      // bloom filter
      // secondary index
      // expression index.
      return allRecords.iterator();
    }
  }

  protected static List<HoodieRecord> processWriteStatusForRLI(WriteStatus writeStatus, HoodieWriteConfig dataWriteConfig) {
    List<HoodieRecord> allRecords = new ArrayList<>();
    for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
      if (!writeStatus.isErrored(recordDelegate.getHoodieKey())) {
        if (recordDelegate.getIgnoreIndexUpdate()) {
          continue;
        }
        HoodieRecord hoodieRecord;
        Option<HoodieRecordLocation> newLocation = recordDelegate.getNewLocation();
        if (newLocation.isPresent()) {
          if (recordDelegate.getCurrentLocation().isPresent()) {
            // This is an update, no need to update index if the location has not changed
            // newLocation should have the same fileID as currentLocation. The instantTimes differ as newLocation's
            // instantTime refers to the current commit which was completed.
            if (!recordDelegate.getCurrentLocation().get().getFileId().equals(newLocation.get().getFileId())) {
              final String msg = String.format("Detected update in location of record with key %s from %s to %s. The fileID should not change.",
                  recordDelegate, recordDelegate.getCurrentLocation().get(), newLocation.get());
              LOG.error(msg);
              throw new HoodieMetadataException(msg);
            }
            // for updates, we can skip updating RLI partition in MDT
          } else {
            // Insert new record case
            hoodieRecord = HoodieMetadataPayload.createRecordIndexUpdate(
                recordDelegate.getRecordKey(), recordDelegate.getPartitionPath(),
                newLocation.get().getFileId(), newLocation.get().getInstantTime(), dataWriteConfig.getWritesFileIdEncoding());
            allRecords.add(hoodieRecord);
          }
        } else {
          // Delete existing index for a deleted record
          hoodieRecord = HoodieMetadataPayload.createRecordIndexDelete(recordDelegate.getRecordKey(), recordDelegate.getPartitionPath(), dataWriteConfig.isPartitionedRecordIndexEnabled());
          allRecords.add(hoodieRecord);
        }
      }
    }
    return allRecords;
  }

  protected static List<HoodieRecord> processWriteStatusForSecondaryIndex(WriteStatus writeStatus) {
    List<HoodieRecord> secondaryIndexRecords = new ArrayList<>(writeStatus.getIndexStats().getSecondaryIndexStats().size());
    for (Map.Entry<String, List<SecondaryIndexStats>> entry : writeStatus.getIndexStats().getSecondaryIndexStats().entrySet()) {
      String indexPartitionName = entry.getKey();
      List<SecondaryIndexStats> secondaryIndexStats = entry.getValue();
      for (SecondaryIndexStats stat : secondaryIndexStats) {
        secondaryIndexRecords.add(HoodieMetadataPayload.createSecondaryIndexRecord(stat.getRecordKey(), stat.getSecondaryKeyValue(), indexPartitionName, stat.isDeleted()));
      }
    }
    return secondaryIndexRecords;
  }
}
