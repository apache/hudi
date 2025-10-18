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

package org.apache.hudi.metadata;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Mapper for Record Level Index (RLI).
 */
public class RecordIndexMapper extends MetadataIndexMapper {
  private static final Logger LOG = LoggerFactory.getLogger(RecordIndexMapper.class);

  public RecordIndexMapper(HoodieWriteConfig dataWriteConfig) {
    super(dataWriteConfig);
  }

  @Override
  protected List<HoodieRecord> generateRecords(WriteStatus writeStatus) {
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
}
