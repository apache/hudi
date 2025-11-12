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

package org.apache.hudi.index.inmemory;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
public class HoodieInMemoryHashIndex
    extends HoodieIndex<Object, Object> {

  private static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

  public HoodieInMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
    synchronized (HoodieInMemoryHashIndex.class) {
      if (recordLocationMap == null) {
        recordLocationMap = new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return records.mapPartitions(hoodieRecordIterator -> {
      List<HoodieRecord<R>> taggedRecords = new ArrayList<>();
      HoodieTimeline commitsTimeline = hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord<R> record = hoodieRecordIterator.next();
        HoodieRecordLocation location = recordLocationMap.get(record.getKey());
        if ((location != null) && HoodieIndexUtils.checkIfValidCommit(commitsTimeline, location.getInstantTime())) {
          record.unseal();
          record.setCurrentLocation(location);
          record.seal();
        }
        taggedRecords.add(record);
      }
      return taggedRecords.iterator();
    }, true);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(
      HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return writeStatuses.map(writeStatus -> {
      for (HoodieRecordDelegate recordDelegate : writeStatus.getIndexStats().getWrittenRecordDelegates()) {
        if (!writeStatus.isErrored(recordDelegate.getHoodieKey())) {
          Option<HoodieRecordLocation> newLocation = recordDelegate.getNewLocation();
          if (newLocation.isPresent()) {
            recordLocationMap.put(recordDelegate.getHoodieKey(), newLocation.get());
          } else {
            // Delete existing index for a deleted record
            recordLocationMap.remove(recordDelegate.getHoodieKey());
          }
        }
      }
      return writeStatus;
    });
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  @VisibleForTesting
  public static void clear() {
    if (recordLocationMap != null) {
      recordLocationMap.clear();
    }
  }
}
