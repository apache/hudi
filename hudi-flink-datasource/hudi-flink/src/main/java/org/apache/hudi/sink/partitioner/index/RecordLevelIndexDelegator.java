/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.StreamerUtil;

import lombok.Getter;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link IndexDelegator} based on the record level index in metadata table.
 */
public class RecordLevelIndexDelegator implements MinibatchIndexDelegator {
  @VisibleForTesting
  @Getter
  private final RecordIndexCache recordIndexCache;
  private final Configuration conf;
  private final HoodieTableMetaClient metaClient;
  private HoodieTableMetadata metadataTable;

  public RecordLevelIndexDelegator(Configuration conf, long initCheckpointId) {
    this.metaClient = StreamerUtil.createMetaClient(conf);
    this.conf = conf;
    this.recordIndexCache = new RecordIndexCache(conf, initCheckpointId);
    reloadMetadataTable();
  }

  @Override
  public HoodieRecordGlobalLocation get(String recordKey) throws IOException {
    return get(Collections.singletonList(recordKey)).get(recordKey);
  }

  @Override
  public void update(String recordKey, HoodieRecordGlobalLocation recordGlobalLocation) {
    recordIndexCache.update(recordKey, recordGlobalLocation);
  }

  @Override
  public Map<String, HoodieRecordGlobalLocation> get(List<String> recordKeys) throws IOException {
    // use a linked hash map to keep the natural order.
    Map<String, HoodieRecordGlobalLocation> keysAndLocations = new LinkedHashMap<>();
    List<String> missedKeys = new ArrayList<>();
    for (String key: recordKeys) {
      HoodieRecordGlobalLocation location = recordIndexCache.get(key);
      if (location == null) {
        missedKeys.add(key);
      }
      // insert anyway even the location is null to keep the natural order.
      keysAndLocations.put(key, location);
    }
    if (!missedKeys.isEmpty()) {
      HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
          metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(missedKeys));
      List<Pair<String, HoodieRecordGlobalLocation>> recordIndexLocations = HoodieDataUtils.dedupeAndCollectAsList(recordIndexData);
      recordIndexLocations.forEach(keyAndLocation -> {
        recordIndexCache.update(keyAndLocation.getKey(), keyAndLocation.getValue());
        keysAndLocations.put(keyAndLocation.getKey(), keyAndLocation.getValue());
      });
    }
    return keysAndLocations;
  }

  @Override
  public void update(List<Pair<String, HoodieRecordGlobalLocation>> recordKeysAndLocations) throws IOException {
    recordKeysAndLocations.forEach(keyAndLocation -> recordIndexCache.update(keyAndLocation.getKey(), keyAndLocation.getValue()));
  }

  @Override
  public void onCheckpoint(long checkpointId) {
    recordIndexCache.addCheckpointCache(checkpointId);
  }

  @Override
  public void onCommitSuccess(long checkpointId) {
    recordIndexCache.clean(checkpointId);
    this.metaClient.reloadActiveTimeline();
    reloadMetadataTable();
  }

  private void reloadMetadataTable() {
    this.metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
  }

  @Override
  public void close() throws IOException {
    this.recordIndexCache.close();
    if (this.metadataTable == null) {
      return;
    }
    try {
      this.metadataTable.close();
    } catch (Exception e) {
      throw new HoodieException("Exception happened during close metadata table.", e);
    }
  }
}
