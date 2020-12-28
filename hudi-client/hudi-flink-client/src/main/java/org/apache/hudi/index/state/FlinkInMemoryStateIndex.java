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

package org.apache.hudi.index.state;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.FlinkHoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Hoodie index implementation backed by flink state.
 *
 * @param <T> type of payload
 */
public class FlinkInMemoryStateIndex<T extends HoodieRecordPayload> extends FlinkHoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(FlinkInMemoryStateIndex.class);
  private MapState<HoodieKey, HoodieRecordLocation> mapState;

  public FlinkInMemoryStateIndex(HoodieFlinkEngineContext context, HoodieWriteConfig config) {
    super(config);
    if (context.getRuntimeContext() != null) {
      MapStateDescriptor<HoodieKey, HoodieRecordLocation> indexStateDesc =
          new MapStateDescriptor<>("indexState", TypeInformation.of(HoodieKey.class), TypeInformation.of(HoodieRecordLocation.class));
      if (context.getRuntimeContext() != null) {
        mapState = context.getRuntimeContext().getMapState(indexStateDesc);
      }
    }
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> records,
                                           HoodieEngineContext context,
                                           HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException {
    return context.map(records, record -> {
      try {
        if (mapState.contains(record.getKey())) {
          record.unseal();
          record.setCurrentLocation(mapState.get(record.getKey()));
          record.seal();
        }
      } catch (Exception e) {
        LOG.error(String.format("Tag record location failed, key = %s, %s", record.getRecordKey(), e.getMessage()));
      }
      return record;
    }, 0);
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatuses,
                                          HoodieEngineContext context,
                                          HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) throws HoodieIndexException {
    return context.map(writeStatuses, writeStatus -> {
      for (HoodieRecord record : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(record.getKey())) {
          HoodieKey key = record.getKey();
          Option<HoodieRecordLocation> newLocation = record.getNewLocation();
          if (newLocation.isPresent()) {
            try {
              mapState.put(key, newLocation.get());
            } catch (Exception e) {
              LOG.error(String.format("Update record location failed, key = %s, %s", record.getRecordKey(), e.getMessage()));
            }
          } else {
            // Delete existing index for a deleted record
            try {
              mapState.remove(key);
            } catch (Exception e) {
              LOG.error(String.format("Remove record location failed, key = %s, %s", record.getRecordKey(), e.getMessage()));
            }
          }
        }
      }
      return writeStatus;
    }, 0);
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
}