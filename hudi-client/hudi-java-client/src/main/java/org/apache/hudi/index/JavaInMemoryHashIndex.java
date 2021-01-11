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

package org.apache.hudi.index;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
@SuppressWarnings("checkstyle:LineLength")
public class JavaInMemoryHashIndex<T extends HoodieRecordPayload> extends JavaHoodieIndex<T> {

  private static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

  public JavaInMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
    synchronized (JavaInMemoryHashIndex.class) {
      if (recordLocationMap == null) {
        recordLocationMap = new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> records, HoodieEngineContext context,
                                           HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
    records.stream().forEach(record -> {
      if (recordLocationMap.containsKey(record.getKey())) {
        record.unseal();
        record.setCurrentLocation(recordLocationMap.get(record.getKey()));
        record.seal();
      }
      taggedRecords.add(record);
    });
    return taggedRecords;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatusList,
                                          HoodieEngineContext context,
                                          HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> hoodieTable) {
    return writeStatusList.stream().map(writeStatus -> {
      for (HoodieRecord record : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(record.getKey())) {
          HoodieKey key = record.getKey();
          Option<HoodieRecordLocation> newLocation = record.getNewLocation();
          if (newLocation.isPresent()) {
            recordLocationMap.put(key, newLocation.get());
          } else {
            // Delete existing index for a deleted record
            recordLocationMap.remove(key);
          }
        }
      }
      return writeStatus;
    }).collect(Collectors.toList());
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
}
