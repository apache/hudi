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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkInMemoryHashIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {

  private static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

  public SparkInMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
    synchronized (SparkInMemoryHashIndex.class) {
      if (recordLocationMap == null) {
        recordLocationMap = new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieEngineContext context,
                                              HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    return recordRDD.mapPartitionsWithIndex(this.new LocationTagFunction(), true);
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
                                             HoodieEngineContext context,
                                             HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    return writeStatusRDD.map(writeStatus -> {
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

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  class LocationTagFunction implements Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> {

    @Override
    public Iterator<HoodieRecord<T>> call(Integer partitionNum, Iterator<HoodieRecord<T>> hoodieRecordIterator) {
      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord<T> rec = hoodieRecordIterator.next();
        if (recordLocationMap.containsKey(rec.getKey())) {
          rec.unseal();
          rec.setCurrentLocation(recordLocationMap.get(rec.getKey()));
          rec.seal();
        }
        taggedRecords.add(rec);
      }
      return taggedRecords.iterator();
    }
  }
}
