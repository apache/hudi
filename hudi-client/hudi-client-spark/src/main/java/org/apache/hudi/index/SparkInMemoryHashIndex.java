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
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.BaseHoodieTable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Spark implementation of hoodie index backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
public class SparkInMemoryHashIndex<T extends HoodieRecordPayload> extends BaseInMemoryHashIndex<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  public SparkInMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieEngineContext context, BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> hoodieTable) throws HoodieIndexException {
    return recordRDD.mapPartitionsWithIndex(this.new LocationTagFunction(), true);
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, HoodieEngineContext context, BaseHoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> hoodieTable) throws HoodieIndexException {
    return writeStatusRDD.map(new Function<WriteStatus, WriteStatus>() {
      @Override
      public WriteStatus call(WriteStatus writeStatus) {
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
      }
    });

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
