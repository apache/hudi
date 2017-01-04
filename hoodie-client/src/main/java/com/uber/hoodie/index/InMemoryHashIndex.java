/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.index;

import com.google.common.base.Optional;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.model.HoodieRecordPayload;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 *
 * ONLY USE FOR LOCAL TESTING
 *
 */
public class InMemoryHashIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

    private static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

    public InMemoryHashIndex(HoodieWriteConfig config, JavaSparkContext jsc) {
        super(config, jsc);
        recordLocationMap = new ConcurrentHashMap<>();
    }

    @Override
    public JavaPairRDD<HoodieKey, Optional<String>> fetchRecordLocation(
        JavaRDD<HoodieKey> hoodieKeys, final HoodieTableMetaClient metaClient) {
        throw new UnsupportedOperationException("InMemory index does not implement check exist yet");
    }

    /**
     * Function that tags each HoodieRecord with an existing location, if known.
     */
    class LocationTagFunction
            implements Function2<Integer, Iterator<HoodieRecord<T>>, Iterator<HoodieRecord<T>>> {
        @Override
        public Iterator<HoodieRecord<T>> call(Integer partitionNum,
                                           Iterator<HoodieRecord<T>> hoodieRecordIterator) {
            List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
            while (hoodieRecordIterator.hasNext()) {
                HoodieRecord<T> rec = hoodieRecordIterator.next();
                if (recordLocationMap.containsKey(rec.getKey())) {
                    rec.setCurrentLocation(recordLocationMap.get(rec.getKey()));
                }
                taggedRecords.add(rec);
            }
            return taggedRecords.iterator();
        }
    }

    @Override
    public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD,
                                             HoodieTableMetaClient metaClient) {
        return recordRDD.mapPartitionsWithIndex(this.new LocationTagFunction(), true);
    }

    @Override
    public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
                                               HoodieTableMetaClient metaClient) {
        return writeStatusRDD.map(new Function<WriteStatus, WriteStatus>() {
            @Override
            public WriteStatus call(WriteStatus writeStatus) {
                for (HoodieRecord record : writeStatus.getWrittenRecords()) {
                    if (!writeStatus.isErrored(record.getKey())) {
                        recordLocationMap.put(record.getKey(), record.getNewLocation());
                    }
                }
                return writeStatus;
            }
        });
    }

    @Override
    public boolean rollbackCommit(String commitTime) {
        // TODO (weiy)
        return true;
    }
}
