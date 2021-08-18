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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.execution.PartitionIdPassthrough;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hoodie Index implementation backed by the record-level index in the Metadata Table.
 */
public class SparkRecordLevelIndex<T extends HoodieRecordPayload> extends SparkHoodieIndex<T> {
  private static final Logger LOG = LogManager.getLogger(SparkRecordLevelIndex.class);

  public SparkRecordLevelIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieEngineContext context,
                                              HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    final int numShards = config.getRecordLevelIndexShardCount();
    JavaRDD<HoodieRecord<T>> y = recordRDD.keyBy(r -> HoodieTableMetadataUtil.keyToShard(r.getRecordKey(), numShards))
        .partitionBy(new PartitionIdPassthrough(numShards))
        .map(t -> t._2);
    ValidationUtils.checkState(y.getNumPartitions() <= numShards);

    return y.mapPartitions(new LocationTagFunction(hoodieTable));
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD,
                                             HoodieEngineContext context,
                                             HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {

    JavaRDD<HoodieRecord> indexUpdateRDD = writeStatusRDD.flatMap(writeStatus -> {
      List<HoodieRecord> records = new LinkedList<>();
      for (HoodieRecord writtenRecord : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(writtenRecord.getKey())) {
          HoodieRecord indexRecord;
          HoodieKey key = writtenRecord.getKey();
          ValidationUtils.checkState(!key.getRecordKey().contains("Option"));
          Option<HoodieRecordLocation> newLocation = writtenRecord.getNewLocation();
          if (newLocation.isPresent()) {
            indexRecord = HoodieMetadataPayload.createRecordLevelIndexRecord(key.getRecordKey(), key.getPartitionPath(),
                newLocation.get().getFileId(), newLocation.get().getInstantTime());
          } else {
            // Delete existing index for a deleted record
            indexRecord = HoodieMetadataPayload.createRecordLevelIndexDelete(key.getRecordKey());
          }

          records.add(indexRecord);
        }
      }

      return records.iterator();
    });

    // TODO: Find a better way to enqueue records on the driver side for commit to metadata table
    if (hoodieTable.getMetadataWriter().isPresent()) {
      SparkHoodieBackedTableMetadataWriter metadataWriter = (SparkHoodieBackedTableMetadataWriter) hoodieTable.getMetadataWriter().get();
      metadataWriter.queueForUpdate(indexUpdateRDD, MetadataPartitionType.RECORD_LEVEL_INDEX, "");
    }

    return writeStatusRDD;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  /**
   * Function that tags each HoodieRecord with an existing location, if known.
   */
  class LocationTagFunction implements FlatMapFunction<Iterator<HoodieRecord<T>>, HoodieRecord<T>> {
    HoodieTable hoodieTable;

    public LocationTagFunction(HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
      this.hoodieTable = hoodieTable;
    }

    @Override
    public Iterator<HoodieRecord<T>> call(Iterator<HoodieRecord<T>> hoodieRecordIterator) {
      List<HoodieRecord<T>> taggedRecords = new ArrayList<>();
      Map<String, Integer> keyToIndexMap = new HashMap<>();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord rec = hoodieRecordIterator.next();
        ValidationUtils.checkState(!rec.getRecordKey().contains("Option"));
        keyToIndexMap.put(rec.getRecordKey(), taggedRecords.size());
        taggedRecords.add(rec);
      }

      Set<String> recordKeys = keyToIndexMap.keySet().stream().sorted().collect(Collectors.toSet());
      Map<String, HoodieRecordLocation> recordIndexInfo = hoodieTable.getMetadataReader().readRecordLevelIndex(recordKeys);

      for (Entry<String, HoodieRecordLocation> e : recordIndexInfo.entrySet()) {
        HoodieRecord rec = taggedRecords.get(keyToIndexMap.get(e.getKey()));
        rec.unseal();
        rec.setCurrentLocation(e.getValue());
        rec.seal();
      }

      return taggedRecords.iterator();
    }
  }
}


