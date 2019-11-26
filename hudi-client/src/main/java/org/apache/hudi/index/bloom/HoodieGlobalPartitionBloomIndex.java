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

package org.apache.hudi.index.bloom;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

/**
 * This BloomIndex is intended for compatibility with the standard HoodieBloomIndex.
 * It allows users to delete and update across partitions by using "*" as the partition key.
 * Uses a similar mechanism as HoodieGlobalBloomIndex to scan the index's across partitions.
 */

public class HoodieGlobalPartitionBloomIndex<T extends HoodieRecordPayload> extends HoodieGlobalBloomIndex<T> {

  private static final transient Logger log = LogManager.getLogger(HoodieGlobalPartitionBloomIndex.class);

  private static final String SELECT_ALL_PARTITION_CHAR = "*";

  public HoodieGlobalPartitionBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  protected JavaRDD<HoodieRecord<T>> tagLocationBacktoRecords(
      JavaPairRDD<HoodieKey, HoodieRecordLocation> keyFilenamePairRDD, JavaRDD<HoodieRecord<T>> recordRDD) {
    JavaPairRDD<String, HoodieRecord<T>> rowKeyRecordPairRDD =
        recordRDD.mapToPair(record -> new Tuple2<>(record.getRecordKey(), record));

    // Here we group by record key to get an iterator of hoodie records matching each record_key
    JavaPairRDD<String, Iterable<Tuple2<HoodieRecordLocation, HoodieKey>>> completeRDD =
        keyFilenamePairRDD.mapToPair(p -> new Tuple2<>(p._1.getRecordKey(), new Tuple2<>(p._2, p._1))).groupByKey();

    return rowKeyRecordPairRDD.leftOuterJoin(completeRDD).values().flatMap(this::generateRecords);
  }

  private Iterator<HoodieRecord<T>> generateRecords(
      Tuple2<HoodieRecord<T>, Optional<Iterable<Tuple2<HoodieRecordLocation, HoodieKey>>>> value) {
    List<HoodieRecord<T>> records = new ArrayList<>();
    if (!value._2.isPresent()) {
      // no matching record_keys is an insert
      if (value._1.getPartitionPath().equals(SELECT_ALL_PARTITION_CHAR)) {
        log.warn("Did not find records in any partitions matching record_key " + value._1.getRecordKey());
      } else {
        records.add(getTaggedRecord(value._1, Option.empty()));
      }
    } else {
      // loop through matching record_keys for updates
      boolean isUpdate = false;
      for (Tuple2<HoodieRecordLocation, HoodieKey> fileTarget : value._2().get()) {
        if (value._1.getPartitionPath().equals(SELECT_ALL_PARTITION_CHAR) || value._1.getPartitionPath().equals(fileTarget._2.getPartitionPath())) {
          records.add(getTaggedRecord(new HoodieRecord<>(fileTarget._2, value._1.getData()),
              Option.ofNullable(fileTarget._1)));
          isUpdate = true;
        }
      }
      if (!isUpdate && !value._1.getPartitionPath().equals(SELECT_ALL_PARTITION_CHAR)) {
        // matching record_key but no matching partitions should be an insert
        records.add(getTaggedRecord(value._1, Option.empty()));
      }
    }
    return records.iterator();
  }
}