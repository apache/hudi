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

package org.apache.hudi.index.simple;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.SparkConfigUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.io.HoodieKeyLocationFetchHandle;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

import scala.Tuple2;

import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * A simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 *
 * @param <T>
 */
public class HoodieSimpleIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  public HoodieSimpleIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
                                             HoodieTable<T> hoodieTable) {
    return writeStatusRDD;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc,
                                              HoodieTable<T> hoodieTable) {
    return tagLocationInternal(recordRDD, jsc, hoodieTable);
  }

  /**
   * Returns an RDD mapping each HoodieKey with a partitionPath/fileID which contains it. Option. Empty if the key is not
   * found.
   *
   * @param hoodieKeys  keys to lookup
   * @param jsc         spark context
   * @param hoodieTable hoodie table object
   */
  @Override
  public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
                                                                                  JavaSparkContext jsc, HoodieTable<T> hoodieTable) {

    return fetchRecordLocationInternal(hoodieKeys, jsc, hoodieTable, config.getSimpleIndexParallelism());
  }

  /**
   * Tags records location for incoming records.
   *
   * @param inputRecordRDD {@link JavaRDD} of incoming records
   * @param jsc            instance of {@link JavaSparkContext} to use
   * @param hoodieTable    instance of {@link HoodieTable} to use
   * @return {@link JavaRDD} of records with record locations set
   */
  protected JavaRDD<HoodieRecord<T>> tagLocationInternal(JavaRDD<HoodieRecord<T>> inputRecordRDD, JavaSparkContext jsc,
                                                         HoodieTable<T> hoodieTable) {
    if (config.getSimpleIndexUseCaching()) {
      inputRecordRDD.persist(SparkConfigUtils.getSimpleIndexInputStorageLevel(config.getProps()));
    }

    JavaPairRDD<HoodieKey, HoodieRecord<T>> keyedInputRecordRDD = inputRecordRDD.mapToPair(record -> new Tuple2<>(record.getKey(), record));
    JavaPairRDD<HoodieKey, HoodieRecordLocation> existingLocationsOnTable = fetchRecordLocationsForAffectedPartitions(keyedInputRecordRDD.keys(), jsc, hoodieTable,
        config.getSimpleIndexParallelism());

    JavaRDD<HoodieRecord<T>> taggedRecordRDD = keyedInputRecordRDD.leftOuterJoin(existingLocationsOnTable)
        .map(entry -> {
          final HoodieRecord<T> untaggedRecord = entry._2._1;
          final Option<HoodieRecordLocation> location = Option.ofNullable(entry._2._2.orNull());
          return HoodieIndexUtils.getTaggedRecord(untaggedRecord, location);
        });

    if (config.getSimpleIndexUseCaching()) {
      inputRecordRDD.unpersist();
    }
    return taggedRecordRDD;
  }

  /**
   * Fetch record locations for passed in {@link JavaRDD} of HoodieKeys.
   *
   * @param lookupKeys  {@link JavaRDD} of {@link HoodieKey}s
   * @param jsc         instance of {@link JavaSparkContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return Hoodiekeys mapped to partitionpath and filenames
   */
  JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocationInternal(JavaRDD<HoodieKey> lookupKeys, JavaSparkContext jsc,
                                                                                   HoodieTable<T> hoodieTable, int parallelism) {
    JavaPairRDD<HoodieKey, Option<HoodieRecordLocation>> keyLocationsRDD = lookupKeys.mapToPair(key -> new Tuple2<>(key, Option.empty()));
    JavaPairRDD<HoodieKey, HoodieRecordLocation> existingRecords = fetchRecordLocationsForAffectedPartitions(lookupKeys, jsc, hoodieTable, parallelism);

    return keyLocationsRDD.leftOuterJoin(existingRecords)
        .mapToPair(entry -> {
          final Option<HoodieRecordLocation> locationOpt = Option.ofNullable(entry._2._2.orNull());
          final HoodieKey key = entry._1;
          return locationOpt
              .map(location -> new Tuple2<>(key, Option.of(Pair.of(key.getPartitionPath(), location.getFileId()))))
              .orElse(new Tuple2<>(key, Option.empty()));
        });
  }

  /**
   * Fetch record locations for passed in {@link HoodieKey}s.
   *
   * @param hoodieKeys  {@link JavaRDD} of {@link HoodieKey}s for which locations are fetched
   * @param jsc         instance of {@link JavaSparkContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return {@link JavaPairRDD} of {@link HoodieKey} and {@link HoodieRecordLocation}
   */
  protected JavaPairRDD<HoodieKey, HoodieRecordLocation> fetchRecordLocationsForAffectedPartitions(JavaRDD<HoodieKey> hoodieKeys, JavaSparkContext jsc, HoodieTable<T> hoodieTable,
                                                                                                   int parallelism) {
    List<String> affectedPartitionPathList = hoodieKeys.map(HoodieKey::getPartitionPath).distinct().collect();
    List<Pair<String, HoodieBaseFile>> latestBaseFiles = getLatestBaseFilesForAllPartitions(affectedPartitionPathList, jsc, hoodieTable);
    return fetchRecordLocations(jsc, hoodieTable, parallelism, latestBaseFiles);
  }

  protected JavaPairRDD<HoodieKey, HoodieRecordLocation> fetchRecordLocations(JavaSparkContext jsc, HoodieTable<T> hoodieTable, int parallelism, List<Pair<String, HoodieBaseFile>> baseFiles) {
    int fetchParallelism = Math.max(1, Math.max(baseFiles.size(), parallelism));
    return jsc.parallelize(baseFiles, fetchParallelism)
        .flatMapToPair(partitionPathBaseFile -> new HoodieKeyLocationFetchHandle(config, hoodieTable, partitionPathBaseFile).locations());
  }
}
