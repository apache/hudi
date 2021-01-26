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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * A global simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkHoodieGlobalSimpleIndex<T extends HoodieRecordPayload> extends SparkHoodieSimpleIndex<T> {

  public SparkHoodieGlobalSimpleIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, HoodieEngineContext context,
                                              HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    return tagLocationInternal(recordRDD, context, hoodieTable);
  }

  /**
   * Tags records location for incoming records.
   *
   * @param inputRecordRDD   {@link JavaRDD} of incoming records
   * @param context         instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} to use
   * @return {@link JavaRDD} of records with record locations set
   */
  @Override
  protected JavaRDD<HoodieRecord<T>> tagLocationInternal(JavaRDD<HoodieRecord<T>> inputRecordRDD, HoodieEngineContext context,
                                                         HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {

    JavaPairRDD<String, HoodieRecord<T>> keyedInputRecordRDD = inputRecordRDD.mapToPair(entry -> new Tuple2<>(entry.getRecordKey(), entry));
    JavaPairRDD<HoodieKey, HoodieRecordLocation> allRecordLocationsInTable = fetchAllRecordLocations(context, hoodieTable,
        config.getGlobalSimpleIndexParallelism());
    return getTaggedRecords(keyedInputRecordRDD, allRecordLocationsInTable);
  }

  /**
   * Fetch record locations for passed in {@link HoodieKey}s.
   *
   * @param context         instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return {@link JavaPairRDD} of {@link HoodieKey} and {@link HoodieRecordLocation}
   */
  protected JavaPairRDD<HoodieKey, HoodieRecordLocation> fetchAllRecordLocations(HoodieEngineContext context,
                                                                                 HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable,
                                                                                 int parallelism) {
    List<Pair<String, HoodieBaseFile>> latestBaseFiles = getAllBaseFilesInTable(context, hoodieTable);
    return fetchRecordLocations(context, hoodieTable, parallelism, latestBaseFiles);
  }

  /**
   * Load all files for all partitions as <Partition, filename> pair RDD.
   */
  protected List<Pair<String, HoodieBaseFile>> getAllBaseFilesInTable(final HoodieEngineContext context,
                                                                      final HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), metaClient.getBasePath());
    // Obtain the latest data files from all the partitions.
    return getLatestBaseFilesForAllPartitions(allPartitionPaths, context, hoodieTable);
  }

  /**
   * Tag records with right {@link HoodieRecordLocation}.
   *
   * @param incomingRecords incoming {@link HoodieRecord}s
   * @param existingRecords existing records with {@link HoodieRecordLocation}s
   * @return {@link JavaRDD} of {@link HoodieRecord}s with tagged {@link HoodieRecordLocation}s
   */
  private JavaRDD<HoodieRecord<T>> getTaggedRecords(JavaPairRDD<String, HoodieRecord<T>> incomingRecords, JavaPairRDD<HoodieKey, HoodieRecordLocation> existingRecords) {
    JavaPairRDD<String, Pair<String, HoodieRecordLocation>> existingRecordByRecordKey = existingRecords
        .mapToPair(entry -> new Tuple2<>(entry._1.getRecordKey(), Pair.of(entry._1.getPartitionPath(), entry._2)));

    return incomingRecords.leftOuterJoin(existingRecordByRecordKey).values()
        .flatMap(entry -> {
          HoodieRecord<T> inputRecord = entry._1;
          Option<Pair<String, HoodieRecordLocation>> partitionPathLocationPair = Option.ofNullable(entry._2.orNull());
          List<HoodieRecord<T>> taggedRecords;

          if (partitionPathLocationPair.isPresent()) {
            String partitionPath = partitionPathLocationPair.get().getKey();
            HoodieRecordLocation location = partitionPathLocationPair.get().getRight();
            if (config.getGlobalSimpleIndexUpdatePartitionPath() && !(inputRecord.getPartitionPath().equals(partitionPath))) {
              // Create an empty record to delete the record in the old partition
              HoodieRecord<T> deleteRecord = new HoodieRecord(new HoodieKey(inputRecord.getRecordKey(), partitionPath), new EmptyHoodieRecordPayload());
              deleteRecord.setCurrentLocation(location);
              deleteRecord.seal();
              // Tag the incoming record for inserting to the new partition
              HoodieRecord<T> insertRecord = (HoodieRecord<T>) HoodieIndexUtils.getTaggedRecord(inputRecord, Option.empty());
              taggedRecords = Arrays.asList(deleteRecord, insertRecord);
            } else {
              // Ignore the incoming record's partition, regardless of whether it differs from its old partition or not.
              // When it differs, the record will still be updated at its old partition.
              HoodieRecord<T> newRecord = new HoodieRecord<>(new HoodieKey(inputRecord.getRecordKey(), partitionPath), inputRecord.getData());
              taggedRecords = Collections.singletonList((HoodieRecord<T>) HoodieIndexUtils.getTaggedRecord(newRecord, Option.ofNullable(location)));
            }
          } else {
            taggedRecords = Collections.singletonList((HoodieRecord<T>) HoodieIndexUtils.getTaggedRecord(inputRecord, Option.empty()));
          }
          return taggedRecords.iterator();
        });
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
