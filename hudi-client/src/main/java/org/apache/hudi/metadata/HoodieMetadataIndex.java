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

package org.apache.hudi.metadata;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * An Index which is specific to the Hoodie Metadata Table.
 *
 * Hoodie Metadata Table saves information in specific named partitions. So the partition and the base file location
 * during indexing can be determined by looking at the record itself. This speeds up the indexing during updates.
 */
public class HoodieMetadataIndex<T extends HoodieMetadataPayload> extends HoodieIndex<T> {

  public HoodieMetadataIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Returns an RDD mapping each HoodieKey with a partitionPath/fileID which contains it. Option.Empty if the key is not
   * found.
   *
   * @param hoodieKeys  keys to lookup
   * @param jsc         spark context
   * @param hoodieTable hoodie table object
   */
  @Override
  public JavaPairRDD<HoodieKey, Option<Pair<String, String>>> fetchRecordLocation(JavaRDD<HoodieKey> hoodieKeys,
                                                                                  JavaSparkContext jsc,
                                                                                  HoodieTable<T> hoodieTable) {
    JavaPairRDD<String, String> partitionRecordKeyPairRDD =
        hoodieKeys.mapToPair(key -> new Tuple2<>(key.getPartitionPath(), key.getRecordKey()));

    // Lookup location of all the hoodie keys
    JavaPairRDD<HoodieKey, HoodieRecordLocation> recordKeyLocationRDD = lookupIndex(hoodieKeys, jsc, hoodieTable);
    JavaPairRDD<HoodieKey, String> keyHoodieKeyPairRDD = hoodieKeys.mapToPair(key -> new Tuple2<>(key, null));

    return keyHoodieKeyPairRDD.leftOuterJoin(recordKeyLocationRDD).mapToPair(keyLoc -> {
      Option<Pair<String, String>> partitionPathFileidPair;
      if (keyLoc._2._2.isPresent()) {
        partitionPathFileidPair = Option.of(Pair.of(keyLoc._1().getPartitionPath(), keyLoc._2._2.get().getFileId()));
      } else {
        partitionPathFileidPair = Option.empty();
      }
      return new Tuple2<>(keyLoc._1, partitionPathFileidPair);
    });
  }

  /**
   * Tag each record with its current location (partition / fileId).
   *
   * @param recordRDD   records to tag
   * @param jsc         spark context
   * @param hoodieTable hoodie table object
   */
  @Override
  public JavaRDD<HoodieRecord<T>> tagLocation(JavaRDD<HoodieRecord<T>> recordRDD, JavaSparkContext jsc,
                                              HoodieTable<T> hoodieTable) throws HoodieIndexException {
    JavaPairRDD<HoodieKey, HoodieRecord> keyRecordPairRDD = recordRDD.mapToPair(r -> new Tuple2<>(r.getKey(), r));
    JavaPairRDD<HoodieKey, HoodieRecordLocation> keyLocationPairRDD =
        lookupIndex(keyRecordPairRDD.keys(), jsc, hoodieTable);

    return keyRecordPairRDD.leftOuterJoin(keyLocationPairRDD).values().map(v1 -> {
      return HoodieIndexUtils.getTaggedRecord(v1._1, Option.ofNullable(v1._2.orNull()));
    });
  }

  /**
   * Update the index (if required) from the results of the writes into the hoodie table.
   *
   * @param writeStatusRDD  status of the writes
   * @param jsc             spark context
   * @param hoodieTable     hoodie table object
   */
  @Override
  public JavaRDD<WriteStatus> updateLocation(JavaRDD<WriteStatus> writeStatusRDD, JavaSparkContext jsc,
                                             HoodieTable<T> hoodieTable)
      throws HoodieIndexException {
    // No update required for metadata index
    return writeStatusRDD;
  }

  /**
   * Lookup the location for each hoodie key and return the pair<key, location> for all record keys already
   * present and drop the record keys if not present.
   */
  private JavaPairRDD<HoodieKey, HoodieRecordLocation> lookupIndex(JavaRDD<HoodieKey> hoodieKeys,
                                                                   JavaSparkContext jsc,
                                                                   HoodieTable<T> hoodieTable) {
    SliceView fsView = hoodieTable.getSliceView();
    List<HoodieBaseFile> baseFiles = fsView.getLatestFileSlices(HoodieMetadataImpl.METADATA_PARTITION_NAME)
        .map(s -> s.getBaseFile())
        .filter(b -> b.isPresent())
        .map(b -> b.get())
        .collect(Collectors.toList());

    // All the metadata fits within a single base file
    if (baseFiles.size() > 1) {
      throw new HoodieMetadataException("Multiple base files found in metadata partition");
    }

    String fileId;
    String instantTime;
    if (!baseFiles.isEmpty()) {
      fileId = baseFiles.get(0).getFileId();
      instantTime = baseFiles.get(0).getCommitTime();
    } else {
      // If there is a log file then we can assume that it has the data
      List<HoodieLogFile> logFiles = fsView.getLatestFileSlices(HoodieMetadataImpl.METADATA_PARTITION_NAME)
          .map(s -> s.getLatestLogFile())
          .filter(b -> b.isPresent())
          .map(b -> b.get())
          .collect(Collectors.toList());
      if (logFiles.isEmpty()) {
        // No base and log files. All are new inserts
        return jsc.emptyRDD().mapToPair(f -> null);
      }

      fileId = logFiles.get(0).getFileId();
      instantTime = logFiles.get(0).getBaseCommitTime();
    }

    String partition = HoodieMetadataImpl.METADATA_PARTITION_NAME;
    return hoodieKeys.mapToPair(key -> {
      return new Tuple2<>(new HoodieKey(key.getRecordKey(), partition), new HoodieRecordLocation(instantTime, fileId));
    });

  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    // Nope, don't need to do anything.
    return true;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

}
