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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSparkMetadataTableUpsertPartitioner extends HoodieClientTestBase {

  private static final Random RANDOM = new Random(0xDEED);
  private static final String RANDOM_INSTANT_TIME = "100000";

  @Test
  public void testUpsertPartitioner() {

    List<BucketInfo> bucketInfoList = new ArrayList<>();
    Map<String, Integer> fileIdToSparkPartitionIndexMap = new HashMap<>();
    Map<String, BucketInfo> fileIdToBucketInfoMapping = new HashMap<>();

    // lets generate 5 file groups for each partition.
    Map<String, List<String>> fileIdsPerPartition = new HashMap<>();
    int counter = 0;
    for (String partition : HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS) {
      for (int i = 0; i < 5; i++) {
        String fileIdPrefix = UUID.randomUUID().toString();
        BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, fileIdPrefix, partition);
        bucketInfoList.add(bucketInfo);
        fileIdToBucketInfoMapping.put(fileIdPrefix, bucketInfo);
        fileIdsPerPartition.computeIfAbsent(partition, s -> new ArrayList<>());
        fileIdsPerPartition.get(partition).add(fileIdPrefix);
        fileIdToSparkPartitionIndexMap.put(fileIdPrefix, counter++);
      }
    }

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

    List<HoodieRecord> records = dataGenerator.generateInserts(RANDOM_INSTANT_TIME, 300);
    // generate expected values
    Map<String, BucketInfo> expectedBucketInfoForRecordKey = new HashMap<>();

    List<Tuple2<HoodieKey, Option<HoodieRecordLocation>>> keysToProbe = records.stream().map(record -> {
      HoodieKey hoodieKey = record.getKey();
      String partition = hoodieKey.getPartitionPath();
      List<String> fileIdsForThisPartition = fileIdsPerPartition.get(partition);
      String fileId = fileIdsForThisPartition.get(RANDOM.nextInt(fileIdsForThisPartition.size()));
      expectedBucketInfoForRecordKey.put(hoodieKey.getRecordKey(), fileIdToBucketInfoMapping.get(fileId));
      return new Tuple2<>(hoodieKey, Option.of(new HoodieRecordLocation(RANDOM_INSTANT_TIME, fileId)));
    }).collect(Collectors.toList());

    SparkHoodiePartitioner partitioner = new SparkMetadataTableUpsertPartitioner(bucketInfoList, fileIdToSparkPartitionIndexMap);
    SparkBucketInfoGetter bucketInfoGetter = partitioner.getSparkBucketInfoGetter();
    keysToProbe.stream().forEach(keyToProbe -> {
      int sparkPartitionIndex = partitioner.getPartition(keyToProbe);
      BucketInfo bucketInfo = bucketInfoGetter.getBucketInfo(sparkPartitionIndex);
      // validate expected values.
      assertEquals(expectedBucketInfoForRecordKey.get(keyToProbe._1.getRecordKey()), bucketInfo);
      assertEquals(bucketInfoList.get(sparkPartitionIndex), bucketInfo);
    });
  }

}
