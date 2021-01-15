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

package org.apache.hudi.error;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieErrorTableConfig;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertHasWriteErrors;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.apache.hudi.common.testutils.Transformations.recordsToHoodieKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieBackedErrorTable extends HoodieClientTestBase {

  @Test
  public void testInsertError() {
    SparkRDDWriteClient writeClient = getHoodieWriteClient(getConfig());
    String commitTime = writeClient.startCommit();
    List<HoodieRecord> hoodieRecords = dataGen.generateInserts(commitTime, 100);

    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(hoodieRecords, 1);
    JavaRDD<WriteStatus> writeStatusRDD = writeClient.insertError(recordsRDD, commitTime);
    List<WriteStatus> statuses = writeStatusRDD.collect();
    assertNoWriteErrors(statuses);
  }

  @Test
  public void testHasErrorRecordInsert() {
    HoodieWriteConfig config1 = getConfigBuilder(TRIP_SCHEMA)
            .withMemoryConfig(HoodieMemoryConfig.newBuilder().withWriteStatusFailureFraction(1).build())
            .withErrorTableConfig(HoodieErrorTableConfig.newBuilder().enable(true).build()).build();
    SparkRDDWriteClient writeClient1 = getHoodieWriteClient(config1);
    final String testPartitionPath = "2020/03/17";
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    String commitTime1 = writeClient1.startCommit();
    List<HoodieRecord> hoodieRecords1 =
            dataGen.generateInsertsStream(commitTime1, 10, false, TRIP_SCHEMA).collect(Collectors.toList());
    List<HoodieKey> insertKeys1 = recordsToHoodieKeys(hoodieRecords1);
    List<WriteStatus> writeStatuses = insertAndCheck(writeClient1, insertKeys1, commitTime1);
    assertHasWriteErrors(writeStatuses);
    assertEquals(10, writeStatuses.get(0).getFailedRecords().size(),
            "file shoule contain 100 records");
  }

  private List<WriteStatus> insertAndCheck(SparkRDDWriteClient client, List<HoodieKey> insertKeys, String commitTime) {
    List<HoodieRecord> records = new ArrayList<>();

    for (HoodieKey hoodieKey : insertKeys) {
      OverwriteWithLatestAvroPayload payload;
      payload = dataGen.generateMissWithOverwriteWithLatestAvroPayload(hoodieKey, commitTime);
      HoodieRecord record = new HoodieRecord(hoodieKey, payload);
      records.add(record);
    }

    JavaRDD<HoodieRecord> insertRecord = jsc.parallelize(records, 1);
    return client.insert(insertRecord, commitTime).collect();
  }
}
