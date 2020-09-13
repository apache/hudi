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

package org.apache.hudi.client;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.PartialUpdatePayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_TRIP_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_SCHEMA;
import static org.apache.hudi.common.testutils.Transformations.recordsToHoodieKeys;
import static org.apache.hudi.common.util.ParquetUtils.readAvroRecords;
import static org.apache.hudi.common.util.ParquetUtils.readAvroSchema;
import static org.apache.hudi.common.util.ParquetUtils.readRowKeysFromParquet;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodiePartialUpdate extends HoodieClientTestBase {

  @Test
  public void testCopyOnWritePartialUpdate() {
    final String testPartitionPath = "2016/09/26";
    HoodieWriteClient client = getHoodieWriteClient(getConfig(true, false));
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);

    List<HoodieRecord> inserts1 =
            dataGen.generateInsertsStream(commitTime1, 100, false, TRIP_SCHEMA).collect(Collectors.toList()); // this writes ~500kb

    List<HoodieKey> insertKeys = recordsToHoodieKeys(inserts1);
    upsertAndCheck(client, insertKeys, commitTime1, false);

    client = getHoodieWriteClient(getConfig(true, true));
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2);

    WriteStatus writeStatus = upsertAndCheck(client, insertKeys, commitTime2, true);

    Schema schema = readAvroSchema(hadoopConf, new Path(basePath, writeStatus.getStat().getPath()));
    List<String> oldSchemaFieldNames = AVRO_TRIP_SCHEMA.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    List<String> parquetFieldNames = schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());

    for (String name : oldSchemaFieldNames) {
      assertTrue(parquetFieldNames.contains(name));
    }

    List<GenericRecord> records1 = readAvroRecords(hadoopConf, new Path(basePath, writeStatus.getStat().getPath()));
    for (GenericRecord record : records1) {
      assertEquals("rider-" + commitTime1, record.get("rider").toString());
      assertEquals("driver-" + commitTime1, record.get("driver").toString());
      assertEquals(String.valueOf(1.0), record.get("timestamp").toString());
    }
  }

  private WriteStatus upsertAndCheck(HoodieWriteClient client, List<HoodieKey> insertKeys, String commitTime, boolean partial) {
    List<HoodieRecord> records = new ArrayList<>();
    for (HoodieKey hoodieKey : insertKeys) {
      PartialUpdatePayload payload;
      if (partial) {
        payload = dataGen.generatePartialUpdatePayloadForPartialTripSchema(hoodieKey, commitTime);
      } else {
        payload = dataGen.generatePartialUpdatePayloadForTripSchema(hoodieKey, commitTime);
      }
      HoodieRecord record = new HoodieRecord(hoodieKey, payload);
      records.add(record);
    }

    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    assertEquals(100,
            readRowKeysFromParquet(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
                    .size(), "file should contain 100 records");

    return statuses.get(0);
  }
}
