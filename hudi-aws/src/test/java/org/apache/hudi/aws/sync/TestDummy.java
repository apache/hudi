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

package org.apache.hudi.aws.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.examples.common.HoodieExampleDataGenerator;
import org.apache.hudi.index.HoodieIndex;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;

/**
 * docker run --rm -p 5000:5000 --name moto motoserver/moto:latest
 * see http://localhost:5000/moto-api/# to check aws resources
 */
public class TestDummy {

  @Test
  public void testJavaClient() throws IOException {
    String tablePath = "file:///tmp/hoodie/sample-table";
    Configuration hadoopConf = new Configuration();
    String tableType = "COPY_ON_WRITE";
    String tableName = "hoodie_rt";
    HoodieTableMetaClient.withPropertyBuilder()
              .setTableType(tableType)
              .setTableName(tableName)
              .setPayloadClassName(HoodieAvroPayload.class.getName())
              .initTable(hadoopConf, tablePath);

    String schema = HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA;
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
            .withSchema(schema).withParallelism(2, 2)
            .withDeleteParallelism(2).forTable(tableName)
            .withEmbeddedTimelineServerEnabled(false)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
    HoodieJavaWriteClient<HoodieAvroPayload> client =
            new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
//
    String newCommitTime = client.startCommit();
    HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();
    List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
    List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
    List<HoodieRecord<HoodieAvroPayload>> writeRecords =
            recordsSoFar.stream().map(r -> new HoodieAvroRecord<>(r)).collect(Collectors.toList());
    client.insert(writeRecords, newCommitTime);
    client.close();
  }
}
