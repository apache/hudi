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

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ITTestAwsGlueCatalogSyncTool extends ITTestGlueUtil {

  @Test
  public void testJavaClient() throws IOException, ExecutionException, InterruptedException, URISyntaxException {

    String parts = "driver";
    HoodieJavaWriteClient<HoodieAvroPayload> client = clientCOW(HoodieDataGenerator.TRIP_EXAMPLE_SCHEMA, Option.of(parts));

    String newCommitTime = client.startCommit();
    List<HoodieRecord<HoodieAvroPayload>> writeRecords = getHoodieRecords(newCommitTime, 1, "driver1");
    client.insert(writeRecords, newCommitTime);
    writeRecords = getHoodieRecords(newCommitTime, 1, "driver2");
    client.insert(writeRecords, newCommitTime);
    client.close();

    Properties hiveProps = getAwsProperties();
    addMetaSyncProps(hiveProps, parts);

    AwsGlueCatalogSyncTool awsGlueCatalogSyncTool = new AwsGlueCatalogSyncTool(hiveProps, hadoopConf);
    awsGlueCatalogSyncTool.initSyncClient(new HiveSyncConfig(hiveProps));
    awsGlueCatalogSyncTool.syncHoodieTable();
    awsGlueCatalogSyncTool.close();
    GlueAsyncClient testclient = getGlueAsyncClient();

    GetDatabaseResponse db = testclient.getDatabase(GetDatabaseRequest.builder().name(DB_NAME).build()).get();
    Assertions.assertTrue(db.database().name().equals(DB_NAME));
    GetTableResponse tbl = testclient.getTable(GetTableRequest.builder().databaseName(DB_NAME).name(TABLE_NAME).build()).get();
    Assertions.assertTrue(tbl.table().name().equals(TABLE_NAME));

    GetPartitionsResponse partitions = testclient.getPartitions(GetPartitionsRequest.builder().databaseName(DB_NAME).tableName(TABLE_NAME).build()).get();
    Assertions.assertEquals(2, partitions.partitions().size());
  }
}
