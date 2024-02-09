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
import org.apache.hudi.aws.credentials.HoodieAWSCredentialsProviderFactory;
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
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.index.HoodieIndex;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;

public class ITTestAwsGlueCatalogSyncTool {

  private static final String MOTO_ENDPOINT = "http://localhost:5000";
  private static final String DB_NAME = "db_name";
  private static final String TABLE_NAME = "tbl_name";
  @Test
  public void testJavaClient() throws IOException, ExecutionException, InterruptedException, URISyntaxException {
    String tablePath = "file:///tmp/hoodie/sample-table";
    Configuration hadoopConf = new Configuration();
    String tableType = "COPY_ON_WRITE";
    String tableName = "hoodie_rt";
    String parts = "driver";
    HoodieTableMetaClient.withPropertyBuilder()
              .setTableType(tableType)
              .setTableName(tableName)
            .setPartitionFields(parts)
              .setPayloadClassName(HoodieAvroPayload.class.getName())
              .initTable(hadoopConf, tablePath);

    String schema = HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA;
    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
            .withSchema(schema).withParallelism(2, 2)
            .withEmbeddedTimelineServerEnabled(false)
            .withDeleteParallelism(2).forTable(tableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
    HoodieJavaWriteClient<HoodieAvroPayload> client =
            new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
    String newCommitTime = client.startCommit();
    HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();
    List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
    List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
    List<HoodieRecord<HoodieAvroPayload>> writeRecords =
            recordsSoFar.stream().map(r -> new HoodieAvroRecord<>(r)).collect(Collectors.toList());
    client.insert(writeRecords, newCommitTime);
    client.close();
    Properties hiveProps = new TypedProperties();
    hiveProps.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "dummy");
    hiveProps.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), "dummy");
    hiveProps.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), "dummy");
    hiveProps.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), MOTO_ENDPOINT);
    hiveProps.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), "eu-west-1");
    hiveProps.setProperty(META_SYNC_BASE_PATH.key(), tablePath);
    hiveProps.setProperty(META_SYNC_DATABASE_NAME.key(), DB_NAME);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_DATABASE_NAME.key(), DB_NAME);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_TABLE_NAME.key(), TABLE_NAME);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_BASE_PATH.key(), tablePath);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), parts);
    AwsGlueCatalogSyncTool awsGlueCatalogSyncTool = new AwsGlueCatalogSyncTool(hiveProps, hadoopConf);
    awsGlueCatalogSyncTool.initSyncClient(new HiveSyncConfig(hiveProps));
    awsGlueCatalogSyncTool.syncHoodieTable();
    awsGlueCatalogSyncTool.close();
    GlueAsyncClient testclient = GlueAsyncClient.builder()
            .credentialsProvider(HoodieAWSCredentialsProviderFactory.getAwsCredentialsProvider(hiveProps))
            .endpointOverride(new URI(MOTO_ENDPOINT))
            .region(Region.of("eu-west-1"))
            .build();

    GetDatabaseResponse db = testclient.getDatabase(GetDatabaseRequest.builder().name(DB_NAME).build()).get();
    Assertions.assertTrue(db.database().name().equals(DB_NAME));
    GetTableResponse tbl = testclient.getTable(GetTableRequest.builder().databaseName(DB_NAME).name(TABLE_NAME).build()).get();
    Assertions.assertTrue(tbl.table().name().equals(TABLE_NAME));

    GetPartitionsResponse partitions = testclient.getPartitions(GetPartitionsRequest.builder().databaseName(DB_NAME).tableName(TABLE_NAME).build()).get();
    Assertions.assertEquals(3, partitions.partitions().size());
  }
}
