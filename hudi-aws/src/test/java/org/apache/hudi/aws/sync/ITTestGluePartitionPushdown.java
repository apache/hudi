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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.sync.common.model.FieldSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.nio.file.Files;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;

@Disabled("HUDI-7475 The tests do not work. Disabling them to unblock Azure CI")
public class ITTestGluePartitionPushdown {
  // This port number must be the same as {@code moto.port} defined in pom.xml
  private static final int MOTO_PORT = 5002;
  private static final String MOTO_ENDPOINT = "http://localhost:" + MOTO_PORT;
  private static final String DB_NAME = "db_name";
  private static final String TABLE_NAME = "tbl_name";
  private String basePath;
  private String tablePath;
  private TypedProperties hiveSyncProps;
  private AWSGlueCatalogSyncClient glueSync;
  private FileSystem fileSystem;
  private Column[] partitionsColumn = {Column.builder().name("part1").type("int").build(), Column.builder().name("part2").type("string").build()};
  List<FieldSchema> partitionsFieldSchema = Arrays.asList(new FieldSchema("part1", "int"), new FieldSchema("part2", "string"));

  @BeforeEach
  public void setUp() throws Exception {
    basePath = Files.createTempDirectory("hivesynctest" + Instant.now().toEpochMilli()).toUri().toString();
    tablePath = basePath + "/" + TABLE_NAME;
    hiveSyncProps = new TypedProperties();
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), MOTO_ENDPOINT);
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), "eu-west-1");
    hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), tablePath);
    hiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key(), DB_NAME);

    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig(hiveSyncProps, new Configuration());
    fileSystem = hiveSyncConfig.getHadoopFileSystem();
    fileSystem.mkdirs(new Path(tablePath));
    StorageConfiguration<?> configuration = HadoopFSUtils.getStorageConf(new Configuration());
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .initTable(configuration, tablePath);

    glueSync = new AWSGlueCatalogSyncClient(new HiveSyncConfig(hiveSyncProps), metaClient);
    glueSync.awsGlue.createDatabase(CreateDatabaseRequest.builder().databaseInput(DatabaseInput.builder().name(DB_NAME).build()).build()).get();

    glueSync.awsGlue.createTable(CreateTableRequest.builder().databaseName(DB_NAME)
            .tableInput(TableInput.builder().name(TABLE_NAME).partitionKeys(
                            partitionsColumn)
                    .storageDescriptor(
                      StorageDescriptor.builder()
                              .serdeInfo(SerDeInfo.builder().serializationLibrary("").build())
                              .location(tablePath)
                              .columns(
                                Column.builder().name("col1").type("string").build()
                              )
                              .build())
                    .build()).build()).get();
  }

  @AfterEach
  public void teardown() throws Exception {
    glueSync.awsGlue.deleteTable(DeleteTableRequest.builder().databaseName(DB_NAME).name(TABLE_NAME).build()).get();
    glueSync.awsGlue.deleteDatabase(DeleteDatabaseRequest.builder().name(DB_NAME).build()).get();
    fileSystem.delete(new Path(tablePath), true);
  }

  private void createPartitions(String...partitions) throws ExecutionException, InterruptedException {
    glueSync.awsGlue.createPartition(CreatePartitionRequest.builder().databaseName(DB_NAME).tableName(TABLE_NAME)
            .partitionInput(PartitionInput.builder()
                    .storageDescriptor(StorageDescriptor.builder().columns(partitionsColumn).build())
                    .values(partitions).build()).build()).get();
  }

  @Test
  public void testEmptyPartitionShouldReturnEmpty() {
    Assertions.assertEquals(0, glueSync.getPartitionsFromList(TABLE_NAME,
            Arrays.asList("1/bar")).size());
  }

  @Test
  public void testPresentPartitionShouldReturnIt() throws ExecutionException, InterruptedException {
    createPartitions("1", "b'ar");
    Assertions.assertEquals(1, glueSync.getPartitionsFromList(TABLE_NAME,
            Arrays.asList("1/b'ar", "2/foo", "1/b''ar")).size());
  }
}
