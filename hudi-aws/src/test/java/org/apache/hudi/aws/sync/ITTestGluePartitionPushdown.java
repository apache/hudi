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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;

@Disabled("HUDI-7475 The tests do not work. Disabling them to unblock Azure CI")
public class ITTestGluePartitionPushdown {

  private static final String MOTO_ENDPOINT = "http://localhost:5010";
  private static final String DB_NAME = "db_name";
  private static final String TABLE_NAME = "tbl_name";
  private String basePath = Files.createTempDirectory("hivesynctest" + Instant.now().toEpochMilli()).toUri().toString();
  private String tablePath = basePath + "/" + TABLE_NAME;
  private TypedProperties hiveSyncProps;
  private AWSGlueCatalogSyncClient glueSync;
  private FileSystem fileSystem;
  private Column[] partitionsColumn = {Column.builder().name("part1").type("int").build(), Column.builder().name("part2").type("string").build()};

  public ITTestGluePartitionPushdown() throws IOException {

  }

  @BeforeEach
  public void setUp() throws Exception {
    hiveSyncProps = new TypedProperties();
//    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_ACCESS_KEY.key(), "dummy");
//    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_SECRET_KEY.key(), "dummy");
//    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_SESSION_TOKEN.key(), "dummy");
    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), MOTO_ENDPOINT);
//    hiveSyncProps.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), "us-east-1");
    hiveSyncProps.setProperty(META_SYNC_BASE_PATH.key(), tablePath);
    hiveSyncProps.setProperty(META_SYNC_DATABASE_NAME.key(), DB_NAME);

    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig(hiveSyncProps, new Configuration());
    fileSystem = hiveSyncConfig.getHadoopFileSystem();
    fileSystem.mkdirs(new Path(tablePath));
    Configuration configuration = new Configuration();
    HoodieTableMetaClient.withPropertyBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE)
            .setTableName(TABLE_NAME)
            .setPayloadClass(HoodieAvroPayload.class)
            .initTable(configuration, tablePath);

    glueSync = new AWSGlueCatalogSyncClient(new HiveSyncConfig(hiveSyncProps));
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

  private List<String> createPartitions(int amount, int partitionSize, boolean createInGlue) throws ExecutionException, InterruptedException {
    Set<String[]> partitions = IntStream.range(0, amount)
        .mapToObj(i -> IntStream.range(0, partitionSize)
            .mapToObj(j -> String.valueOf(ThreadLocalRandom.current().nextInt(100000)))
            .toArray(String[]::new))
        .collect(Collectors.toSet());
    List<String[]> partitionsList = new ArrayList<>(partitions);
    if (createInGlue) {
      for (List<String[]> inp : CollectionUtils.batches(partitionsList, 1000)) {
        glueSync.awsGlue.batchCreatePartition(BatchCreatePartitionRequest.builder().databaseName(DB_NAME).tableName(TABLE_NAME)
            .partitionInputList(
                inp.stream().map(
                    inp1 -> PartitionInput
                        .builder()
                        .storageDescriptor(StorageDescriptor.builder().columns(partitionsColumn).build())
                        .values(inp1).build()).collect(Collectors.toList()))
            .build()).get();
      }
    }
    return partitionsList.stream().map(partition -> String.join("/", partition)).collect(Collectors.toList());
  }

  @Test
  public void testEmptyPartitionShouldReturnEmpty() {
    Assertions.assertEquals(0, glueSync.getPartitionsFromList(TABLE_NAME,
            Arrays.asList("1/bar")).size());
  }

  @Test
  public void testPresentPartitionShouldReturnIt() throws ExecutionException, InterruptedException {
    List<String> partitions = createPartitions(1, 2, true);
    Assertions.assertEquals(1, glueSync.getPartitionsFromList(TABLE_NAME,
            Arrays.asList(partitions.get(0), "2/foo", "1/b''ar")).size());
  }

  @Test
  public void testCreatingManyPartitionsAndThenReadingAllShouldShowAll() throws ExecutionException, InterruptedException {
    List<String> partitions = createPartitions(2000, 2, false);
    glueSync.addPartitionsToTable(TABLE_NAME, partitions);
    Assertions.assertEquals(partitions.size(), glueSync.getAllPartitions(TABLE_NAME).size());
  }

  @Test
  public void testDeletingManyPartitions() throws ExecutionException, InterruptedException {
    List<String> partitions = createPartitions(2000, 2, true);
    int firstDeleteBatch = 1000;
    glueSync.dropPartitions(TABLE_NAME, partitions.subList(0, firstDeleteBatch));
    Assertions.assertEquals(partitions.size() - 1000, glueSync.getAllPartitions(TABLE_NAME).size());
    glueSync.dropPartitions(TABLE_NAME, partitions.subList(firstDeleteBatch, partitions.size()));
    Assertions.assertEquals(0, glueSync.getAllPartitions(TABLE_NAME).size());
  }

  @Test
  public void testReadFromList() throws ExecutionException, InterruptedException {
    List<String> existingPartitions = createPartitions(1000, 2, true);
    List<String> fakePartitions = createPartitions(1000, 2, false);
    Assertions.assertEquals(existingPartitions.size(), glueSync.getPartitionsFromList(
        TABLE_NAME,
        Stream.concat(existingPartitions.stream(), fakePartitions.stream())
            .collect(Collectors.toList())
    ).size());
    Assertions.assertEquals(300, glueSync.getPartitionsFromList(
        TABLE_NAME,
        existingPartitions.subList(0, 300)
    ).size());
  }
}
