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

import org.apache.hudi.sync.common.model.FieldSchema;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for AWS Glue partition pushdown functionality.
 * These tests validate that partition filtering and pushdown works correctly
 * with AWS Glue catalog using moto for AWS service mocking.
 */
public class ITTestGluePartitionPushdown extends AWSGlueIntegrationTestBase {

  private static final String TABLE_NAME = "tbl_name";
  private Column[] partitionsColumn = {
      Column.builder().name("part1").type("int").build(),
      Column.builder().name("part2").type("string").build()
  };
  private List<FieldSchema> partitionsFieldSchema = Arrays.asList(
      new FieldSchema("part1", "int"),
      new FieldSchema("part2", "string")
  );

  @BeforeEach
  public void setUpPushdownTest() throws Exception {
    // Create a test table with partitions for pushdown testing
    glueClient.createTable(CreateTableRequest.builder()
        .databaseName(getDatabaseName())
        .tableInput(TableInput.builder()
            .name(TABLE_NAME)
            .partitionKeys(partitionsColumn)
            .storageDescriptor(StorageDescriptor.builder()
                .serdeInfo(SerDeInfo.builder().serializationLibrary("").build())
                .location(getTablePath())
                .columns(Column.builder().name("col1").type("string").build())
                .build())
            .build())
        .build());
  }

  @AfterEach
  public void tearDownPushdownTest() throws Exception {
    try {
      glueClient.deleteTable(DeleteTableRequest.builder()
          .databaseName(getDatabaseName())
          .name(TABLE_NAME)
          .build());
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  /**
   * Helper method to create partitions in the test table.
   */
  private void createPartitions(String... partitions) {
    glueClient.createPartition(CreatePartitionRequest.builder()
        .databaseName(getDatabaseName())
        .tableName(TABLE_NAME)
        .partitionInput(PartitionInput.builder()
            .storageDescriptor(StorageDescriptor.builder().columns(partitionsColumn).build())
            .values(Arrays.asList(partitions))
            .build())
        .build());
  }

  @Test
  public void testEmptyPartitionShouldReturnEmpty() {
    assertEquals(0, glueSync.getPartitionsFromList(TABLE_NAME,
        Arrays.asList("1/bar")).size());
  }

  @Test
  public void testPresentPartitionShouldReturnIt() {
    createPartitions("1", "b'ar");
    assertEquals(1, glueSync.getPartitionsFromList(TABLE_NAME,
        Arrays.asList("1/b'ar", "2/foo", "1/b''ar")).size());
  }
}