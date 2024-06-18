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

import org.apache.hudi.aws.testutils.GlueTestUtil;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TestAWSGlueSyncClient {

  @Mock
  private GlueAsyncClient mockAwsGlue;

  private AWSGlueCatalogSyncClient awsGlueSyncClient;

  @BeforeEach
  void setUp() throws IOException {
    GlueTestUtil.setUp();
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, GlueTestUtil.getHiveSyncConfig());
  }

  @AfterEach
  void clear() throws IOException {
    GlueTestUtil.clear();
  }

  @AfterAll
  static void cleanUp() throws IOException {
    GlueTestUtil.teardown();
  }

  @Test
  void testCreateOrReplaceTable_TableExists() {
    String tableName = "testTable";
    String databaseName = "testdb";
    String inputFormatClass = "inputFormat";
    MessageType storageSchema = GlueTestUtil.getSimpleSchema();
    String outputFormatClass = "outputFormat";
    String serdeClass = "serde";
    HashMap<String, String> serdeProperties = new HashMap<>();
    HashMap<String, String> tableProperties = new HashMap<>();
    software.amazon.awssdk.services.glue.model.StorageDescriptor storageDescriptor = software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
        .serdeInfo(SerDeInfo.builder().serializationLibrary(serdeClass).parameters(serdeProperties).build())
        .inputFormat(inputFormatClass)
        .outputFormat(outputFormatClass)
        .build();
    Table tempTable = Table.builder()
        .name("tempTable")
        .tableType("COPY_ON_WRITE")
        .parameters(new HashMap<>())
        .storageDescriptor(storageDescriptor)
        .databaseName(databaseName)
        .build();
    GetTableResponse response = GetTableResponse.builder()
        .table(tempTable)
        .build();

    // Mock methods
    CompletableFuture<GetTableResponse> tableResponse = CompletableFuture.completedFuture(response);
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);

    CompletableFuture<DeleteTableResponse> deleteTableResponse = CompletableFuture.completedFuture(DeleteTableResponse.builder().build());
    Mockito.when(mockAwsGlue.deleteTable(any(DeleteTableRequest.class))).thenReturn(deleteTableResponse);

    awsGlueSyncClient.createOrReplaceTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);

    // Verify that awsGlue.updateTable() is called exactly once
    verify(mockAwsGlue, times(1)).updateTable(any(UpdateTableRequest.class));
    verify(mockAwsGlue, times(0)).createTable(any(CreateTableRequest.class));
    verify(mockAwsGlue, times(1)).deleteTable(any(DeleteTableRequest.class));
  }

  @Test
  void testCreateOrReplaceTable_TableDoesNotExist() {
    String tableName = "testTable";
    MessageType storageSchema = GlueTestUtil.getSimpleSchema();
    String inputFormatClass = "inputFormat";
    String outputFormatClass = "outputFormat";
    String serdeClass = "serde";
    HashMap<String, String> serdeProperties = new HashMap<>();
    HashMap<String, String> tableProperties = new HashMap<>();

    // Mock methods
    CompletableFuture<GetTableResponse> tableResponse = CompletableFuture.completedFuture(GetTableResponse.builder().build());
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    CompletableFuture<CreateTableResponse> createTableResponse = CompletableFuture.completedFuture(CreateTableResponse.builder().build());
    Mockito.when(mockAwsGlue.createTable(any(CreateTableRequest.class))).thenReturn(createTableResponse);

    awsGlueSyncClient.createOrReplaceTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);
    // Verify that awsGlue.createTable() is called
    verify(mockAwsGlue, times(1)).createTable(any(CreateTableRequest.class));
  }

  @Test
  void testDropTable() {
    DeleteTableResponse response = DeleteTableResponse.builder().build();
    CompletableFuture<DeleteTableResponse> future = CompletableFuture.completedFuture(response);
    // mock aws glue delete table call
    Mockito.when(mockAwsGlue.deleteTable(any(DeleteTableRequest.class))).thenReturn(future);

    awsGlueSyncClient.dropTable("test");
    // verify if aws glue delete table method called once
    verify(mockAwsGlue, times(1)).deleteTable(any(DeleteTableRequest.class));
  }
}
