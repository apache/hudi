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
import org.apache.hudi.config.GlueCatalogSyncClientConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.model.FieldSchema;

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
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.aws.testutils.GlueTestUtil.glueSyncProps;
import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
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
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, GlueTestUtil.getHiveSyncConfig(), GlueTestUtil.getMetaClient());
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
  void testCreateOrReplaceTable_TableExists() throws ExecutionException, InterruptedException {
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
    Table table = Table.builder()
        .name(tableName)
        .tableType("COPY_ON_WRITE")
        .parameters(new HashMap<>())
        .storageDescriptor(storageDescriptor)
        .databaseName(databaseName)
        .build();

    GetTableResponse tableResponse = GetTableResponse.builder()
        .table(table)
        .build();

    GetTableRequest getTableRequestForTable = GetTableRequest.builder().databaseName(databaseName).name(tableName).build();
    // Mock methods
    CompletableFuture<GetTableResponse> tableResponseFuture = CompletableFuture.completedFuture(tableResponse);
    CompletableFuture<GetTableResponse> mockTableNotFoundResponse = mock(CompletableFuture.class);
    ExecutionException executionException = new ExecutionException("failed to get table", EntityNotFoundException.builder().build());
    Mockito.when(mockTableNotFoundResponse.get()).thenThrow(executionException);

    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(mockTableNotFoundResponse);
    Mockito.when(mockAwsGlue.getTable(getTableRequestForTable)).thenReturn(tableResponseFuture).thenReturn(mockTableNotFoundResponse);
    Mockito.when(mockAwsGlue.createTable(any(CreateTableRequest.class))).thenReturn(CompletableFuture.completedFuture(CreateTableResponse.builder().build()));

    CompletableFuture<DeleteTableResponse> deleteTableResponse = CompletableFuture.completedFuture(DeleteTableResponse.builder().build());
    Mockito.when(mockAwsGlue.deleteTable(any(DeleteTableRequest.class))).thenReturn(deleteTableResponse);

    awsGlueSyncClient.createOrReplaceTable(tableName, storageSchema, inputFormatClass, outputFormatClass, serdeClass, serdeProperties, tableProperties);

    verify(mockAwsGlue, times(2)).deleteTable(any(DeleteTableRequest.class));
    verify(mockAwsGlue, times(3)).getTable(any(GetTableRequest.class));
    verify(mockAwsGlue, times(2)).createTable(any(CreateTableRequest.class));
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

  @Test
  void testMetastoreFieldSchemas() {
    String tableName = "testTable";
    List<Column> columns = Arrays.asList(GlueTestUtil.getColumn("name", "string", "person's name"),
        GlueTestUtil.getColumn("age", "int", "person's age"));
    List<Column> partitionKeys = Arrays.asList(GlueTestUtil.getColumn("city", "string", "person's city"));
    CompletableFuture<GetTableResponse> tableResponse = getTableWithDefaultProps(tableName, columns, partitionKeys);
    // mock aws glue get table call
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    List<FieldSchema> fields = awsGlueSyncClient.getMetastoreFieldSchemas(tableName);
    // verify if fields are present
    assertEquals(3, fields.size(), "Glue table schema contain 3 fields");
    assertEquals("name", fields.get(0).getName(), "glue table first column should be name");
    assertEquals("string", fields.get(0).getType(), "glue table first column type should be string");
    assertEquals("person's name", fields.get(0).getComment().get(), "glue table first column comment should person's name");
    assertEquals("age", fields.get(1).getName(), "glue table second column should be age");
    assertEquals("int", fields.get(1).getType(), "glue table second column type should be int");
    assertEquals("person's age", fields.get(1).getComment().get(), "glue table second column comment should person's age");
    assertEquals("city", fields.get(2).getName(), "glue table third column should be city");
    assertEquals("string", fields.get(2).getType(), "glue table third column type should be string");
    assertEquals("person's city", fields.get(2).getComment().get(), "glue table third column comment should be person's city");
  }

  @Test
  void testMetastoreFieldSchemas_EmptyPartitions() {
    String tableName = "testTable";
    List<Column> columns = Arrays.asList(GlueTestUtil.getColumn("name", "string", "person's name"),
        GlueTestUtil.getColumn("age", "int", "person's age"));
    CompletableFuture<GetTableResponse> tableResponse = getTableWithDefaultProps(tableName, columns, Collections.emptyList());
    // mock aws glue get table call
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    List<FieldSchema> fields = awsGlueSyncClient.getMetastoreFieldSchemas(tableName);
    // verify if fields are present
    assertEquals(2, fields.size(), "Glue table schema contain 3 fields");
    assertEquals("name", fields.get(0).getName(), "glue table first column should be name");
    assertEquals("string", fields.get(0).getType(), "glue table first column type should be string");
    assertEquals("person's name", fields.get(0).getComment().get(), "glue table first column comment should person's name");
    assertEquals("age", fields.get(1).getName(), "glue table second column should be age");
    assertEquals("int", fields.get(1).getType(), "glue table second column type should be int");
    assertEquals("person's age", fields.get(1).getComment().get(), "glue table second column comment should person's age");
  }

  @Test
  void testMetastoreFieldSchemas_ExceptionThrows() {
    String tableName = "testTable";
    // mock aws glue get table call to throw an exception
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(EntityNotFoundException.class);
    assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.getMetastoreFieldSchemas(tableName));
  }

  @Test
  void testGetTableLocation() {
    String tableName = "testTable";
    List<Column> columns = Arrays.asList(Column.builder().name("name").type("string").comment("person's name").build(),
        Column.builder().name("age").type("int").comment("person's age").build());
    CompletableFuture<GetTableResponse> tableResponse = getTableWithDefaultProps(tableName, columns, Collections.emptyList());
    // mock aws glue get table call
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    String basePath = awsGlueSyncClient.getTableLocation(tableName);
    // verify if table base path is correct
    assertEquals(glueSyncProps.get(META_SYNC_BASE_PATH.key()), basePath, "table base path should match");
  }

  @Test
  void testGetTableLocation_ThrowsException() {
    String tableName = "testTable";
    // mock aws glue get table call to throw an exception
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(EntityNotFoundException.class);
    assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.getTableLocation(tableName));
  }

  @Test
  void testUpdateTableProperties() throws ExecutionException, InterruptedException {
    String tableName = "test";
    GlueTestUtil.getColumn("name", "string", "person's name");
    List<Column> columns = Arrays.asList(GlueTestUtil.getColumn("name", "string", "person's name"),
        GlueTestUtil.getColumn("age", "int", "person's age"));
    List<Column> partitionKeys = Collections.singletonList(GlueTestUtil.getColumn("city", "string", "person's city"));
    CompletableFuture<GetTableResponse> tableResponseFuture = getTableWithDefaultProps(tableName, columns, partitionKeys);
    HashMap<String, String> newTableProperties = new HashMap<>();
    newTableProperties.put("last_commit_time_sync", "100");

    CompletableFuture<UpdateTableResponse> mockUpdateTableResponse = mock(CompletableFuture.class);
    Mockito.when(mockUpdateTableResponse.get()).thenReturn(UpdateTableResponse.builder().build());
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponseFuture);
    Mockito.when(mockAwsGlue.updateTable(any(UpdateTableRequest.class))).thenReturn(mockUpdateTableResponse);
    boolean updated = awsGlueSyncClient.updateTableProperties(tableName, newTableProperties);
    assertTrue(updated, "should return true when new parameters is not empty");
    verify(mockAwsGlue, times(1)).updateTable(any(UpdateTableRequest.class));

    Mockito.when(mockUpdateTableResponse.get()).thenThrow(new InterruptedException());
    assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.updateTableProperties(tableName, newTableProperties));
  }

  @Test
  void testTableAndDatabaseName() {
    assertEquals(GlueTestUtil.DB_NAME, awsGlueSyncClient.getDatabaseName());
    assertEquals(GlueTestUtil.TABLE_NAME, awsGlueSyncClient.getTableName());

    String dbName = "test_db1";
    String tableName = "test_table1";
    Properties properties = new Properties();
    properties.setProperty(GlueCatalogSyncClientConfig.GLUE_SYNC_DATABASE_NAME.key(), dbName);
    properties.setProperty(GlueCatalogSyncClientConfig.GLUE_SYNC_TABLE_NAME.key(), tableName);

    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig(properties);
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(dbName, awsGlueSyncClient.getDatabaseName());
    assertEquals(tableName, awsGlueSyncClient.getTableName());

    dbName = "test_db2";
    tableName = "test_table2";
    properties = new Properties();
    properties.setProperty(META_SYNC_DATABASE_NAME.key(), dbName);
    properties.setProperty(META_SYNC_TABLE_NAME.key(), tableName);

    hiveSyncConfig = new HiveSyncConfig(properties);
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(dbName, awsGlueSyncClient.getDatabaseName());
    assertEquals(tableName, awsGlueSyncClient.getTableName());

    dbName = "test_db3";
    tableName = "test_table3";
    properties = new Properties();
    properties.setProperty(DATABASE_NAME.key(), dbName);
    properties.setProperty(HOODIE_TABLE_NAME_KEY, tableName);

    hiveSyncConfig = new HiveSyncConfig(properties);
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(dbName, awsGlueSyncClient.getDatabaseName());
    assertEquals(tableName, awsGlueSyncClient.getTableName());

    hiveSyncConfig = new HiveSyncConfig(new Properties());
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(META_SYNC_DATABASE_NAME.defaultValue(), awsGlueSyncClient.getDatabaseName());
    assertEquals(META_SYNC_TABLE_NAME.defaultValue(), awsGlueSyncClient.getTableName());
  }

  private CompletableFuture<GetTableResponse> getTableWithDefaultProps(String tableName, List<Column> columns, List<Column> partitionColumns) {
    String databaseName = "testdb";
    String inputFormatClass = "inputFormat";
    String outputFormatClass = "outputFormat";
    String serdeClass = "serde";
    HashMap<String, String> serdeProperties = new HashMap<>();
    HashMap<String, String> tableProperties = new HashMap<>();
    software.amazon.awssdk.services.glue.model.StorageDescriptor storageDescriptor = software.amazon.awssdk.services.glue.model.StorageDescriptor.builder()
        .serdeInfo(SerDeInfo.builder().serializationLibrary(serdeClass).parameters(serdeProperties).build())
        .inputFormat(inputFormatClass)
        .location(glueSyncProps.getString(META_SYNC_BASE_PATH.key()))
        .columns(columns)
        .outputFormat(outputFormatClass)
        .build();
    Table table = Table.builder()
        .name(tableName)
        .tableType("COPY_ON_WRITE")
        .parameters(new HashMap<>())
        .storageDescriptor(storageDescriptor)
        .partitionKeys(partitionColumns)
        .parameters(tableProperties)
        .databaseName(databaseName)
        .build();
    GetTableResponse response = GetTableResponse.builder()
        .table(table)
        .build();
    return CompletableFuture.completedFuture(response);
  }
}
