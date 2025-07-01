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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.GlueCatalogSyncClientConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueServiceClientConfiguration;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TagResourceRequest;
import software.amazon.awssdk.services.glue.model.TagResourceResponse;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.aws.testutils.GlueTestUtil.glueSyncProps;
import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestAWSGlueSyncClient {
  private static final String CATALOG_ID = "DEFAULT_AWS_ACCOUNT_ID";

  @Mock
  private GlueAsyncClient mockAwsGlue;
  @Mock
  private StsClient mockSts;

  private AWSGlueCatalogSyncClient awsGlueSyncClient;

  @BeforeEach
  void setUp() throws IOException {
    GlueTestUtil.setUp();
    when(mockSts.getCallerIdentity(GetCallerIdentityRequest.builder().build())).thenReturn(GetCallerIdentityResponse.builder().account(CATALOG_ID).build());
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, GlueTestUtil.getHiveSyncConfig(), GlueTestUtil.getMetaClient());
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

    GetTableRequest getTableRequestForTable = GetTableRequest.builder().catalogId(CATALOG_ID).databaseName(databaseName).name(tableName).build();
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTableLocationUsingCatalogId(boolean useConfiguredCatalogId) {
    String catalogId = useConfiguredCatalogId ? UUID.randomUUID().toString() : CATALOG_ID;
    TypedProperties properties = GlueTestUtil.getHiveSyncConfig().getProps();
    if (useConfiguredCatalogId) {
      properties.setProperty(GlueCatalogSyncClientConfig.GLUE_CATALOG_ID.key(), catalogId);
    }
    when(mockSts.getCallerIdentity(GetCallerIdentityRequest.builder().build())).thenReturn(GetCallerIdentityResponse.builder().account(CATALOG_ID).build());
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, new HiveSyncConfig(properties), GlueTestUtil.getMetaClient());

    String testdb = "testdb";
    String tableName = "testTable";
    List<Column> columns = Arrays.asList(Column.builder().name("name").type("string").comment("person's name").build(),
        Column.builder().name("age").type("int").comment("person's age").build());
    CompletableFuture<GetTableResponse> tableResponse = getTableWithDefaultProps(tableName, columns, Collections.emptyList());
    // mock aws glue get table call
    GetTableRequest getTableRequestForTable = GetTableRequest.builder().catalogId(catalogId).databaseName(testdb).name(tableName).build();
    Mockito.when(mockAwsGlue.getTable(getTableRequestForTable)).thenReturn(tableResponse);
    String basePath = awsGlueSyncClient.getTableLocation(tableName);
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
  void testCreateDatabase_WhenDatabaseAlreadyExists_DoesNothing() {
    String dbName = "existingDb";

    CompletableFuture<GetDatabaseResponse> existsFuture =
        CompletableFuture.completedFuture(
            GetDatabaseResponse.builder()
                .database(Database.builder().name(dbName).build())
                .build()
        );
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(existsFuture);
    awsGlueSyncClient.createDatabase(dbName);
    verify(mockAwsGlue, never()).createDatabase(any(CreateDatabaseRequest.class));
  }

  @Test
  void testCreateDatabase_WhenDatabaseDoesNotExist_CreatesDatabase() throws Exception {
    String dbName = "newDb";

    CompletableFuture<GetDatabaseResponse> notFoundFuture = mock(CompletableFuture.class);
    ExecutionException notFoundEx =
        new ExecutionException(EntityNotFoundException.builder().build());
    when(notFoundFuture.get()).thenThrow(notFoundEx);
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(notFoundFuture);

    CompletableFuture<CreateDatabaseResponse> createFuture =
        CompletableFuture.completedFuture(CreateDatabaseResponse.builder().build());
    when(mockAwsGlue.createDatabase(any(CreateDatabaseRequest.class))).thenReturn(createFuture);
    GlueServiceClientConfiguration mockConfig = mock(GlueServiceClientConfiguration.class);
    when(mockAwsGlue.serviceClientConfiguration()).thenReturn(mockConfig);
    when(mockConfig.region()).thenReturn(Region.US_EAST_1);
    awsGlueSyncClient.createDatabase(dbName);
    verify(mockAwsGlue).createDatabase(argThat((CreateDatabaseRequest req) ->
        req.catalogId().equals(CATALOG_ID)
            && req.databaseInput().name().equals(dbName)
    ));
  }

  @Test
  void testGetAllPartitions_SinglePage() {
    String tableName = "tbl";
    StorageDescriptor sd1 = StorageDescriptor.builder().location("s3://loc1").build();
    StorageDescriptor sd2 = StorageDescriptor.builder().location("s3://loc2").build();
    software.amazon.awssdk.services.glue.model.Partition awsPart1 = software.amazon.awssdk.services.glue.model.Partition.builder()
        .values("2025","05","19").storageDescriptor(sd1).build();
    software.amazon.awssdk.services.glue.model.Partition awsPart2 = software.amazon.awssdk.services.glue.model.Partition.builder()
        .values("2025","05","18").storageDescriptor(sd2).build();

    GetPartitionsResponse page = GetPartitionsResponse.builder()
        .partitions(awsPart1, awsPart2)
        .nextToken(null)
        .build();
    CompletableFuture<GetPartitionsResponse> future = CompletableFuture.completedFuture(page);
    when(mockAwsGlue.getPartitions(any(GetPartitionsRequest.class))).thenReturn(future);

    List<Partition> result = awsGlueSyncClient.getAllPartitions(tableName);

    assertEquals(2, result.size());
    assertEquals("s3://loc1", result.get(0).getStorageLocation());
    assertEquals(Arrays.asList("2025","05","19"), result.get(0).getValues());
    assertEquals("s3://loc2", result.get(1).getStorageLocation());
    verify(mockAwsGlue, times(1)).getPartitions(any(GetPartitionsRequest.class));
  }

  @Test
  void testGetAllPartitions_MultiplePages() {
    String tableName = "tbl";

    StorageDescriptor sd1 = StorageDescriptor.builder().location("s3://first").build();
    software.amazon.awssdk.services.glue.model.Partition awsPart1 =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("A")
            .storageDescriptor(sd1)
            .build();
    GetPartitionsResponse page1 = GetPartitionsResponse.builder()
        .partitions(awsPart1)
        .nextToken("tok1")
        .build();

    StorageDescriptor sd2 = StorageDescriptor.builder().location("s3://second").build();
    software.amazon.awssdk.services.glue.model.Partition awsPart2 =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("B")
            .storageDescriptor(sd2)
            .build();
    GetPartitionsResponse page2 = GetPartitionsResponse.builder()
        .partitions(awsPart2)
        .nextToken(null)
        .build();

    when(mockAwsGlue.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(page1))
        .thenReturn(CompletableFuture.completedFuture(page2));

    List<Partition> result = awsGlueSyncClient.getAllPartitions(tableName);

    assertEquals(2, result.size());
    assertEquals("s3://first",  result.get(0).getStorageLocation());
    assertEquals(Arrays.asList("A"), result.get(0).getValues());
    assertEquals("s3://second", result.get(1).getStorageLocation());
    assertEquals(Arrays.asList("B"), result.get(1).getValues());

    verify(mockAwsGlue, times(2)).getPartitions(any(GetPartitionsRequest.class));
  }

  @Test
  void testGetAllPartitions_ThrowsWrappedException() {
    String tableName = "tbl";

    CompletableFuture<GetPartitionsResponse> badFuture = mock(CompletableFuture.class);
    try {
      when(badFuture.get()).thenThrow(new ExecutionException(new RuntimeException("boom")));
    } catch (InterruptedException | ExecutionException e) {
      // won't actually happen in stub setup
    }
    when(mockAwsGlue.getPartitions(any(GetPartitionsRequest.class))).thenReturn(badFuture);

    HoodieGlueSyncException ex = assertThrows(
        HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.getAllPartitions(tableName)
    );
    assertTrue(ex.getMessage().contains("Failed to get all partitions for table"));
  }

  @Test
  void testDatabaseExists_WhenDatabaseExists() {
    String dbName = "db";
    GetDatabaseResponse resp = GetDatabaseResponse.builder()
        .database(Database.builder().name(dbName).build())
        .build();
    CompletableFuture<GetDatabaseResponse> successFuture = CompletableFuture.completedFuture(resp);
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(successFuture);

    boolean exists = awsGlueSyncClient.databaseExists(dbName);

    assertTrue(exists, "Expected databaseExists to return true when AWS Glue returns a Database");
  }

  @Test
  void testDatabaseExists_WhenNotExists() throws ExecutionException, InterruptedException {
    String dbName = "db";
    CompletableFuture<GetDatabaseResponse> notFoundFuture = mock(CompletableFuture.class);
    when(notFoundFuture.get()).thenThrow(new ExecutionException(EntityNotFoundException.builder().build()));
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(notFoundFuture);

    boolean exists = awsGlueSyncClient.databaseExists(dbName);

    assertFalse(exists, "Expected databaseExists to return false when AWS Glue signals EntityNotFound");
    verify(mockAwsGlue).getDatabase(any(GetDatabaseRequest.class));
  }

  @Test
  void testDatabaseExists_WhenThrowsOtherException() throws ExecutionException, InterruptedException {
    String dbName = "db";
    CompletableFuture<GetDatabaseResponse> errorFuture = mock(CompletableFuture.class);
    when(errorFuture.get()).thenThrow(new ExecutionException(new RuntimeException("boom")));
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(errorFuture);

    HoodieGlueSyncException ex = assertThrows(
        HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.databaseExists(dbName),
        "Expected a HoodieGlueSyncException when AWS Glue throws a non-EntityNotFound error"
    );
    assertTrue(ex.getMessage().contains("Fail to check if database exists"),
        "Exception message should indicate database existence check failure");
  }

  @Test
  void testAddPartitionsToTable_NoPartitions() {
    // empty list -> no calls
    awsGlueSyncClient.addPartitionsToTable("tbl", Collections.emptyList());
    verify(mockAwsGlue, never()).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  void testAddPartitionsToTable_Success() {
    String tableName = "tbl";
    List<String> parts = Arrays.asList("2025/05/20", "2025/05/19");

    // stub getTable to return a dummy StorageDescriptor
    StorageDescriptor baseSd = StorageDescriptor.builder().location("s3://base").build();
    Table table = Table.builder().name(tableName).storageDescriptor(baseSd).build();
    GetTableResponse gt = GetTableResponse.builder().table(table).build();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(gt));

    // stub batchCreatePartition to succeed with no errors
    BatchCreatePartitionResponse ok = BatchCreatePartitionResponse.builder().errors(Collections.emptyList()).build();
    when(mockAwsGlue.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(ok));

    awsGlueSyncClient.addPartitionsToTable(tableName, parts);

    // capture the request
    ArgumentCaptor<BatchCreatePartitionRequest> cap = ArgumentCaptor.forClass(BatchCreatePartitionRequest.class);
    verify(mockAwsGlue).batchCreatePartition(cap.capture());
    BatchCreatePartitionRequest req = cap.getValue();
    assertEquals(TestAWSGlueSyncClient.CATALOG_ID, req.catalogId());
    assertEquals(tableName, req.tableName());
    assertEquals(parts.size(), req.partitionInputList().size());
  }

  @Test
  void testAddPartitionsToTable_AlreadyExistsErrors() {
    String tableName = "tbl";
    List<String> parts = Arrays.asList("2025/05/20");

    // stub getTable
    StorageDescriptor baseSd = StorageDescriptor.builder().location("s3://base").build();
    Table table = Table.builder().name(tableName).storageDescriptor(baseSd).build();
    GetTableResponse gt = GetTableResponse.builder().table(table).build();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(gt));

    // stub an error list with AlreadyExistsException
    ErrorDetail detail = ErrorDetail.builder().errorCode("AlreadyExistsException").build();
    PartitionError pe = PartitionError.builder().errorDetail(detail).build();
    BatchCreatePartitionResponse resp = BatchCreatePartitionResponse.builder()
        .errors(Collections.singletonList(pe))
        .build();
    when(mockAwsGlue.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(resp));

    // should swallow the AlreadyExists error and not throw
    awsGlueSyncClient.addPartitionsToTable(tableName, parts);

    verify(mockAwsGlue).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  void testUpdatePartitionsToTable_NoPartitions() {
    awsGlueSyncClient.updatePartitionsToTable("tbl", Collections.emptyList());
    verify(mockAwsGlue, never()).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
  }

  @Test
  void testUpdatePartitionsToTable_Success() {
    String tableName = "tbl";
    List<String> changed = Arrays.asList("2025/05/20");

    StorageDescriptor baseSd = StorageDescriptor.builder().location("s3://base").build();
    Table table = Table.builder().name(tableName).storageDescriptor(baseSd).build();
    GetTableResponse gt = GetTableResponse.builder().table(table).build();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(gt));

    BatchUpdatePartitionResponse ok = BatchUpdatePartitionResponse.builder().errors(Collections.emptyList()).build();
    when(mockAwsGlue.batchUpdatePartition(any(BatchUpdatePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(ok));

    awsGlueSyncClient.updatePartitionsToTable(tableName, changed);

    verify(mockAwsGlue).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
  }

  @Test
  void testUpdatePartitionsToTable_ErrorResponses() {
    String tableName = "tbl";
    List<String> changed = Arrays.asList("year=2025");

    StorageDescriptor baseSd = StorageDescriptor.builder().location("s3://base").build();
    Table table = Table.builder().storageDescriptor(baseSd).build();
    GetTableResponse gt = GetTableResponse.builder().table(table).build();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(gt));
    HoodieGlueSyncException ex = assertThrows(
        HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updatePartitionsToTable(tableName, changed)
    );
    // The exception is wrapped by the parallelizeChange method, so check the root cause
    assertTrue(ex.getMessage().contains("Failed to parallelize operation"));
    assertTrue(ex.getCause() != null && ex.getCause().getCause() != null);
    assertTrue(ex.getCause().getCause().getMessage().contains("Fail to update partitions"));
  }

  @Test
  void testDropPartitions_NoPartitions() {
    awsGlueSyncClient.dropPartitions("tbl", Collections.emptyList());
    verify(mockAwsGlue, never()).batchDeletePartition(any(BatchDeletePartitionRequest.class));
  }

  @Test
  void testDropPartitions_Success() {
    String tableName = "tbl";
    List<String> toDrop = Arrays.asList("2025/05/19");

    BatchDeletePartitionResponse ok = BatchDeletePartitionResponse.builder().errors(Collections.emptyList()).build();
    when(mockAwsGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(ok));

    awsGlueSyncClient.dropPartitions(tableName, toDrop);

    verify(mockAwsGlue).batchDeletePartition(any(BatchDeletePartitionRequest.class));
  }

  @Test
  void testDropPartitions_ErrorResponses() {
    String tableName = "tbl";
    List<String> toDrop = Arrays.asList("2025/05/19");

    // stub a non-empty error list
    ErrorDetail detail = ErrorDetail.builder().errorCode("Boom").build();
    PartitionError pe = PartitionError.builder().errorDetail(detail).build();
    BatchDeletePartitionResponse resp = BatchDeletePartitionResponse.builder()
        .errors(Collections.singletonList(pe))
        .build();
    when(mockAwsGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(resp));

    HoodieGlueSyncException ex = assertThrows(
        HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.dropPartitions(tableName, toDrop)
    );
    // The exception is wrapped by the parallelizeChange method, so check the root cause
    assertTrue(ex.getMessage().contains("Failed to parallelize operation"));
    assertTrue(ex.getCause() != null && ex.getCause().getCause() != null);
    assertTrue(ex.getCause().getCause().getMessage().contains("Fail to drop partitions"));
  }

  @Disabled("Integration test – requires real AWS environment")
  @Test
  void testIntegrationTableExists_RealGlueEnvironment() {
    // Use us-west-2 and testing_acme-dev
    String dbName = "acme_default";
    String tblName = "hudi_table";

    HiveSyncConfig config = new HiveSyncConfig(new Properties());
    config.setValue(META_SYNC_DATABASE_NAME, dbName);
    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    AWSGlueCatalogSyncClient client = new AWSGlueCatalogSyncClient(config, metaClient);
    assertTrue(client.tableExists(tblName),
        "Expected tableExists(...) to be true for an existing table in Glue");

    String randomTable = "none_" + UUID.randomUUID().toString().replace("-", "");
    assertFalse(client.tableExists(randomTable),
        "Expected tableExists(...) to be false for a non-existent table");
    client.close();
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
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(dbName, awsGlueSyncClient.getDatabaseName());
    assertEquals(tableName, awsGlueSyncClient.getTableName());

    dbName = "test_db2";
    tableName = "test_table2";
    properties = new Properties();
    properties.setProperty(META_SYNC_DATABASE_NAME.key(), dbName);
    properties.setProperty(META_SYNC_TABLE_NAME.key(), tableName);

    hiveSyncConfig = new HiveSyncConfig(properties);
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(dbName, awsGlueSyncClient.getDatabaseName());
    assertEquals(tableName, awsGlueSyncClient.getTableName());

    dbName = "test_db3";
    tableName = "test_table3";
    properties = new Properties();
    properties.setProperty(DATABASE_NAME.key(), dbName);
    properties.setProperty(HOODIE_TABLE_NAME_KEY, tableName);

    hiveSyncConfig = new HiveSyncConfig(properties);
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(dbName, awsGlueSyncClient.getDatabaseName());
    assertEquals(tableName, awsGlueSyncClient.getTableName());

    hiveSyncConfig = new HiveSyncConfig(new Properties());
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, hiveSyncConfig, GlueTestUtil.getMetaClient());
    assertEquals(META_SYNC_DATABASE_NAME.defaultValue(), awsGlueSyncClient.getDatabaseName());
    assertEquals(META_SYNC_TABLE_NAME.defaultValue(), awsGlueSyncClient.getTableName());
  }

  @Test
  void testResourceTagging() throws ExecutionException, InterruptedException {
    // Setup test properties with resource tags
    TypedProperties props = GlueTestUtil.getHiveSyncConfig().getProps();
    props.setProperty(GlueCatalogSyncClientConfig.GLUE_SYNC_RESOURCE_TAGS.key(), "CostCenter:SomeCenter,Environment:Production");
    
    when(mockSts.getCallerIdentity(GetCallerIdentityRequest.builder().build()))
        .thenReturn(GetCallerIdentityResponse.builder().account(CATALOG_ID).build());
    
    // Mock the service configuration and region using deep nested mocks
    GlueServiceClientConfiguration mockConfig = mock(GlueServiceClientConfiguration.class);
    when(mockAwsGlue.serviceClientConfiguration()).thenReturn(mockConfig);
    when(mockConfig.region()).thenReturn(Region.US_EAST_1);
    
    AWSGlueCatalogSyncClient clientWithTags = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, 
        new HiveSyncConfig(props), GlueTestUtil.getMetaClient());

    // Mock table does not exist (for createTable to proceed)
    CompletableFuture<GetTableResponse> tableNotFoundFuture = mock(CompletableFuture.class);
    ExecutionException tableNotFoundEx = new ExecutionException(EntityNotFoundException.builder().build());
    when(tableNotFoundFuture.get()).thenThrow(tableNotFoundEx);
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableNotFoundFuture);
    
    // Mock successful createTable response
    when(mockAwsGlue.createTable(any(CreateTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(CreateTableResponse.builder().build()));
    
    // Mock successful tagResource response
    when(mockAwsGlue.tagResource(any(TagResourceRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(TagResourceResponse.builder().build()));
    
    // Mock database does not exist (for createDatabase to proceed)
    CompletableFuture<GetDatabaseResponse> dbNotFoundFuture = mock(CompletableFuture.class);
    ExecutionException dbNotFoundEx = new ExecutionException(EntityNotFoundException.builder().build());
    when(dbNotFoundFuture.get()).thenThrow(dbNotFoundEx);
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(dbNotFoundFuture);
    
    when(mockAwsGlue.createDatabase(any(CreateDatabaseRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(CreateDatabaseResponse.builder().build()));

    // Test table creation with tagging
    String tableName = "test_table";
    MessageType storageSchema = GlueTestUtil.getSimpleSchema();
    clientWithTags.createTable(tableName, storageSchema, "inputFormat", "outputFormat", 
        "serdeClass", new HashMap<>(), new HashMap<>());

    // Test database creation with tagging
    String dbName = "test_db";
    clientWithTags.createDatabase(dbName);

    // Verify tagResource was called once (only for database, table tagging was removed)
    ArgumentCaptor<TagResourceRequest> tagCaptor = ArgumentCaptor.forClass(TagResourceRequest.class);
    verify(mockAwsGlue, times(1)).tagResource(tagCaptor.capture());
    
    List<TagResourceRequest> tagRequests = tagCaptor.getAllValues();
    
    // Verify database tagging (table tagging functionality was removed)
    TagResourceRequest dbTagRequest = tagRequests.get(0);
    assertTrue(dbTagRequest.resourceArn().contains("database"));
    assertTrue(dbTagRequest.resourceArn().contains(dbName));
    assertEquals("SomeCenter", dbTagRequest.tagsToAdd().get("CostCenter"));
    assertEquals("Production", dbTagRequest.tagsToAdd().get("Environment"));
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
