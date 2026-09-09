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

import org.apache.hudi.HoodieVersion;
import org.apache.hudi.aws.testutils.GlueTestUtil;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.GlueCatalogSyncClientConfig;
import org.apache.hudi.config.HoodieAWSConfig;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.SchemaDifference;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueAsyncClientBuilder;
import software.amazon.awssdk.services.glue.GlueServiceClientConfiguration;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionFailureEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreatePartitionIndexRequest;
import software.amazon.awssdk.services.glue.model.CreatePartitionIndexResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeletePartitionIndexRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionIndexResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionIndexesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionIndexesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.KeySchemaElement;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionIndex;
import software.amazon.awssdk.services.glue.model.PartitionIndexDescriptor;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.hudi.aws.testutils.GlueTestUtil.glueSyncProps;
import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.sync.common.HoodieMetaSyncOperations.HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC;
import static org.apache.hudi.sync.common.HoodieMetaSyncOperations.HOODIE_LAST_COMMIT_TIME_SYNC;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestAWSGlueSyncClient {
  private static final String CATALOG_ID = "DEFAULT_AWS_ACCOUNT_ID";
  private static final String GLUE_PARTITION_INDEX_ENABLE = "partition_filtering.enabled";

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
    HoodieSchema storageSchema = GlueTestUtil.getSimpleSchema();
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
    HoodieSchema storageSchema = GlueTestUtil.getSimpleSchema();
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
    ArgumentCaptor<BatchUpdatePartitionRequest> captor = ArgumentCaptor.forClass(BatchUpdatePartitionRequest.class);
    when(mockAwsGlue.batchUpdatePartition(captor.capture()))
        .thenReturn(CompletableFuture.completedFuture(ok));

    awsGlueSyncClient.updatePartitionsToTable(tableName, changed);

    verify(mockAwsGlue).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
    List<BatchUpdatePartitionRequestEntry> entries = captor.getValue().entries();
    assertEquals(1, entries.size());
    String syncBasePath = GlueTestUtil.getHiveSyncConfig().getString(META_SYNC_BASE_PATH);
    assertEquals(new StoragePath(syncBasePath, "2025/05/20").toString(),
        entries.get(0).partitionInput().storageDescriptor().location());
    assertEquals(Collections.singletonList("2025-05-20"), entries.get(0).partitionValueList());
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

  @Test
  void testDropPartitions_IgnoresEntityNotFound() {
    String tableName = "tbl";
    List<String> toDrop = List.of("2025/05/19");

    // Glue reports EntityNotFoundException for a partition that no longer exists; it should be ignored.
    ErrorDetail detail = ErrorDetail.builder().errorCode(EntityNotFoundException.class.getSimpleName()).build();
    PartitionError pe = PartitionError.builder().partitionValues(Arrays.asList("2025", "05", "19")).errorDetail(detail).build();
    BatchDeletePartitionResponse resp = BatchDeletePartitionResponse.builder()
        .errors(Collections.singletonList(pe))
        .build();
    when(mockAwsGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(resp));

    // should swallow the EntityNotFound error and not throw
    awsGlueSyncClient.dropPartitions(tableName, toDrop);

    verify(mockAwsGlue).batchDeletePartition(any(BatchDeletePartitionRequest.class));
  }

  @Test
  void testDropPartitions_MixedErrorsStillThrow() {
    String tableName = "tbl";
    List<String> toDrop = Arrays.asList("2025/05/19", "2025/05/18");

    // One ignorable EntityNotFound error and one real error -> should still throw for the real one.
    PartitionError ignorable = PartitionError.builder()
        .partitionValues(Arrays.asList("2025", "05", "19"))
        .errorDetail(ErrorDetail.builder().errorCode(EntityNotFoundException.class.getSimpleName()).build())
        .build();
    PartitionError real = PartitionError.builder()
        .partitionValues(Arrays.asList("2025", "05", "18"))
        .errorDetail(ErrorDetail.builder().errorCode("InternalServiceException").build())
        .build();
    BatchDeletePartitionResponse resp = BatchDeletePartitionResponse.builder()
        .errors(Arrays.asList(ignorable, real))
        .build();
    when(mockAwsGlue.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(resp));

    HoodieGlueSyncException ex = assertThrows(
        HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.dropPartitions(tableName, toDrop)
    );
    // Walk the full cause chain: the error list is nested a few wrappers deep.
    StringBuilder chain = new StringBuilder();
    for (Throwable t = ex; t != null; t = t.getCause()) {
      chain.append(t.getMessage()).append('\n');
    }
    String messages = chain.toString();
    assertTrue(messages.contains("Fail to drop partitions"));
    // Only the real error should be surfaced, not the ignored EntityNotFound one.
    assertTrue(messages.contains("InternalServiceException"));
    assertFalse(messages.contains(EntityNotFoundException.class.getSimpleName()));
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
    HoodieSchema storageSchema = GlueTestUtil.getSimpleSchema();
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

  @Test
  void testUpdateHoodieWriterVersion() throws ExecutionException, InterruptedException {
    String tableName = "test";
    List<Column> columns = Arrays.asList(
        GlueTestUtil.getColumn("name", "string", "person's name"),
        GlueTestUtil.getColumn("age", "int", "person's age"));
    List<Column> partitionKeys = Collections.singletonList(
        GlueTestUtil.getColumn("city", "string", "person's city"));
    CompletableFuture<GetTableResponse> tableResponseFuture =
        getTableWithDefaultProps(tableName, columns, partitionKeys);

    CompletableFuture<UpdateTableResponse> mockUpdateTableResponse = Mockito.mock(CompletableFuture.class);
    Mockito.when(mockUpdateTableResponse.get()).thenReturn(UpdateTableResponse.builder().build());
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponseFuture);
    Mockito.when(mockAwsGlue.updateTable(any(UpdateTableRequest.class))).thenReturn(mockUpdateTableResponse);

    awsGlueSyncClient.updateHoodieWriterVersion(tableName);

    // Capture the request to verify the writer-version parameter was set correctly
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockAwsGlue, times(1)).updateTable(captor.capture());
    UpdateTableRequest sent = captor.getValue();
    assertEquals(
        HoodieVersion.get(),
        sent.tableInput().parameters().get(HoodieVersion.HOODIE_WRITER_VERSION),
        "Writer version parameter should be set on the table");
  }

  @Test
  void testUpdateHoodieWriterVersionThrowsGlueException() throws ExecutionException, InterruptedException {
    String tableName = "test";
    List<Column> columns = Collections.singletonList(
        GlueTestUtil.getColumn("name", "string", "person's name"));
    List<Column> partitionKeys = Collections.singletonList(
        GlueTestUtil.getColumn("city", "string", "person's city"));
    CompletableFuture<GetTableResponse> tableResponseFuture =
        getTableWithDefaultProps(tableName, columns, partitionKeys);

    CompletableFuture<UpdateTableResponse> mockUpdateTableResponse = Mockito.mock(CompletableFuture.class);
    Mockito.when(mockUpdateTableResponse.get()).thenThrow(new InterruptedException());
    Mockito.when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(tableResponseFuture);
    Mockito.when(mockAwsGlue.updateTable(any(UpdateTableRequest.class))).thenReturn(mockUpdateTableResponse);

    HoodieGlueSyncException ex = assertThrows(
        HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updateHoodieWriterVersion(tableName));
    assertTrue(ex.getMessage().contains(tableName), "exception message should mention the table");
    assertTrue(ex.getMessage().contains(HoodieVersion.get()), "exception message should mention the writer version");
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

  @ParameterizedTest
  @ValueSource(strings = {"2024-01-15", "datestr=2024-01-15", "2024/01/15"})
  void testUpdateTableSchema_cascadePreservesGlueRecordedPartitionLocation(String partitionDir) {
    String tableName = GlueTestUtil.TABLE_NAME;
    String basePath = GlueTestUtil.getHiveSyncConfig().getString(META_SYNC_BASE_PATH);
    String partitionLocation = new StoragePath(basePath, partitionDir).toString();

    Table table = Table.builder()
        .name(tableName)
        .databaseName(GlueTestUtil.DB_NAME)
        .storageDescriptor(StorageDescriptor.builder()
            .location(basePath)
            .columns(Column.builder().name("name").type("string").build())
            .build())
        .partitionKeys(Column.builder().name("datestr").type("string").build())
        .build();
    // the cascade re-reads the table after the schema update, so the second read carries the new columns
    Table updatedTable = table.toBuilder()
        .storageDescriptor(table.storageDescriptor().toBuilder()
            .columns(Column.builder().name("id").type("int").build(),
                Column.builder().name("name").type("string").build())
            .build())
        .build();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(table).build()))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(updatedTable).build()));
    when(mockAwsGlue.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));

    software.amazon.awssdk.services.glue.model.Partition gluePartition =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("2024-01-15")
            .storageDescriptor(StorageDescriptor.builder().location(partitionLocation).build())
            .build();
    when(mockAwsGlue.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetPartitionsResponse.builder().partitions(gluePartition).nextToken(null).build()));

    ArgumentCaptor<BatchUpdatePartitionRequest> captor = ArgumentCaptor.forClass(BatchUpdatePartitionRequest.class);
    when(mockAwsGlue.batchUpdatePartition(captor.capture()))
        .thenReturn(CompletableFuture.completedFuture(BatchUpdatePartitionResponse.builder().build()));

    HoodieSchema schema = GlueTestUtil.getSimpleSchema();
    SchemaDifference schemaDiff = SchemaDifference.newBuilder(schema, new HashMap<>())
        .updateTableColumn("name", "string")
        .build();

    awsGlueSyncClient.updateTableSchema(tableName, schema, schemaDiff);

    List<BatchUpdatePartitionRequestEntry> entries = captor.getValue().entries();
    assertEquals(1, entries.size());
    assertEquals(partitionLocation, entries.get(0).partitionInput().storageDescriptor().location());
    assertEquals(Collections.singletonList("2024-01-15"), entries.get(0).partitionValueList());
    assertEquals(updatedTable.storageDescriptor().columns(),
        entries.get(0).partitionInput().storageDescriptor().columns());

    InOrder inOrder = inOrder(mockAwsGlue);
    inOrder.verify(mockAwsGlue).updateTable(any(UpdateTableRequest.class));
    inOrder.verify(mockAwsGlue).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
  }

  @Test
  void testUpdateTableSchema_issuesOneUpdateTable() {
    String tableName = GlueTestUtil.TABLE_NAME;
    Table table = Table.builder()
        .name(tableName)
        .databaseName(GlueTestUtil.DB_NAME)
        .storageDescriptor(StorageDescriptor.builder()
            .location("s3://base")
            .columns(Column.builder().name("name").type("string").build())
            .build())
        .partitionKeys(Column.builder().name("datestr").type("string").build())
        .build();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(table).build()));
    when(mockAwsGlue.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));

    HoodieSchema schema = GlueTestUtil.getSimpleSchema();
    SchemaDifference schemaDiff = SchemaDifference.newBuilder(schema, new HashMap<>())
        .addTableColumn("added", "string")
        .build();

    awsGlueSyncClient.updateTableSchema(tableName, schema, schemaDiff);

    verify(mockAwsGlue, times(1)).updateTable(any(UpdateTableRequest.class));
    verify(mockAwsGlue, never()).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
  }

  @Test
  void testGetPartitionsFromList_returnsPartitionsKnownToGlue() {
    String tableName = "tbl";
    software.amazon.awssdk.services.glue.model.Partition gluePartition =
        software.amazon.awssdk.services.glue.model.Partition.builder()
            .values("2024-01-15")
            .storageDescriptor(StorageDescriptor.builder().location("s3://base/2024/01/15").build())
            .build();
    ArgumentCaptor<BatchGetPartitionRequest> captor = ArgumentCaptor.forClass(BatchGetPartitionRequest.class);
    when(mockAwsGlue.batchGetPartition(captor.capture()))
        .thenReturn(CompletableFuture.completedFuture(
            BatchGetPartitionResponse.builder().partitions(gluePartition).build()));

    List<Partition> result = awsGlueSyncClient.getPartitionsFromList(tableName, Arrays.asList("2024/01/15", "2024/01/16"));

    assertEquals(1, result.size(), "only the partition Glue knows about is returned");
    assertEquals(Collections.singletonList("2024-01-15"), result.get(0).getValues());
    assertEquals("s3://base/2024/01/15", result.get(0).getStorageLocation());

    BatchGetPartitionRequest sent = captor.getValue();
    assertEquals(GlueTestUtil.DB_NAME, sent.databaseName());
    assertEquals(tableName, sent.tableName());
    assertEquals(Arrays.asList(Collections.singletonList("2024-01-15"), Collections.singletonList("2024-01-16")),
        sent.partitionsToGet().stream().map(PartitionValueList::values).collect(Collectors.toList()),
        "the requested partitions are the extracted partition values, not the storage paths");
  }

  @Test
  void testGetPartitionsFromList_emptyListDoesNotCallGlue() {
    assertTrue(awsGlueSyncClient.getPartitionsFromList("tbl", Collections.emptyList()).isEmpty());
    verify(mockAwsGlue, never()).batchGetPartition(any(BatchGetPartitionRequest.class));
  }

  @Test
  void testGetMetastoreSchema_mergesColumnsAndPartitionKeys() {
    String tableName = "tbl";
    List<Column> columns = Arrays.asList(GlueTestUtil.getColumn("name", "string", null),
        GlueTestUtil.getColumn("age", "int", null));
    List<Column> partitionKeys = Collections.singletonList(GlueTestUtil.getColumn("datestr", "string", null));
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(getTableWithDefaultProps(tableName, columns, partitionKeys));

    Map<String, String> schema = awsGlueSyncClient.getMetastoreSchema(tableName);

    assertEquals(3, schema.size());
    assertEquals("STRING", schema.get("name"), "column types are upper cased");
    assertEquals("INT", schema.get("age"));
    assertEquals("STRING", schema.get("datestr"), "partition keys are merged into the schema");
  }

  @Test
  void testGetMetastoreSchema_wrapsGlueFailure() {
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(new RuntimeException("boom"));
    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.getMetastoreSchema("tbl"));
    assertTrue(ex.getMessage().contains("Fail to get schema for table"));
  }

  @Test
  void testGetLastCommitTimeSynced_readsTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(HOODIE_LAST_COMMIT_TIME_SYNC, "100");
    parameters.put(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC, "110");
    Table withSyncTimes = tableWithParameters("synced", parameters);
    Table withoutSyncTimes = tableWithParameters("unsynced", new HashMap<>());
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(withSyncTimes).build()))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(withoutSyncTimes).build()));

    assertEquals("100", awsGlueSyncClient.getLastCommitTimeSynced("synced").get());
    assertEquals("110", awsGlueSyncClient.getLastCommitCompletionTimeSynced("synced").get());
    // the table is cached per name, so the second table name triggers the second stubbed response
    assertFalse(awsGlueSyncClient.getLastCommitTimeSynced("unsynced").isPresent());
    assertFalse(awsGlueSyncClient.getLastCommitCompletionTimeSynced("unsynced").isPresent());
    verify(mockAwsGlue, times(2)).getTable(any(GetTableRequest.class));
  }

  @Test
  void testGetStorageFieldSchemas_readsFieldsAndDocsFromStorage() {
    Map<String, FieldSchema> byName = awsGlueSyncClient.getStorageFieldSchemas().stream()
        .collect(Collectors.toMap(FieldSchema::getName, f -> f));

    assertEquals("int", byName.get("id").getType());
    assertEquals(GlueTestUtil.ID_FIELD_DOC, byName.get("id").getComment().get());
    assertEquals("string", byName.get("name").getType());
    assertEquals(GlueTestUtil.NAME_FIELD_DOC, byName.get("name").getComment().get());
    assertTrue(byName.containsKey("_hoodie_commit_time"), "metadata fields are part of the storage schema");
  }

  @Test
  void testManagePartitionIndexes_disabledDeactivatesFlagAndDropsIndexes() throws Exception {
    String tableName = "tbl";
    Map<String, String> parameters = new HashMap<>();
    parameters.put(GLUE_PARTITION_INDEX_ENABLE, "true");
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters(tableName, parameters)).build()));
    ArgumentCaptor<UpdateTableRequest> updateCaptor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    when(mockAwsGlue.updateTable(updateCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));
    when(mockAwsGlue.getPartitionIndexes(any(GetPartitionIndexesRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetPartitionIndexesResponse.builder()
            .partitionIndexDescriptorList(partitionIndexDescriptor("idx_one", "datestr"))
            .build()));
    ArgumentCaptor<DeletePartitionIndexRequest> deleteCaptor = ArgumentCaptor.forClass(DeletePartitionIndexRequest.class);
    when(mockAwsGlue.deletePartitionIndex(deleteCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(DeletePartitionIndexResponse.builder().build()));

    awsGlueSyncClient.managePartitionIndexes(tableName);

    assertEquals("false", updateCaptor.getValue().tableInput().parameters().get(GLUE_PARTITION_INDEX_ENABLE),
        "partition index usage is deactivated when the feature is off");
    assertEquals(Collections.singletonList("idx_one"), deleteCaptor.getAllValues().stream()
        .map(DeletePartitionIndexRequest::indexName).collect(Collectors.toList()));
    verify(mockAwsGlue, never()).createPartitionIndex(any(CreatePartitionIndexRequest.class));
  }

  @Test
  void testManagePartitionIndexes_enabledDropsStaleIndexesAndCreatesMissingOnes() throws Exception {
    String tableName = "tbl";
    TypedProperties props = GlueTestUtil.getHiveSyncConfig().getProps();
    props.setProperty(GlueCatalogSyncClientConfig.META_SYNC_PARTITION_INDEX_FIELDS_ENABLE.key(), "true");
    props.setProperty(GlueCatalogSyncClientConfig.META_SYNC_PARTITION_INDEX_FIELDS.key(), "datestr;hour,region");
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, new HiveSyncConfig(props), GlueTestUtil.getMetaClient());

    // the table has no partition_filtering.enabled parameter, so indexing has to be activated first
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters(tableName, new HashMap<>())).build()));
    ArgumentCaptor<UpdateTableRequest> updateCaptor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    when(mockAwsGlue.updateTable(updateCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));

    PartitionIndexDescriptor keptIndex = partitionIndexDescriptor("kept_idx", "datestr", "hour");
    when(mockAwsGlue.getPartitionIndexes(any(GetPartitionIndexesRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetPartitionIndexesResponse.builder()
            .partitionIndexDescriptorList(keptIndex, partitionIndexDescriptor("stale_idx", "old_col"))
            .build()))
        // after a drop the index list is re-read
        .thenReturn(CompletableFuture.completedFuture(GetPartitionIndexesResponse.builder()
            .partitionIndexDescriptorList(keptIndex)
            .build()));
    ArgumentCaptor<DeletePartitionIndexRequest> deleteCaptor = ArgumentCaptor.forClass(DeletePartitionIndexRequest.class);
    when(mockAwsGlue.deletePartitionIndex(deleteCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(DeletePartitionIndexResponse.builder().build()));
    ArgumentCaptor<CreatePartitionIndexRequest> createCaptor = ArgumentCaptor.forClass(CreatePartitionIndexRequest.class);
    when(mockAwsGlue.createPartitionIndex(createCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(CreatePartitionIndexResponse.builder().build()));

    awsGlueSyncClient.managePartitionIndexes(tableName);

    assertEquals("true", updateCaptor.getValue().tableInput().parameters().get(GLUE_PARTITION_INDEX_ENABLE));
    assertEquals(Collections.singletonList("stale_idx"), deleteCaptor.getAllValues().stream()
        .map(DeletePartitionIndexRequest::indexName).collect(Collectors.toList()),
        "only the index that is no longer configured is dropped");
    assertEquals(1, createCaptor.getAllValues().size(), "the already existing index is not recreated");
    PartitionIndex created = createCaptor.getValue().partitionIndex();
    assertEquals(Collections.singletonList("region"), created.keys());
    assertEquals("hudi_managed_[region]", created.indexName());
    verify(mockAwsGlue, times(2)).getPartitionIndexes(any(GetPartitionIndexesRequest.class));
  }

  @Test
  void testParsePartitionsIndexConfig_keepsOnlyTheFirstThreeIndexes() {
    TypedProperties props = GlueTestUtil.getHiveSyncConfig().getProps();
    props.setProperty(GlueCatalogSyncClientConfig.META_SYNC_PARTITION_INDEX_FIELDS.key(), "a;b,c,d,e");
    awsGlueSyncClient = new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, new HiveSyncConfig(props), GlueTestUtil.getMetaClient());

    assertEquals(Arrays.asList(Arrays.asList("a", "b"), Collections.singletonList("c"), Collections.singletonList("d")),
        awsGlueSyncClient.parsePartitionsIndexConfig(), "glue supports at most three partition indexes");
  }

  @Test
  void testUpdateLastCommitTimeSynced_writesTimelineInstantToTableParameters() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters(tableName, new HashMap<>())).build()));
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    when(mockAwsGlue.updateTable(captor.capture()))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));
    when(mockAwsGlue.getPartitionIndexes(any(GetPartitionIndexesRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetPartitionIndexesResponse.builder().build()));

    awsGlueSyncClient.updateLastCommitTimeSynced(tableName);

    Map<String, String> parameters = captor.getValue().tableInput().parameters();
    assertEquals(GlueTestUtil.INSTANT_TIME, parameters.get(HOODIE_LAST_COMMIT_TIME_SYNC),
        "the last instant of the active timeline is synced");
    assertEquals(GlueTestUtil.COMPLETION_TIME, parameters.get(HOODIE_LAST_COMMIT_COMPLETION_TIME_SYNC),
        "the completion time of that instant is synced alongside it");
    assertTrue(captor.getValue().skipArchive(), "table archiving is skipped by default");
  }

  /**
   * An indexation already in flight surfaces as an {@link ExecutionException}, anything else lands in the
   * catch-all. Neither may fail the commit-time sync.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testUpdateLastCommitTimeSynced_partitionIndexFailureDoesNotFailTheSync(boolean asExecutionFailure) throws Exception {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters(tableName, new HashMap<>())).build()));
    when(mockAwsGlue.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));
    if (asExecutionFailure) {
      CompletableFuture<GetPartitionIndexesResponse> failed = mock(CompletableFuture.class);
      when(failed.get()).thenThrow(new ExecutionException(new RuntimeException("indexing in progress")));
      when(mockAwsGlue.getPartitionIndexes(any(GetPartitionIndexesRequest.class))).thenReturn(failed);
    } else {
      when(mockAwsGlue.getPartitionIndexes(any(GetPartitionIndexesRequest.class))).thenThrow(new RuntimeException("boom"));
    }

    awsGlueSyncClient.updateLastCommitTimeSynced(tableName);

    verify(mockAwsGlue, times(1)).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testUpdateLastCommitTimeSynced_wrapsGlueFailure() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters(tableName, new HashMap<>())).build()));
    when(mockAwsGlue.updateTable(any(UpdateTableRequest.class))).thenThrow(new RuntimeException("boom"));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updateLastCommitTimeSynced(tableName));
    assertTrue(ex.getMessage().contains("Fail to update last sync commit time"));
  }

  @Test
  void testUpdateSerdeProperties_emptyPropertiesSkipUpdate() {
    assertFalse(awsGlueSyncClient.updateSerdeProperties("tbl", Collections.emptyMap(), false));
    verify(mockAwsGlue, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testUpdateSerdeProperties_unchangedPropertiesSkipUpdate() {
    String tableName = "tbl";
    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put("serialization.format", "1");
    serdeProperties.put("path", "s3://base");
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithSerdeProperties(tableName, serdeProperties)).build()));

    assertFalse(awsGlueSyncClient.updateSerdeProperties(tableName, new HashMap<>(serdeProperties), false));
    verify(mockAwsGlue, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testUpdateSerdeProperties_changedPropertiesRewriteSerdeInfo() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithSerdeProperties(tableName,
                serdePropertiesOf("serialization.format", "1", "location", "s3://old"))).build()));
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    when(mockAwsGlue.updateTable(captor.capture()))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));

    Map<String, String> serdeProperties = new HashMap<>();
    serdeProperties.put("path", "s3://new");
    assertTrue(awsGlueSyncClient.updateSerdeProperties(tableName, serdeProperties, false));

    SerDeInfo sent = captor.getValue().tableInput().storageDescriptor().serdeInfo();
    assertEquals("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", sent.serializationLibrary(),
        "the serde class is derived from the base file format");
    assertEquals("s3://new", sent.parameters().get("path"));
    assertEquals("1", sent.parameters().get("serialization.format"), "the serialization format is defaulted in");
  }

  @Test
  void testUpdateSerdeProperties_wrapsGlueFailure() {
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(EntityNotFoundException.class);
    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updateSerdeProperties("tbl", new HashMap<>(Collections.singletonMap("path", "s3://new")), false));
    assertTrue(ex.getMessage().contains("Failed to update table serde info for table"));
  }

  @Test
  void testCreateTable_existingTableIsNotRecreated() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(getTableWithDefaultProps(tableName, Collections.emptyList(), Collections.emptyList()));

    awsGlueSyncClient.createTable(tableName, GlueTestUtil.getSimpleSchema(), "inputFormat", "outputFormat",
        "serde", new HashMap<>(), new HashMap<>());

    verify(mockAwsGlue, never()).createTable(any(CreateTableRequest.class));
  }

  @Test
  void testTableExists_wrapsNonEntityNotFoundExecutionFailure() throws Exception {
    CompletableFuture<GetTableResponse> failed = mock(CompletableFuture.class);
    when(failed.get()).thenThrow(new ExecutionException(new RuntimeException("boom")));
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(failed);

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.tableExists("tbl"));
    assertTrue(ex.getMessage().contains("Fail to get table"));
  }

  @Test
  void testTableExists_wrapsClientFailure() {
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(new RuntimeException("boom"));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.tableExists("tbl"));
    assertTrue(ex.getMessage().contains("Fail to get table"));
  }

  @Test
  void testDatabaseExists_wrapsClientFailure() {
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenThrow(new RuntimeException("boom"));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.databaseExists("db"));
    assertTrue(ex.getMessage().contains("Fail to check if database exists"));
  }

  @Test
  void testDropTable_interruptionRestoresTheInterruptFlag() throws Exception {
    CompletableFuture<DeleteTableResponse> failed = mock(CompletableFuture.class);
    when(failed.get()).thenThrow(new InterruptedException("interrupted"));
    when(mockAwsGlue.deleteTable(any(DeleteTableRequest.class))).thenReturn(failed);

    assertThrows(HoodieGlueSyncException.class, () -> awsGlueSyncClient.dropTable("tbl"));
    assertTrue(Thread.interrupted(), "the interrupt flag is restored for handlers up the stack");
  }

  @Test
  void testBuildAsyncClient_appliesTheConfiguredEndpointAndRegion() {
    TypedProperties props = GlueTestUtil.getHiveSyncConfig().getProps();
    props.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), "https://glue.eu-west-1.amazonaws.com");
    props.setProperty(HoodieAWSConfig.AWS_GLUE_REGION.key(), "eu-west-1");

    try (MockedStatic<GlueAsyncClient> glueStatic = mockStatic(GlueAsyncClient.class);
         MockedStatic<StsClient> stsStatic = mockStatic(StsClient.class)) {
      GlueAsyncClientBuilder builder = mock(GlueAsyncClientBuilder.class);
      glueStatic.when(GlueAsyncClient::builder).thenReturn(builder);
      when(builder.credentialsProvider(any())).thenReturn(builder);
      when(builder.endpointOverride(any(URI.class))).thenReturn(builder);
      when(builder.region(any(Region.class))).thenReturn(builder);
      when(builder.build()).thenReturn(mockAwsGlue);
      stsStatic.when(StsClient::create).thenReturn(mockSts);

      new AWSGlueCatalogSyncClient(new HiveSyncConfig(props), GlueTestUtil.getMetaClient());

      verify(builder).endpointOverride(URI.create("https://glue.eu-west-1.amazonaws.com"));
      verify(builder).region(Region.of("eu-west-1"));
    }
  }

  @Test
  void testBuildAsyncClient_rejectsAMalformedEndpoint() {
    TypedProperties props = GlueTestUtil.getHiveSyncConfig().getProps();
    props.setProperty(HoodieAWSConfig.AWS_GLUE_ENDPOINT.key(), "https://glue eu-west-1.amazonaws.com");
    HiveSyncConfig config = new HiveSyncConfig(props);

    try (MockedStatic<GlueAsyncClient> glueStatic = mockStatic(GlueAsyncClient.class)) {
      GlueAsyncClientBuilder builder = mock(GlueAsyncClientBuilder.class);
      glueStatic.when(GlueAsyncClient::builder).thenReturn(builder);
      when(builder.credentialsProvider(any())).thenReturn(builder);

      RuntimeException ex = assertThrows(RuntimeException.class,
          () -> new AWSGlueCatalogSyncClient(config, GlueTestUtil.getMetaClient()));
      assertTrue(ex.getCause() instanceof URISyntaxException, "the malformed endpoint is reported as its parse failure");
    }
  }

  @Test
  void testReplicationOperationsAreUnsupported() {
    assertThrows(UnsupportedOperationException.class, () -> awsGlueSyncClient.getLastReplicatedTime("tbl"));
    assertThrows(UnsupportedOperationException.class, () -> awsGlueSyncClient.updateLastReplicatedTimeStamp("tbl", "101"));
    assertThrows(UnsupportedOperationException.class, () -> awsGlueSyncClient.deleteLastReplicatedTimeStamp("tbl"));
  }

  @Test
  void testGeneratePushDownFilter_delegatesToTheGlueFilterGenerator() {
    assertEquals("datestr = '2024-01-15'", awsGlueSyncClient.generatePushDownFilter(
        Collections.singletonList("2024/01/15"), Collections.singletonList(new FieldSchema("datestr", "string"))));
  }

  @Test
  void testGetPartitionsFromList_wrapsGlueFailure() throws Exception {
    CompletableFuture<BatchGetPartitionResponse> failed = mock(CompletableFuture.class);
    when(failed.get()).thenThrow(new ExecutionException(new RuntimeException("boom")));
    when(mockAwsGlue.batchGetPartition(any(BatchGetPartitionRequest.class))).thenReturn(failed);

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.getPartitionsFromList("tbl", Collections.singletonList("2024/01/15")));
    assertTrue(ex.getMessage().contains("Failed to get all partitions for table"));
  }

  @Test
  void testAddPartitionsToTable_nonAlreadyExistsErrorsFailTheSync() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder()
            .table(Table.builder().name(tableName)
                .storageDescriptor(StorageDescriptor.builder().location("s3://base").build()).build())
            .build()));
    PartitionError error = PartitionError.builder()
        .errorDetail(ErrorDetail.builder().errorCode("AccessDeniedException").build()).build();
    when(mockAwsGlue.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            BatchCreatePartitionResponse.builder().errors(Collections.singletonList(error)).build()));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.addPartitionsToTable(tableName, Collections.singletonList("2024/01/15")));
    assertTrue(ex.getCause().getCause().getMessage().contains("Fail to add partitions to"),
        "an error that is not AlreadyExists fails the sync");
  }

  @Test
  void testUpdatePartitionsToTable_errorResponsesFailTheSync() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder()
            .table(Table.builder().name(tableName)
                .storageDescriptor(StorageDescriptor.builder().location("s3://base").build()).build())
            .build()));
    BatchUpdatePartitionFailureEntry error = BatchUpdatePartitionFailureEntry.builder()
        .partitionValueList("2024-01-15")
        .errorDetail(ErrorDetail.builder().errorCode("AccessDeniedException").build()).build();
    when(mockAwsGlue.batchUpdatePartition(any(BatchUpdatePartitionRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            BatchUpdatePartitionResponse.builder().errors(Collections.singletonList(error)).build()));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updatePartitionsToTable(tableName, Collections.singletonList("2024/01/15")));
    assertTrue(ex.getCause().getCause().getMessage().contains("Fail to update partitions to"));
  }

  @Test
  void testPartitionIndexEnableAccessors_wrapGlueFailures() {
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(new RuntimeException("boom"));

    assertTrue(assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.getPartitionIndexEnable("tbl"))
        .getMessage().contains("Fail to get parameter partition_filtering.enabled"));
    assertTrue(assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updatePartitionIndexEnable("tbl", true))
        .getMessage().contains("Fail to update parameter partition_filtering.enabled"));
  }

  @Test
  void testLastCommitTimeAccessors_wrapGlueFailures() {
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(new RuntimeException("boom"));

    assertTrue(assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.getLastCommitTimeSynced("tbl"))
        .getMessage().contains("Fail to get last sync commit time"));
    assertTrue(assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.getLastCommitCompletionTimeSynced("other"))
        .getMessage().contains("Failed to get the last commit completion time synced"));
  }

  @Test
  void testUpdateTableProperties_propertiesAlreadyInTheCatalogSkipUpdate() {
    String tableName = "tbl";
    Map<String, String> existing = new HashMap<>();
    existing.put("hudi.metadata-listing-enabled", "FALSE");
    existing.put(HOODIE_LAST_COMMIT_TIME_SYNC, "100");
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters(tableName, existing)).build()));

    Map<String, String> update = new HashMap<>();
    update.put(HOODIE_LAST_COMMIT_TIME_SYNC, "100");
    assertFalse(awsGlueSyncClient.updateTableProperties(tableName, update));
    verify(mockAwsGlue, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testStorageSchemaReads_failWhenTheTableHasNoCommits() throws IOException {
    AWSGlueCatalogSyncClient clientWithoutCommits = clientForTableWithoutCommits();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(getTableWithDefaultProps("tbl", Collections.emptyList(), Collections.emptyList()));

    assertTrue(assertThrows(HoodieGlueSyncException.class, clientWithoutCommits::getStorageFieldSchemas)
        .getMessage().contains("Failed to get field schemas from storage"));
    assertTrue(assertThrows(HoodieGlueSyncException.class,
        () -> clientWithoutCommits.updateTableComments("tbl", Collections.emptyList(), Collections.emptyList()))
        .getMessage().contains("Failed to get schema's doc from storage"));
  }

  @Test
  void testUpdateLastCommitTimeSynced_withoutACommitNothingIsSynced() throws IOException {
    AWSGlueCatalogSyncClient clientWithoutCommits = clientForTableWithoutCommits();
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(
            GetTableResponse.builder().table(tableWithParameters("tbl", new HashMap<>())).build()));
    when(mockAwsGlue.getPartitionIndexes(any(GetPartitionIndexesRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetPartitionIndexesResponse.builder().build()));

    clientWithoutCommits.updateLastCommitTimeSynced("tbl");

    verify(mockAwsGlue, never()).updateTable(any(UpdateTableRequest.class));
  }

  @Test
  void testUpdateTableSchema_wrapsGlueFailure() {
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenThrow(new RuntimeException("boom"));
    HoodieSchema schema = GlueTestUtil.getSimpleSchema();
    SchemaDifference schemaDiff = SchemaDifference.newBuilder(schema, new HashMap<>()).build();

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.updateTableSchema("tbl", schema, schemaDiff));
    assertTrue(ex.getMessage().contains("Fail to update definition for table"));
  }

  @Test
  void testUpdateTableSchema_cascadeWithoutPartitionsIssuesNoPartitionUpdate() {
    String tableName = GlueTestUtil.TABLE_NAME;
    Table table = tableWithColumns(tableName,
        Collections.singletonList(Column.builder().name("name").type("string").build()),
        Collections.singletonList(Column.builder().name("datestr").type("string").build()));
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetTableResponse.builder().table(table).build()));
    when(mockAwsGlue.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(UpdateTableResponse.builder().build()));
    when(mockAwsGlue.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(GetPartitionsResponse.builder().nextToken(null).build()));

    HoodieSchema schema = GlueTestUtil.getSimpleSchema();
    awsGlueSyncClient.updateTableSchema(tableName, schema,
        SchemaDifference.newBuilder(schema, new HashMap<>()).updateTableColumn("name", "string").build());

    verify(mockAwsGlue, never()).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
  }

  @Test
  void testCreateOrReplaceTable_wrapsFailureOfTheReplace() {
    String tableName = "tbl";
    when(mockAwsGlue.getTable(any(GetTableRequest.class)))
        .thenReturn(getTableWithDefaultProps(tableName, Collections.emptyList(), Collections.emptyList()));
    when(mockAwsGlue.deleteTable(any(DeleteTableRequest.class))).thenThrow(new RuntimeException("boom"));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.createOrReplaceTable(tableName, GlueTestUtil.getSimpleSchema(), "inputFormat",
            "outputFormat", "serde", new HashMap<>(), new HashMap<>()));
    assertTrue(ex.getMessage().contains("Fail to recreate the table"));
  }

  @Test
  void testCreateTable_wrapsGlueFailure() throws Exception {
    String tableName = "tbl";
    CompletableFuture<GetTableResponse> notFound = mock(CompletableFuture.class);
    when(notFound.get()).thenThrow(new ExecutionException(EntityNotFoundException.builder().build()));
    when(mockAwsGlue.getTable(any(GetTableRequest.class))).thenReturn(notFound);
    when(mockAwsGlue.createTable(any(CreateTableRequest.class))).thenThrow(new RuntimeException("boom"));

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.createTable(tableName, GlueTestUtil.getSimpleSchema(), "inputFormat",
            "outputFormat", "serde", new HashMap<>(), new HashMap<>()));
    assertTrue(ex.getMessage().contains("Fail to create"));
  }

  @Test
  void testCreateDatabase_wrapsGlueFailure() throws Exception {
    String dbName = "db";
    CompletableFuture<GetDatabaseResponse> notFound = mock(CompletableFuture.class);
    when(notFound.get()).thenThrow(new ExecutionException(EntityNotFoundException.builder().build()));
    when(mockAwsGlue.getDatabase(any(GetDatabaseRequest.class))).thenReturn(notFound);
    CompletableFuture<CreateDatabaseResponse> failed = mock(CompletableFuture.class);
    when(failed.get()).thenThrow(new ExecutionException(new RuntimeException("boom")));
    when(mockAwsGlue.createDatabase(any(CreateDatabaseRequest.class))).thenReturn(failed);

    HoodieGlueSyncException ex = assertThrows(HoodieGlueSyncException.class,
        () -> awsGlueSyncClient.createDatabase(dbName));
    assertTrue(ex.getMessage().contains("Fail to create database"));
  }

  private AWSGlueCatalogSyncClient clientForTableWithoutCommits() throws IOException {
    HoodieTableMetaClient withoutCommits = GlueTestUtil.createTableWithoutCommits();
    TypedProperties props = TypedProperties.copy(GlueTestUtil.getHiveSyncConfig().getProps());
    props.setProperty(META_SYNC_BASE_PATH.key(), withoutCommits.getBasePath().toString());
    return new AWSGlueCatalogSyncClient(mockAwsGlue, mockSts, new HiveSyncConfig(props), withoutCommits);
  }

  private static Map<String, String> serdePropertiesOf(String... keysAndValues) {
    Map<String, String> properties = new HashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      properties.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return properties;
  }

  private static PartitionIndexDescriptor partitionIndexDescriptor(String indexName, String... keys) {
    return PartitionIndexDescriptor.builder()
        .indexName(indexName)
        .keys(Arrays.stream(keys).map(key -> KeySchemaElement.builder().name(key).build()).collect(Collectors.toList()))
        .build();
  }

  private static Table tableWithParameters(String tableName, Map<String, String> parameters) {
    return tableWithColumns(tableName, Collections.singletonList(Column.builder().name("name").type("string").build()),
        Collections.singletonList(Column.builder().name("datestr").type("string").build()))
        .toBuilder()
        .parameters(parameters)
        .build();
  }

  private static Table tableWithColumns(String tableName, List<Column> columns, List<Column> partitionKeys) {
    return Table.builder()
        .name(tableName)
        .databaseName(GlueTestUtil.DB_NAME)
        .tableType("COPY_ON_WRITE")
        .parameters(new HashMap<>())
        .storageDescriptor(StorageDescriptor.builder().location("s3://base").columns(columns).build())
        .partitionKeys(partitionKeys)
        .build();
  }

  private static Table tableWithSerdeProperties(String tableName, Map<String, String> serdeProperties) {
    Table table = tableWithColumns(tableName,
        Collections.singletonList(Column.builder().name("name").type("string").build()),
        Collections.singletonList(Column.builder().name("datestr").type("string").build()));
    return table.toBuilder()
        .storageDescriptor(table.storageDescriptor().toBuilder()
            .serdeInfo(SerDeInfo.builder().serializationLibrary("serde").parameters(serdeProperties).build())
            .build())
        .build();
  }
}
