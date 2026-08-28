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

package org.apache.hudi.table.catalog;

import org.apache.hudi.adapter.HiveCatalogConstants.AlterHiveDatabaseOp;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.keygen.SimpleAvroKeyGenerator;
import org.apache.hudi.sink.partitioner.profile.WriteProfiles;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.CatalogUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.apache.hudi.adapter.HiveCatalogConstants.ALTER_DATABASE_OP;
import static org.apache.hudi.adapter.HiveCatalogConstants.DATABASE_LOCATION_URI;
import static org.apache.hudi.adapter.HiveCatalogConstants.DATABASE_OWNER_NAME;
import static org.apache.hudi.adapter.HiveCatalogConstants.DATABASE_OWNER_TYPE;
import static org.apache.hudi.adapter.HiveCatalogConstants.ROLE_OWNER;
import static org.apache.hudi.adapter.HiveCatalogConstants.USER_OWNER;
import static org.apache.hudi.configuration.FlinkOptions.ORDERING_FIELDS;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_NAME;
import static org.apache.hudi.table.catalog.HoodieCatalogTestUtils.createStorageConf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieHiveCatalog}.
 */
public class TestHoodieHiveCatalog extends BaseTestHoodieCatalog {
  Schema schema =
      Schema.newBuilder()
          .column("uuid", DataTypes.INT().notNull())
          .column("name", DataTypes.STRING())
          .column("age", DataTypes.INT())
          .column("infos", DataTypes.ARRAY(DataTypes.STRING()))
          .column("par1", DataTypes.STRING())
          .column("ts_3", DataTypes.TIMESTAMP(3))
          .column("ts_6", DataTypes.TIMESTAMP(6))
          .primaryKey("uuid")
          .build();
  List<String> partitions = Collections.singletonList("par1");

  Schema multiKeySinglePartitionTableSchema =
      Schema.newBuilder()
          .column("uuid", DataTypes.INT().notNull())
          .column("name", DataTypes.STRING().notNull())
          .column("age", DataTypes.INT())
          .column("par1", DataTypes.STRING())
          .primaryKey("uuid", "name")
          .build();

  Schema singleKeyMultiPartitionTableSchema =
      Schema.newBuilder()
          .column("uuid", DataTypes.INT().notNull())
          .column("name", DataTypes.STRING())
          .column("par1", DataTypes.STRING())
          .column("par2", DataTypes.STRING())
          .primaryKey("uuid")
          .build();
  List<String> multiPartitions = Arrays.asList("par1", "par2");

  private static HoodieHiveCatalog hoodieCatalog;
  private final ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "test");

  @BeforeAll
  public static void createCatalog() {
    hoodieCatalog = HoodieCatalogTestUtils.createHiveCatalog();
    hoodieCatalog.open();
  }

  @AfterEach
  public void dropTable() throws TableNotExistException {
    hoodieCatalog.dropTable(tablePath, true);
  }

  @AfterAll
  public static void closeCatalog() {
    if (hoodieCatalog != null) {
      hoodieCatalog.close();
    }
  }

  @Test
  void testDatabaseLifecycle() throws Exception {
    String databaseName = "catalog_database_lifecycle";
    hoodieCatalog.dropDatabase(databaseName, true, true);

    try {
      CatalogDatabase database =
          new CatalogDatabaseImpl(new HashMap<>(), "catalog api database");
      hoodieCatalog.createDatabase(databaseName, database, false);

      assertTrue(hoodieCatalog.databaseExists(databaseName));
      assertTrue(hoodieCatalog.listDatabases().contains(databaseName));
      CatalogDatabase storedDatabase = hoodieCatalog.getDatabase(databaseName);
      assertEquals("catalog api database", storedDatabase.getComment());
      assertNotNull(storedDatabase.getProperties().get(DATABASE_LOCATION_URI));
      assertThrows(
          DatabaseAlreadyExistException.class,
          () -> hoodieCatalog.createDatabase(databaseName, database, false));
      hoodieCatalog.createDatabase(databaseName, database, true);

      Map<String, String> changedProperties = new HashMap<>();
      changedProperties.put("purpose", "coverage");
      changedProperties.put("is_generic", "true");
      hoodieCatalog.alterDatabase(
          databaseName,
          new CatalogDatabaseImpl(changedProperties, null),
          false);
      assertEquals(
          "coverage",
          hoodieCatalog.getDatabase(databaseName).getProperties().get("purpose"));
      assertFalse(
          hoodieCatalog.getDatabase(databaseName).getProperties().containsKey("is_generic"));

      // Hive 2.x ObjectStore persists parameter and owner changes, but not location changes.
      Map<String, String> userOwner = new HashMap<>();
      userOwner.put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_OWNER.name());
      userOwner.put(DATABASE_OWNER_NAME, "catalog-user");
      userOwner.put(DATABASE_OWNER_TYPE, USER_OWNER);
      hoodieCatalog.alterDatabase(
          databaseName,
          new CatalogDatabaseImpl(userOwner, null),
          false);
      assertEquals(
          PrincipalType.USER,
          hoodieCatalog.getClient().getDatabase(databaseName).getOwnerType());
      assertEquals(
          "catalog-user",
          hoodieCatalog.getClient().getDatabase(databaseName).getOwnerName());

      Map<String, String> roleOwner = new HashMap<>();
      roleOwner.put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_OWNER.name());
      roleOwner.put(DATABASE_OWNER_NAME, "catalog-role");
      roleOwner.put(DATABASE_OWNER_TYPE, ROLE_OWNER);
      hoodieCatalog.alterDatabase(
          databaseName,
          new CatalogDatabaseImpl(roleOwner, null),
          false);
      assertEquals(
          PrincipalType.ROLE,
          hoodieCatalog.getClient().getDatabase(databaseName).getOwnerType());
      assertEquals(
          "catalog-role",
          hoodieCatalog.getClient().getDatabase(databaseName).getOwnerName());

      Map<String, String> invalidOwner = new HashMap<>();
      invalidOwner.put(ALTER_DATABASE_OP, AlterHiveDatabaseOp.CHANGE_OWNER.name());
      invalidOwner.put(DATABASE_OWNER_NAME, "catalog-group");
      invalidOwner.put(DATABASE_OWNER_TYPE, "GROUP");
      assertThrows(
          org.apache.flink.table.catalog.exceptions.CatalogException.class,
          () -> hoodieCatalog.alterDatabase(
              databaseName,
              new CatalogDatabaseImpl(invalidOwner, null),
              false));

      hoodieCatalog.alterDatabase(
          "missing_catalog_api_db",
          new CatalogDatabaseImpl(Collections.emptyMap(), null),
          true);
      assertThrows(
          DatabaseNotExistException.class,
          () -> hoodieCatalog.alterDatabase(
              "missing_catalog_api_db",
              new CatalogDatabaseImpl(Collections.emptyMap(), null),
              false));

      hoodieCatalog.dropDatabase(databaseName, false, false);
      assertFalse(hoodieCatalog.databaseExists(databaseName));
      assertThrows(
          DatabaseNotExistException.class,
          () -> hoodieCatalog.dropDatabase(databaseName, false, false));
      hoodieCatalog.dropDatabase(databaseName, true, false);
    } finally {
      hoodieCatalog.dropDatabase(databaseName, true, true);
    }
  }

  @Test
  void testTableLifecycleAgainstMetastore() throws Exception {
    String databaseName = "catalog_table_lifecycle";
    ObjectPath databaseTablePath = new ObjectPath(databaseName, "catalog_api_table");
    createCatalogDatabase(databaseName);

    try {
      CatalogTable catalogTable = createCatalogTable("stored in hms");
      hoodieCatalog.createTable(databaseTablePath, catalogTable, false);
      assertThrows(
          TableAlreadyExistException.class,
          () -> hoodieCatalog.createTable(databaseTablePath, catalogTable, false));
      hoodieCatalog.createTable(databaseTablePath, catalogTable, true);

      assertTrue(hoodieCatalog.listTables(databaseName).contains(databaseTablePath.getObjectName()));
      assertTrue(hoodieCatalog.tableExists(databaseTablePath));
      Table hiveTable = hoodieCatalog.getHiveTable(databaseTablePath);
      assertEquals("hudi", hiveTable.getParameters().get(CONNECTOR.key()));
      assertEquals("stored in hms", hiveTable.getParameters().get(TableOptionProperties.COMMENT));
      assertEquals(
          "uuid:int,name:string,age:int,infos:array<string>,ts_3:timestamp,ts_6:timestamp",
          hiveTable.getSd().getCols().stream()
              .filter(field -> !field.getName().startsWith("_hoodie_"))
              .map(field -> field.getName() + ":" + field.getType())
              .collect(Collectors.joining(",")));
      assertEquals(
          schema.getColumns().stream()
              .map(Schema.UnresolvedColumn::getName)
              .collect(Collectors.toList()),
          hoodieCatalog.getTable(databaseTablePath).getUnresolvedSchema().getColumns().stream()
              .map(Schema.UnresolvedColumn::getName)
              .collect(Collectors.toList()));

      assertThrows(
          DatabaseNotEmptyException.class,
          () -> hoodieCatalog.dropDatabase(databaseName, false, false));
      hoodieCatalog.dropTable(databaseTablePath, false);
      assertFalse(hoodieCatalog.tableExists(databaseTablePath));
      assertThrows(
          TableNotExistException.class,
          () -> hoodieCatalog.dropTable(databaseTablePath, false));
    } finally {
      dropCatalogObjects(databaseName, databaseTablePath);
    }
  }

  @Test
  void testHiveSyncOptionsInjected() throws Exception {
    String databaseName = "catalog_hive_sync_options";
    ObjectPath databaseTablePath = new ObjectPath(databaseName, "catalog_api_table");
    createCatalogDatabase(databaseName);

    try {
      hoodieCatalog.createTable(databaseTablePath, createCatalogTable("hive sync options"), false);

      String metastoreUris = hoodieCatalog.getHiveConf().getVar(
          org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS);
      String remoteMetastoreUri = "thrift://localhost:9083";
      try {
        // Simulate a remote catalog so getTable must propagate its endpoint to Hive sync.
        hoodieCatalog.getHiveConf().setVar(
            org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS,
            remoteMetastoreUri);
        Map<String, String> supplementedOptions =
            hoodieCatalog.getTable(databaseTablePath).getOptions();
        assertEquals("true", supplementedOptions.get(FlinkOptions.HIVE_SYNC_ENABLED.key()));
        assertEquals("hms", supplementedOptions.get(FlinkOptions.HIVE_SYNC_MODE.key()));
        assertEquals(databaseName, supplementedOptions.get(FlinkOptions.HIVE_SYNC_DB.key()));
        assertEquals(
            databaseTablePath.getObjectName(),
            supplementedOptions.get(FlinkOptions.HIVE_SYNC_TABLE.key()));
        assertEquals(
            remoteMetastoreUri,
            supplementedOptions.get(FlinkOptions.HIVE_SYNC_METASTORE_URIS.key()));
      } finally {
        hoodieCatalog.getHiveConf().setVar(
            org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS,
            metastoreUris);
      }
    } finally {
      dropCatalogObjects(databaseName, databaseTablePath);
    }
  }

  private CatalogTable createCatalogTable(String comment) {
    Map<String, String> tableOptions = new HashMap<>();
    tableOptions.put(CONNECTOR.key(), "hudi");
    return CatalogUtils.createCatalogTable(schema, partitions, tableOptions, comment);
  }

  private static void createCatalogDatabase(String databaseName) throws Exception {
    hoodieCatalog.dropDatabase(databaseName, true, true);
    hoodieCatalog.createDatabase(
        databaseName,
        new CatalogDatabaseImpl(Collections.emptyMap(), "catalog api database"),
        false);
  }

  private static void dropCatalogObjects(
      String databaseName, ObjectPath databaseTablePath) throws Exception {
    if (hoodieCatalog.tableExists(databaseTablePath)) {
      hoodieCatalog.dropTable(databaseTablePath, true);
    }
    hoodieCatalog.dropDatabase(databaseName, true, true);
  }

  @Test
  void testUnsupportedCatalogOperationsAndDefaults() throws Exception {
    CatalogPartitionSpec partitionSpec =
        new CatalogPartitionSpec(Collections.singletonMap("par1", "20260728"));
    ObjectPath functionPath = new ObjectPath(TEST_DEFAULT_DATABASE, "function");
    ObjectPath missingTablePath = new ObjectPath("missing_database", "missing_table");

    assertThrows(HoodieCatalogException.class, () -> hoodieCatalog.listViews(TEST_DEFAULT_DATABASE));
    assertEquals(
        Collections.emptyList(),
        hoodieCatalog.listTables(missingTablePath.getDatabaseName()));
    assertFalse(hoodieCatalog.tableExists(missingTablePath));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.renameTable(missingTablePath, "renamed", false));
    assertEquals(Collections.emptyList(), hoodieCatalog.listPartitions(tablePath));
    assertEquals(Collections.emptyList(), hoodieCatalog.listPartitions(tablePath, partitionSpec));
    assertEquals(
        Collections.emptyList(),
        hoodieCatalog.listPartitionsByFilter(tablePath, Collections.emptyList()));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.getPartition(tablePath, partitionSpec));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.partitionExists(tablePath, partitionSpec));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.createPartition(tablePath, partitionSpec, null, false));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.alterPartition(tablePath, partitionSpec, null, false));
    assertThrows(
        PartitionNotExistException.class,
        () -> hoodieCatalog.dropPartition(missingTablePath, partitionSpec, false));
    hoodieCatalog.dropPartition(missingTablePath, partitionSpec, true);

    Map<String, String> tableOptions = Collections.singletonMap(CONNECTOR.key(), "hudi");
    CatalogTable table =
        CatalogUtils.createCatalogTable(schema, partitions, tableOptions, "missing database");
    assertThrows(
        DatabaseNotExistException.class,
        () -> hoodieCatalog.createTable(missingTablePath, table, false));

    assertEquals(Collections.emptyList(), hoodieCatalog.listFunctions(TEST_DEFAULT_DATABASE));
    assertThrows(
        org.apache.flink.table.catalog.exceptions.FunctionNotExistException.class,
        () -> hoodieCatalog.getFunction(functionPath));
    assertFalse(hoodieCatalog.functionExists(functionPath));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.createFunction(functionPath, null, false));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.alterFunction(functionPath, null, false));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.dropFunction(functionPath, false));

    assertSame(CatalogTableStatistics.UNKNOWN, hoodieCatalog.getTableStatistics(tablePath));
    assertSame(
        CatalogColumnStatistics.UNKNOWN,
        hoodieCatalog.getTableColumnStatistics(tablePath));
    assertSame(
        CatalogTableStatistics.UNKNOWN,
        hoodieCatalog.getPartitionStatistics(tablePath, partitionSpec));
    assertSame(
        CatalogColumnStatistics.UNKNOWN,
        hoodieCatalog.getPartitionColumnStatistics(tablePath, partitionSpec));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.alterTableStatistics(
            tablePath, CatalogTableStatistics.UNKNOWN, false));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.alterTableColumnStatistics(
            tablePath, CatalogColumnStatistics.UNKNOWN, false));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.alterPartitionStatistics(
            tablePath, partitionSpec, CatalogTableStatistics.UNKNOWN, false));
    assertThrows(
        HoodieCatalogException.class,
        () -> hoodieCatalog.alterPartitionColumnStatistics(
            tablePath, partitionSpec, CatalogColumnStatistics.UNKNOWN, false));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testCreateAndGetHoodieTable(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FactoryUtil.CONNECTOR.key(), "hudi");
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.toString());

    CatalogTable table = CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    hoodieCatalog.createTable(tablePath, table, false);

    // validate hive table
    Table hiveTable = hoodieCatalog.getHiveTable(tablePath);
    String fieldSchema = hiveTable.getSd().getCols().stream()
        .map(f -> f.getName() + ":" + f.getType())
        .collect(Collectors.joining(","));
    String expectedFieldSchema = ""
        + "_hoodie_commit_time:string,"
        + "_hoodie_commit_seqno:string,"
        + "_hoodie_record_key:string,"
        + "_hoodie_partition_path:string,"
        + "_hoodie_file_name:string,"
        + "uuid:int,"
        + "name:string,"
        + "age:int,"
        + "infos:array<string>,"
        + "ts_3:timestamp,"
        + "ts_6:timestamp";
    assertEquals(expectedFieldSchema, fieldSchema);
    String partitionSchema = hiveTable.getPartitionKeys().stream()
        .map(f -> f.getName() + ":" + f.getType())
        .collect(Collectors.joining(","));
    assertEquals("par1:string", partitionSchema);

    // validate spark schema properties
    String avroSchemaStr = hiveTable.getParameters().get("spark.sql.sources.schema.part.0");
    String expectedAvroSchemaStr = ""
        + "{\"type\":\"struct\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"_hoodie_partition_path\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"_hoodie_file_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"uuid\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},"
        + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"age\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"infos\",\"type\":{\"type\":\"array\", \"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"ts_3\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"ts_6\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},"
        + "{\"name\":\"par1\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}";
    assertEquals(expectedAvroSchemaStr, avroSchemaStr);

    // validate array field nullable
    ObjectMapper mapper = new ObjectMapper();
    JsonNode arrayFieldTypeNode = mapper.readTree(avroSchemaStr).get("fields").get(8).get("type");
    assertThat(arrayFieldTypeNode.get("type").asText(), is("array"));
    assertThat(arrayFieldTypeNode.get("containsNull").asBoolean(), is(true));

    // validate catalog table
    CatalogBaseTable table1 = hoodieCatalog.getTable(tablePath);
    assertEquals("hudi", table1.getOptions().get(CONNECTOR.key()));
    assertEquals(tableType.toString(), table1.getOptions().get(FlinkOptions.TABLE_TYPE.key()));
    assertEquals("uuid", table1.getOptions().get(FlinkOptions.RECORD_KEY_FIELD.key()));
    assertNull(table1.getOptions().get(ORDERING_FIELDS.key()), "preCombine key is not declared");
    String tableSchema = table1.getUnresolvedSchema().getColumns().stream()
        .map(Schema.UnresolvedColumn::toString)
        .collect(Collectors.joining(","));
    String expectedTableSchema = "`uuid` INT NOT NULL,`name` STRING,`age` INT,`infos` ARRAY<STRING>,`par1` STRING,`ts_3` TIMESTAMP(3),`ts_6` TIMESTAMP(6)";
    assertEquals(expectedTableSchema, tableSchema);
    assertEquals(Collections.singletonList("uuid"), table1.getUnresolvedSchema().getPrimaryKey().get().getColumnNames());
    assertEquals(Collections.singletonList("par1"), ((CatalogTable) table1).getPartitionKeys());

    // validate the full name of table create schema
    HoodieTableConfig tableConfig = StreamerUtil.getTableConfig(table1.getOptions().get(FlinkOptions.PATH.key()), hoodieCatalog.getHiveConf()).get();
    Option<HoodieSchema> tableCreateSchema = tableConfig.getTableCreateSchema();
    assertTrue(tableCreateSchema.isPresent(), "Table should have been created");
    assertThat(tableCreateSchema.get().getFullName(), is("hoodie.test.test_record"));

    // validate explicit primary key
    options.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
    table = CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    hoodieCatalog.alterTable(tablePath, table, true);

    CatalogBaseTable table2 = hoodieCatalog.getTable(tablePath);
    assertEquals("id", table2.getOptions().get(FlinkOptions.RECORD_KEY_FIELD.key()));
    options.remove(RECORDKEY_FIELD_NAME.key());

    // validate key generator for partitioned table
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        createStorageConf(), hoodieCatalog.inferTablePath(tablePath, table));
    String keyGeneratorClassName = metaClient.getTableConfig().getKeyGeneratorClassName();
    assertEquals(keyGeneratorClassName, SimpleAvroKeyGenerator.class.getName());

    // validate single key and multiple partition for partitioned table
    ObjectPath singleKeyMultiPartitionPath = new ObjectPath("default", "tb_skmp_" + System.currentTimeMillis());
    CatalogTable singleKeyMultiPartitionTable =
        CatalogUtils.createCatalogTable(singleKeyMultiPartitionTableSchema, multiPartitions, options, "hudi table");
    hoodieCatalog.createTable(singleKeyMultiPartitionPath, singleKeyMultiPartitionTable, false);

    HoodieTableMetaClient singleKeyMultiPartitionTableMetaClient = HoodieTestUtils.createMetaClient(
        createStorageConf(),
        hoodieCatalog.inferTablePath(singleKeyMultiPartitionPath, singleKeyMultiPartitionTable));
    assertThat(singleKeyMultiPartitionTableMetaClient.getTableConfig().getKeyGeneratorClassName(), is(ComplexAvroKeyGenerator.class.getName()));

    // validate multiple key and single partition for partitioned table
    ObjectPath multiKeySinglePartitionPath = new ObjectPath("default", "tb_mksp_" + System.currentTimeMillis());

    options.remove(RECORDKEY_FIELD_NAME.key());
    CatalogTable multiKeySinglePartitionTable =
        CatalogUtils.createCatalogTable(multiKeySinglePartitionTableSchema, partitions, options, "hudi table");
    hoodieCatalog.createTable(multiKeySinglePartitionPath, multiKeySinglePartitionTable, false);

    HoodieTableMetaClient multiKeySinglePartitionTableMetaClient = HoodieTestUtils.createMetaClient(
        createStorageConf(),
        hoodieCatalog.inferTablePath(multiKeySinglePartitionPath, multiKeySinglePartitionTable));
    assertThat(multiKeySinglePartitionTableMetaClient.getTableConfig().getKeyGeneratorClassName(), is(ComplexAvroKeyGenerator.class.getName()));

    // validate key generator for non partitioned table
    ObjectPath nonPartitionPath = new ObjectPath("default", "tb_" + tableType);
    CatalogTable nonPartitionTable =
        CatalogUtils.createCatalogTable(schema, new ArrayList<>(), options, "hudi table");
    hoodieCatalog.createTable(nonPartitionPath, nonPartitionTable, false);

    metaClient = HoodieTestUtils.createMetaClient(
        createStorageConf(), hoodieCatalog.inferTablePath(nonPartitionPath, nonPartitionTable));
    keyGeneratorClassName = metaClient.getTableConfig().getKeyGeneratorClassName();
    assertEquals(keyGeneratorClassName, NonpartitionedAvroKeyGenerator.class.getName());

    // validate the order of partition fields in the multi-partition table
    List<String> multiPartitions = Arrays.asList("par2", "par1");
    ObjectPath multiPartitionsTablePath = new ObjectPath("default", "tb_mp_" + System.currentTimeMillis());
    CatalogTable multiPartitionsTable =
        CatalogUtils.createCatalogTable(singleKeyMultiPartitionTableSchema, multiPartitions, options, "multi-partition hudi table");
    RuntimeException exception = assertThrows(HoodieCatalogException.class, () -> hoodieCatalog.createTable(multiPartitionsTablePath, multiPartitionsTable, false));
    assertThat(exception.getCause().getMessage(), containsString("The order of regular fields(par1,par2) and partition fields(par2,par1) needs to be consistent"));
  }

  @Test
  void testCreateTableWithIndexType() throws TableNotExistException, TableAlreadyExistException, DatabaseNotExistException {
    Map<String, String> options = new HashMap<>();
    options.put(FactoryUtil.CONNECTOR.key(), "hudi");
    // hoodie.index.type
    options.put(HoodieIndexConfig.INDEX_TYPE.key(), "BUCKET");
    CatalogTable table =
        CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    hoodieCatalog.createTable(tablePath, table, false);
    Map<String, String> params = hoodieCatalog.getHiveTable(tablePath).getParameters();
    assertResult(params, "BUCKET");
    options.remove(HoodieIndexConfig.INDEX_TYPE.key());

    // index.type
    options.put(FlinkOptions.INDEX_TYPE.key(), FlinkOptions.INDEX_TYPE.defaultValue());
    table =
        CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    ObjectPath newTablePath1 = new ObjectPath("default", "test" + System.currentTimeMillis());
    hoodieCatalog.createTable(newTablePath1, table, false);

    params = hoodieCatalog.getHiveTable(newTablePath1).getParameters();
    assertResult(params, FlinkOptions.INDEX_TYPE.defaultValue());

    // index.type + hoodie.index.type
    options.put(HoodieIndexConfig.INDEX_TYPE.key(), "BUCKET");
    table = CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    ObjectPath newTablePath2 = new ObjectPath("default", "test" + System.currentTimeMillis());
    hoodieCatalog.createTable(newTablePath2, table, false);

    params = hoodieCatalog.getHiveTable(newTablePath2).getParameters();
    assertResult(params, "BUCKET");
  }

  private void assertResult(Map<String, String> params, String index) {
    assertTrue(params.containsKey(HoodieIndexConfig.INDEX_TYPE.key()));
    assertFalse(params.containsKey(FlinkOptions.INDEX_TYPE.key()));
    assertThat(params.get(HoodieIndexConfig.INDEX_TYPE.key()), is(index));
  }

  @Test
  void testCreateTableWithoutOrderingFields() throws TableAlreadyExistException, DatabaseNotExistException, IOException, TableNotExistException {
    String db = "default";
    hoodieCatalog = HoodieCatalogTestUtils.createHiveCatalog();
    hoodieCatalog.open();

    Map<String, String> options = new HashMap<>();
    options.put(FactoryUtil.CONNECTOR.key(), "hudi");

    TypedProperties props = createTableAndReturnTableProperties(options, new ObjectPath(db, "tmptb1"));
    assertFalse(props.containsKey(HoodieTableConfig.ORDERING_FIELDS.key()));

    options.put(ORDERING_FIELDS.key(), "ts_3");
    props = createTableAndReturnTableProperties(options, new ObjectPath(db, "tmptb2"));
    assertTrue(props.containsKey(HoodieTableConfig.ORDERING_FIELDS.key()));
    assertEquals("ts_3", props.get(HoodieTableConfig.ORDERING_FIELDS.key()));
  }

  private TypedProperties createTableAndReturnTableProperties(Map<String, String> options, ObjectPath tablePath)
      throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
    CatalogTable table =
        CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    hoodieCatalog.createTable(tablePath, table, true);

    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(
        createStorageConf(), hoodieCatalog.inferTablePath(tablePath, table));
    return metaClient.getTableConfig().getProps();
  }

  @Test
  public void testCreateExternalTable() throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException, IOException {
    HoodieHiveCatalog catalog = HoodieCatalogTestUtils.createHiveCatalog("myCatalog", true);
    catalog.open();
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable table =
        CatalogUtils.createCatalogTable(schema, Collections.emptyList(), originOptions, "hudi table");
    catalog.createTable(tablePath, table, false);
    Table table1 = catalog.getHiveTable(tablePath);
    assertTrue(Boolean.parseBoolean(table1.getParameters().get("EXTERNAL")));
    assertEquals("EXTERNAL_TABLE", table1.getTableType());

    catalog.dropTable(tablePath, false);
    StoragePath path = new StoragePath(table1.getParameters().get(FlinkOptions.PATH.key()));
    boolean created = StreamerUtil.fileExists(HoodieTestUtils.getStorage(path), path);
    assertTrue(created, "Table should have been created");
  }

  @Test
  public void testCreateNonHoodieTable() throws TableAlreadyExistException, DatabaseNotExistException {
    CatalogTable table = CatalogUtils.createCatalogTable(
        schema, Collections.emptyList(), Collections.singletonMap(FactoryUtil.CONNECTOR.key(), "hudi-fake"), "hudi table");
    try {
      hoodieCatalog.createTable(tablePath, table, false);
    } catch (HoodieCatalogException e) {
      assertEquals("Unsupported connector identity hudi-fake, supported identity is hudi", e.getMessage());
    }
  }

  @Test
  public void testCreateHoodieTableWithWrongTableType() {
    HashMap<String,String> properties = new HashMap<>();
    properties.put(FactoryUtil.CONNECTOR.key(), "hudi");
    properties.put("table.type","wrong type");
    CatalogTable table = CatalogUtils.createCatalogTable(schema, Collections.emptyList(), properties, "hudi table");
    assertThrows(HoodieCatalogException.class, () -> hoodieCatalog.createTable(tablePath, table, false));
  }

  @Test
  public void testLanceFormatNotSupportedByHiveCatalog() {
    HashMap<String, String> properties = new HashMap<>();
    properties.put(FactoryUtil.CONNECTOR.key(), "hudi");
    properties.put(HoodieTableConfig.BASE_FILE_FORMAT.key(), "LANCE");
    CatalogTable table = CatalogUtils.createCatalogTable(schema, Collections.emptyList(), properties, "lance table");
    // createTable wraps the HoodieValidationException in a HoodieCatalogException
    HoodieCatalogException ex = assertThrows(HoodieCatalogException.class,
        () -> hoodieCatalog.createTable(tablePath, table, false));
    assertThat(ex.getCause(), instanceOf(HoodieValidationException.class));
    assertThat(ex.getCause().getMessage(), containsString("Lance base file format is not supported by this reader or writer path"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDropTable(boolean external) throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException, IOException {
    HoodieHiveCatalog catalog = HoodieCatalogTestUtils.createHiveCatalog("myCatalog", external);
    catalog.open();

    CatalogTable catalogTable = CatalogUtils.createCatalogTable(
        schema, Collections.emptyList(), Collections.singletonMap(FactoryUtil.CONNECTOR.key(), "hudi"), "hudi table");
    catalog.createTable(tablePath, catalogTable, false);
    Table table = catalog.getHiveTable(tablePath);
    assertEquals(external, Boolean.parseBoolean(table.getParameters().get("EXTERNAL")));

    catalog.dropTable(tablePath, false);
    StoragePath path = new StoragePath(table.getParameters().get(FlinkOptions.PATH.key()));
    boolean existing = StreamerUtil.fileExists(HoodieTestUtils.getStorage(path), path);
    assertEquals(external, existing);
  }

  @Test
  public void testAlterTable() throws Exception {
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable originTable = CatalogUtils.createCatalogTable(schema, partitions, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, originTable, false);

    Table hiveTable = hoodieCatalog.getHiveTable(tablePath);
    Map<String, String> newOptions = hiveTable.getParameters();
    newOptions.put("k", "v");
    CatalogTable newTable = CatalogUtils.createCatalogTable(schema, partitions, newOptions, "alter hudi table");
    hoodieCatalog.alterTable(tablePath, newTable, false);

    hiveTable = hoodieCatalog.getHiveTable(tablePath);
    assertEquals(hiveTable.getParameters().get(CONNECTOR.key()), "hudi");
    assertEquals(hiveTable.getParameters().get("k"), "v");
  }

  @Test
  public void testRenameTable() throws Exception {
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    originOptions.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    originOptions.put(HoodieTableConfig.TABLE_STORAGE_LAYOUT.key(),
        HoodieTableConfig.TableStorageLayout.LSM_TREE.configValue());
    CatalogTable originTable = CatalogUtils.createCatalogTable(schema, partitions, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, originTable, false);

    hoodieCatalog.renameTable(tablePath, "test1", false);

    ObjectPath renamedTablePath = new ObjectPath("default", "test1");
    Table renamedHiveTable = hoodieCatalog.getHiveTable(renamedTablePath);
    assertEquals("test1", renamedHiveTable.getTableName());

    HoodieTableMetaClient renamedMetaClient = HoodieTestUtils.createMetaClient(
        createStorageConf(), renamedHiveTable.getSd().getLocation());
    assertEquals(HoodieTableConfig.TableStorageLayout.LSM_TREE,
        renamedMetaClient.getTableConfig().getTableStorageLayout());

    hoodieCatalog.renameTable(renamedTablePath, "test", false);
  }

  @Test
  public void testDropPartition() throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable table = CatalogUtils.createCatalogTable(schema, partitions, options, "hudi table");
    hoodieCatalog.createTable(tablePath, table, false);

    CatalogPartitionSpec partitionSpec = new CatalogPartitionSpec(new HashMap<String, String>() {
      {
        put("par1", "20221020");
      }
    });
    // drop non-exist partition
    assertThrows(PartitionNotExistException.class,
        () -> hoodieCatalog.dropPartition(tablePath, partitionSpec, false));

    Table hiveTable = hoodieCatalog.getHiveTable(tablePath);
    StorageDescriptor partitionSd = new StorageDescriptor(hiveTable.getSd());
    partitionSd.setLocation(new Path(partitionSd.getLocation(), HoodieCatalogUtil.inferPartitionPath(true, partitionSpec)).toString());
    hoodieCatalog.getClient().add_partition(new Partition(Collections.singletonList("20221020"),
        tablePath.getDatabaseName(), tablePath.getObjectName(), 0, 0, partitionSd, null));
    assertNotNull(getHivePartition(partitionSpec));

    // drop partition 'par1'
    hoodieCatalog.dropPartition(tablePath, partitionSpec, false);

    String tablePathStr = hoodieCatalog.inferTablePath(tablePath, hoodieCatalog.getTable(tablePath));
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(hoodieCatalog.getHiveConf()), tablePathStr);
    HoodieInstant latestInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().orElse(null);
    assertNotNull(latestInstant, "Delete partition commit should be completed");
    HoodieCommitMetadata commitMetadata = WriteProfiles.getCommitMetadata(tablePath.getObjectName(), new org.apache.flink.core.fs.Path(tablePathStr),
        latestInstant, metaClient.getActiveTimeline());
    assertThat(commitMetadata, instanceOf(HoodieReplaceCommitMetadata.class));
    HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
    assertThat(replaceCommitMetadata.getPartitionToReplaceFileIds().size(), is(1));
    assertThrows(NoSuchObjectException.class, () -> getHivePartition(partitionSpec));
  }

  @Test
  public void testMappingHiveConfPropsToHiveTableParams() throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException {
    HoodieHiveCatalog catalog = HoodieCatalogTestUtils.createHiveCatalog("myCatalog", true);
    catalog.open();
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable table = CatalogUtils.createCatalogTable(schema, Collections.emptyList(), originOptions, "hudi table");
    catalog.createTable(tablePath, table, false);

    Table hiveTable = hoodieCatalog.getHiveTable(tablePath);
    assertEquals("false", hiveTable.getParameters().get("hadoop.hive.metastore.sasl.enabled"));
  }

  @Test
  public void checkParameterSemantic() throws TableAlreadyExistException, DatabaseNotExistException {
    HoodieHiveCatalog catalog = HoodieCatalogTestUtils.createHiveCatalog("myCatalog", true);
    catalog.open();
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");

    // validate pk: same quantity but different values
    String pkError = String.format("Primary key fields definition has inconsistency between pk statement and option '%s'",
        FlinkOptions.RECORD_KEY_FIELD.key());
    originOptions.put(FlinkOptions.RECORD_KEY_FIELD.key(), "name");
    CatalogTable pkTable = CatalogUtils.createCatalogTable(schema, partitions, originOptions, "hudi table");
    assertThrows(HoodieValidationException.class, () -> catalog.createTable(tablePath, pkTable, false), pkError);
    originOptions.remove(FlinkOptions.RECORD_KEY_FIELD.key());

    // validate pk: the pk field exist in options but not in pk statement.
    originOptions.put(FlinkOptions.RECORD_KEY_FIELD.key(), "uuid,name");
    CatalogTable pkTable1 = CatalogUtils.createCatalogTable(schema, partitions, originOptions, "hudi table");
    assertThrows(HoodieValidationException.class, () -> catalog.createTable(tablePath, pkTable1, false), pkError);
    originOptions.remove(FlinkOptions.RECORD_KEY_FIELD.key());

    // validate partition key: same quantity but different values
    String partitionKeyError = String.format("Partition key fields definition has inconsistency between partition key statement and option '%s'",
        FlinkOptions.PARTITION_PATH_FIELD.key());
    originOptions.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "name");
    CatalogTable partitionKeytable = CatalogUtils.createCatalogTable(schema, partitions, originOptions, "hudi table");
    assertThrows(HoodieValidationException.class, () -> catalog.createTable(tablePath, partitionKeytable, false), partitionKeyError);
    originOptions.remove(FlinkOptions.PARTITION_PATH_FIELD.key());

    // validate partition key: the partition key field exist in options but not in partition key statement.
    originOptions.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "par1,name");
    CatalogTable partitionKeytable1 = CatalogUtils.createCatalogTable(schema, partitions, originOptions, "hudi table");
    assertThrows(HoodieValidationException.class, () -> catalog.createTable(tablePath, partitionKeytable1, false), partitionKeyError);
    originOptions.remove(FlinkOptions.PARTITION_PATH_FIELD.key());
  }

  private Partition getHivePartition(CatalogPartitionSpec partitionSpec) throws Exception {
    return hoodieCatalog.getClient().getPartition(
        tablePath.getDatabaseName(),
        tablePath.getObjectName(),
        HoodieCatalogUtil.getOrderedPartitionValues(
            hoodieCatalog.getName(), hoodieCatalog.getHiveConf(), partitionSpec, partitions, tablePath));
  }

  @Override
  AbstractCatalog getCatalog() {
    return hoodieCatalog;
  }
}
