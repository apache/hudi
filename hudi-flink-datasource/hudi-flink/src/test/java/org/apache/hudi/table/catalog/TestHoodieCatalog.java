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

import org.apache.hudi.configuration.FlinkOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.table.catalog.CatalogOptions.CATALOG_PATH;
import static org.apache.hudi.table.catalog.CatalogOptions.DEFAULT_DATABASE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieCatalog}.
 */
public class TestHoodieCatalog {

  private static final String TEST_DEFAULT_DATABASE = "test_db";
  private static final String NONE_EXIST_DATABASE = "none_exist_database";
  private static final List<Column> CREATE_COLUMNS = Arrays.asList(
      Column.physical("uuid", DataTypes.VARCHAR(20)),
      Column.physical("name", DataTypes.VARCHAR(20)),
      Column.physical("age", DataTypes.INT()),
      Column.physical("tss", DataTypes.TIMESTAMP(3)),
      Column.physical("partition", DataTypes.VARCHAR(10))
  );
  private static final UniqueConstraint CONSTRAINTS = UniqueConstraint.primaryKey("uuid", Arrays.asList("uuid"));
  private static final ResolvedSchema CREATE_TABLE_SCHEMA =
      new ResolvedSchema(
          CREATE_COLUMNS,
          Collections.emptyList(),
          CONSTRAINTS);

  private static final List<Column> EXPECTED_TABLE_COLUMNS =
      CREATE_COLUMNS.stream()
          .map(
              col -> {
                // Flink char/varchar is transform to string in avro.
                if (col.getDataType()
                    .getLogicalType()
                    .getTypeRoot()
                    .equals(LogicalTypeRoot.VARCHAR)) {
                  return Column.physical(col.getName(), DataTypes.STRING());
                } else {
                  return col;
                }
              })
          .collect(Collectors.toList());
  private static final ResolvedSchema EXPECTED_TABLE_SCHEMA =
      new ResolvedSchema(EXPECTED_TABLE_COLUMNS, Collections.emptyList(), CONSTRAINTS);

  private static final Map<String, String> EXPECTED_OPTIONS = new HashMap<>();

  static {
    EXPECTED_OPTIONS.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    EXPECTED_OPTIONS.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    EXPECTED_OPTIONS.put(FlinkOptions.PRE_COMBINE.key(), "true");
  }

  private static final ResolvedCatalogTable EXPECTED_CATALOG_TABLE = new ResolvedCatalogTable(
      CatalogTable.of(
          Schema.newBuilder().fromResolvedSchema(CREATE_TABLE_SCHEMA).build(),
          "test",
          Arrays.asList("partition"),
          EXPECTED_OPTIONS),
      CREATE_TABLE_SCHEMA
  );

  private TableEnvironment streamTableEnv;
  private HoodieCatalog catalog;

  @TempDir
  File tempFile;

  @BeforeEach
  void beforeEach() {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
    streamTableEnv = TableEnvironmentImpl.create(settings);
    streamTableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
    File testDb = new File(tempFile, TEST_DEFAULT_DATABASE);
    testDb.mkdir();
    Map<String, String> catalogOptions = new HashMap<>();
    catalogOptions.put(CATALOG_PATH.key(), tempFile.getAbsolutePath());
    catalogOptions.put(DEFAULT_DATABASE.key(), TEST_DEFAULT_DATABASE);
    catalog = new HoodieCatalog("hudi", Configuration.fromMap(catalogOptions));
    catalog.open();
  }

  @Test
  public void testListDatabases() {
    List<String> actual = catalog.listDatabases();
    assertTrue(actual.contains(TEST_DEFAULT_DATABASE));
    assertFalse(actual.contains(NONE_EXIST_DATABASE));
  }

  @Test
  public void testDatabaseExists() {
    assertTrue(catalog.databaseExists(TEST_DEFAULT_DATABASE));
    assertFalse(catalog.databaseExists(NONE_EXIST_DATABASE));
  }

  @Test
  public void testCreateAndDropDatabase() throws Exception {
    CatalogDatabase expected = new CatalogDatabaseImpl(Collections.emptyMap(), null);
    catalog.createDatabase("db1", expected, true);

    CatalogDatabase actual = catalog.getDatabase("db1");
    assertTrue(catalog.listDatabases().contains("db1"));
    assertEquals(expected.getProperties(), actual.getProperties());

    // create exist database
    assertThrows(DatabaseAlreadyExistException.class,
        () -> catalog.createDatabase("db1", expected, false));

    // drop exist database
    catalog.dropDatabase("db1", true);
    assertFalse(catalog.listDatabases().contains("db1"));

    // drop non-exist database
    assertThrows(DatabaseNotExistException.class,
        () -> catalog.dropDatabase(NONE_EXIST_DATABASE, false));
  }

  @Test
  public void testCreateDatabaseWithOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("k1", "v1");
    options.put("k2", "v2");

    assertThrows(
        CatalogException.class,
        () -> catalog.createDatabase("db1", new CatalogDatabaseImpl(options, null), true));
  }

  @Test
  public void testCreateTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // test create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    // test table exist
    assertTrue(catalog.tableExists(tablePath));

    // test create exist table
    assertThrows(TableAlreadyExistException.class,
        () -> catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, false));
  }

  @Test
  public void testListTable() throws Exception {
    ObjectPath tablePath1 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    ObjectPath tablePath2 = new ObjectPath(TEST_DEFAULT_DATABASE, "tb2");

    // create table
    catalog.createTable(tablePath1, EXPECTED_CATALOG_TABLE, true);
    catalog.createTable(tablePath2, EXPECTED_CATALOG_TABLE, true);

    // test list table
    List<String> tables = catalog.listTables(TEST_DEFAULT_DATABASE);
    assertTrue(tables.contains(tablePath1.getObjectName()));
    assertTrue(tables.contains(tablePath2.getObjectName()));

    // test list non-exist database table
    assertThrows(DatabaseNotExistException.class,
        () -> catalog.listTables(NONE_EXIST_DATABASE));
  }

  @Test
  public void testGetTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    Map<String, String> expectedOptions = new HashMap<>(EXPECTED_OPTIONS);
    expectedOptions.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    expectedOptions.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    expectedOptions.put(FlinkOptions.PRE_COMBINE.key(), "true");
    expectedOptions.put("connector", "hudi");
    expectedOptions.put(
        FlinkOptions.PATH.key(),
        String.format("%s/%s/%s", tempFile.getAbsolutePath(), tablePath.getDatabaseName(), tablePath.getObjectName()));

    // test get table
    CatalogBaseTable actualTable = catalog.getTable(tablePath);
    // validate schema
    Schema actualSchema = actualTable.getUnresolvedSchema();
    Schema expectedSchema = Schema.newBuilder().fromResolvedSchema(EXPECTED_TABLE_SCHEMA).build();
    assertEquals(expectedSchema, actualSchema);
    // validate options
    Map<String, String> actualOptions = actualTable.getOptions();
    assertEquals(expectedOptions, actualOptions);
    // validate comment
    assertEquals(EXPECTED_CATALOG_TABLE.getComment(), actualTable.getComment());
    // validate partition key
    assertEquals(EXPECTED_CATALOG_TABLE.getPartitionKeys(), ((CatalogTable) actualTable).getPartitionKeys());
  }

  @Test
  public void dropTable() throws Exception {
    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // create table
    catalog.createTable(tablePath, EXPECTED_CATALOG_TABLE, true);

    // test drop table
    catalog.dropTable(tablePath, true);
    assertFalse(catalog.tableExists(tablePath));

    // drop non-exist table
    assertThrows(TableNotExistException.class,
        () -> catalog.dropTable(new ObjectPath(TEST_DEFAULT_DATABASE, "non_exist"), false));
  }
}
