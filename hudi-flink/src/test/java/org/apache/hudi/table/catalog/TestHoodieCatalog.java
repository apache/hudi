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
  private final List<Column> createColumns = Arrays.asList(
      Column.physical("uuid", DataTypes.VARCHAR(20)),
      Column.physical("name", DataTypes.VARCHAR(20)),
      Column.physical("age", DataTypes.INT()),
      Column.physical("tss", DataTypes.TIMESTAMP(3)),
      Column.physical("partition", DataTypes.VARCHAR(10))
  );
  private final UniqueConstraint constraints = UniqueConstraint.primaryKey("uuid", Arrays.asList("uuid"));
  private final ResolvedSchema createTableSchema =
      new ResolvedSchema(
          createColumns,
          Collections.emptyList(),
          constraints);

  private final List<Column> expectedTableColumns =
      createColumns.stream()
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
  private final ResolvedSchema expectedTableSchema =
      new ResolvedSchema(expectedTableColumns, Collections.emptyList(), constraints);

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
    File db1 = new File(tempFile, TEST_DEFAULT_DATABASE);
    db1.mkdir();
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

    catalog.dropDatabase("db1", true);
    assertFalse(catalog.listDatabases().contains("db1"));
  }

  @Test
  public void testCreateDatabaseWithOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("k1", "v1");
    options.put("k2", "v2");

    assertThrows(
        CatalogException.class,
        () -> catalog.createDatabase("db1", new CatalogDatabaseImpl(options, null), true),
        "Hudi catalog doesn't support to create database with options."
    );
  }

  @Test
  public void testTableRelatedMethod() throws Exception {
    Map<String, String> expectedOptions = new HashMap<>();
    expectedOptions.put(FlinkOptions.TABLE_TYPE.key(), FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    expectedOptions.put(FlinkOptions.INDEX_GLOBAL_ENABLED.key(), "false");
    expectedOptions.put(FlinkOptions.PRE_COMBINE.key(), "true");

    ResolvedCatalogTable expectedCatalogTable = new ResolvedCatalogTable(
        CatalogTable.of(
            Schema.newBuilder().fromResolvedSchema(createTableSchema).build(),
            "test",
            Arrays.asList("partition"),
            expectedOptions),
        createTableSchema
    );

    ObjectPath tablePath = new ObjectPath(TEST_DEFAULT_DATABASE, "tb1");
    // test create table
    catalog.createTable(tablePath, expectedCatalogTable, true);

    // test table exist
    assertTrue(catalog.tableExists(tablePath));

    // test list table
    assertTrue(catalog.listTables(TEST_DEFAULT_DATABASE).contains(tablePath.getObjectName()));

    // test get table
    CatalogBaseTable actualTable = catalog.getTable(tablePath);
    // validate schema
    Schema actualSchema = actualTable.getUnresolvedSchema();
    Schema expectedSchema = Schema.newBuilder().fromResolvedSchema(expectedTableSchema).build();
    assertEquals(expectedSchema, actualSchema);
    // validate options
    expectedOptions.put("connector", "hudi");
    expectedOptions.put(
        FlinkOptions.PATH.key(),
        String.format("%s/%s/%s", tempFile.getAbsolutePath(), tablePath.getDatabaseName(), tablePath.getObjectName()));
    Map<String, String> actualOptions = actualTable.getOptions();
    assertEquals(expectedOptions, actualOptions);
    // validate comment
    assertEquals(expectedCatalogTable.getComment(), actualTable.getComment());
    // validate partition key
    assertEquals(expectedCatalogTable.getPartitionKeys(),((CatalogTable) actualTable).getPartitionKeys());

    // test drop table
    catalog.dropTable(tablePath, true);
    assertFalse(catalog.tableExists(tablePath));
  }
}
