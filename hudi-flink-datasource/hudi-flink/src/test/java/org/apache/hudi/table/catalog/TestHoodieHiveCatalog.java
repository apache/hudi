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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieCatalogException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hudi.util.StreamerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieHiveCatalog}.
 */
public class TestHoodieHiveCatalog {
  TableSchema schema =
      TableSchema.builder()
          .field("uuid", DataTypes.INT().notNull())
          .field("name", DataTypes.STRING())
          .field("age", DataTypes.INT())
          .field("par1", DataTypes.STRING())
          .field("ts", DataTypes.BIGINT())
          .primaryKey("uuid")
          .build();
  List<String> partitions = Collections.singletonList("par1");
  private static HoodieHiveCatalog hoodieCatalog;
  private final ObjectPath tablePath = new ObjectPath("default", "test");

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

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testCreateAndGetHoodieTable(HoodieTableType tableType) throws Exception {
    Map<String, String> options = new HashMap<>();
    options.put(FactoryUtil.CONNECTOR.key(), "hudi");
    options.put(FlinkOptions.TABLE_TYPE.key(), tableType.toString());

    CatalogTable table =
        new CatalogTableImpl(schema, partitions, options, "hudi table");
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
        + "ts:bigint";
    assertEquals(expectedFieldSchema, fieldSchema);
    String partitionSchema = hiveTable.getPartitionKeys().stream()
        .map(f -> f.getName() + ":" + f.getType())
        .collect(Collectors.joining(","));
    assertEquals("par1:string", partitionSchema);

    // validate catalog table
    CatalogBaseTable table1 = hoodieCatalog.getTable(tablePath);
    assertEquals("hudi", table1.getOptions().get(CONNECTOR.key()));
    assertEquals(tableType.toString(), table1.getOptions().get(FlinkOptions.TABLE_TYPE.key()));
    assertEquals("uuid", table1.getOptions().get(FlinkOptions.RECORD_KEY_FIELD.key()));
    assertNull(table1.getOptions().get(FlinkOptions.PRECOMBINE_FIELD.key()), "preCombine key is not declared");
    String tableSchema = table1.getUnresolvedSchema().getColumns().stream()
        .map(Schema.UnresolvedColumn::toString)
        .collect(Collectors.joining(","));
    String expectedTableSchema = "`uuid` INT NOT NULL,`name` STRING,`age` INT,`par1` STRING,`ts` BIGINT";
    assertEquals(expectedTableSchema, tableSchema);
    assertEquals(Collections.singletonList("uuid"), table1.getUnresolvedSchema().getPrimaryKey().get().getColumnNames());
    assertEquals(Collections.singletonList("par1"), ((CatalogTable)table1).getPartitionKeys());

    // validate explicit primary key
    options.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
    table = new CatalogTableImpl(schema, partitions, options, "hudi table");
    hoodieCatalog.alterTable(tablePath, table, true);

    CatalogBaseTable table2 = hoodieCatalog.getTable(tablePath);
    assertEquals("id", table2.getOptions().get(FlinkOptions.RECORD_KEY_FIELD.key()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateExternalTable(boolean isExternal) throws TableAlreadyExistException, DatabaseNotExistException, TableNotExistException, IOException {
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    originOptions.put(CatalogOptions.TABLE_EXTERNAL.key(), String.valueOf(isExternal));
    CatalogTable table =
        new CatalogTableImpl(schema, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, table, false);
    Table table1 = hoodieCatalog.getHiveTable(tablePath);
    if (isExternal) {
      assertTrue(Boolean.parseBoolean(table1.getParameters().get(CatalogOptions.TABLE_EXTERNAL.key())));
      assertEquals("EXTERNAL_TABLE", table1.getTableType());
    } else {
      assertFalse(Boolean.parseBoolean(table1.getParameters().get(CatalogOptions.TABLE_EXTERNAL.key())));
      assertEquals("MANAGED_TABLE", table1.getTableType());
    }

    hoodieCatalog.dropTable(tablePath, false);
    Path path = new Path(table1.getParameters().get(FlinkOptions.PATH.key()));
    boolean exists = StreamerUtil.fileExists(FileSystem.getLocal(new Configuration()), path);
    assertTrue(isExternal && exists || !isExternal && !exists);
  }

  @Test
  public void testCreateNonHoodieTable() throws TableAlreadyExistException, DatabaseNotExistException {
    CatalogTable table =
        new CatalogTableImpl(schema, Collections.emptyMap(), "hudi table");
    try {
      hoodieCatalog.createTable(tablePath, table, false);
    } catch (HoodieCatalogException e) {
      assertEquals(String.format("The %s is not hoodie table", tablePath.getObjectName()), e.getMessage());
    }
  }

  @Test
  public void testAlterTable() throws Exception {
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable originTable =
        new CatalogTableImpl(schema, partitions, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, originTable, false);

    Table hiveTable = hoodieCatalog.getHiveTable(tablePath);
    Map<String, String> newOptions = hiveTable.getParameters();
    newOptions.put("k", "v");
    CatalogTable newTable = new CatalogTableImpl(schema, partitions, newOptions, "alter hudi table");
    hoodieCatalog.alterTable(tablePath, newTable, false);

    hiveTable = hoodieCatalog.getHiveTable(tablePath);
    assertEquals(hiveTable.getParameters().get(CONNECTOR.key()), "hudi");
    assertEquals(hiveTable.getParameters().get("k"), "v");
  }

  @Test
  public void testRenameTable() throws Exception {
    Map<String, String> originOptions = new HashMap<>();
    originOptions.put(FactoryUtil.CONNECTOR.key(), "hudi");
    CatalogTable originTable =
        new CatalogTableImpl(schema, partitions, originOptions, "hudi table");
    hoodieCatalog.createTable(tablePath, originTable, false);

    hoodieCatalog.renameTable(tablePath, "test1", false);

    assertEquals(hoodieCatalog.getHiveTable(new ObjectPath("default", "test1")).getTableName(), "test1");

    hoodieCatalog.renameTable(new ObjectPath("default", "test1"), "test", false);
  }
}
