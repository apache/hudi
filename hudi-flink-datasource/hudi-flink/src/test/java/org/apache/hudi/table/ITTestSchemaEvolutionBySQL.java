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

package org.apache.hudi.table;

import org.apache.hudi.adapter.TestHoodieCatalogs;
import org.apache.hudi.exception.HoodieCatalogException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.hudi.utils.TestData.assertRowsEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * IT cases for schema evolution by alter table SQL using catalog.
 */
@EnabledIf("supportAdvancedAlterTableSyntax")
@ExtendWith(FlinkMiniCluster.class)
public abstract class ITTestSchemaEvolutionBySQL {
  protected static final String CATALOG_NAME = "hudi_catalog";
  protected static final String DB_NAME = "hudi";
  private TableEnvironment tableEnv;
  protected Catalog catalog;

  private static final String CREATE_TABLE_DDL = ""
      + "create table t1("
      + "  f_int int,"
      + "  f_date date,"
      + "  f_str string,"
      + "  f_par string,"
      + "  primary key(f_int) not enforced"
      + ")"
      + "partitioned by (`f_par`)"
      + "with ("
      + "  'connector' = 'hudi',"
      + "  'hoodie.datasource.write.recordkey.field' = 'f_int',"
      + "  'hoodie.schema.on.read.enable' = 'true',"
      + "  'connector' = 'hudi',"
      + "  'precombine.field' = 'f_date'"
      + ")";

  private static final String INITIALIZE_INSERT_SQL = ""
      + "insert into t1 values "
      + "(1, TO_DATE('2022-02-02'), 'first', '1'), "
      + "(2, DATE '2022-02-02', 'second', '2')";

  @BeforeEach
  void beforeEach() {
    tableEnv = TestTableEnvs.getBatchTableEnv();
    tableEnv.getConfig().getConfiguration()
        .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
    catalog = createCatalog();
    catalog.open();
    tableEnv.registerCatalog(CATALOG_NAME, catalog);
    tableEnv.executeSql("use catalog " + CATALOG_NAME);
    tableEnv.executeSql("create database if not exists " + DB_NAME);
    tableEnv.executeSql("use " + DB_NAME);
    tableEnv.executeSql(CREATE_TABLE_DDL);
  }

  @AfterEach
  void afterEach() {
    if (catalog != null) {
      catalog.close();
    }
  }

  @Test
  void testAddColumns() {
    execInsertSql(tableEnv, INITIALIZE_INSERT_SQL);

    String alterSql = "alter table t1 add (f_name string after f_date, f_long bigint)";
    tableEnv.executeSql(alterSql);

    String newInsertSql = ""
        + "insert into t1 values "
        + "(3, TO_DATE('2022-02-02'), 'Hi', 'third', '3', 1000), "
        + "(1, TO_DATE('2022-02-02'), 'Hello', 'first', '1', 500)";
    execInsertSql(tableEnv, newInsertSql);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    String expected = "["
        + "+I[1, 2022-02-02, Hello, first, 1, 500], "
        + "+I[2, 2022-02-02, null, second, 2, null], "
        + "+I[3, 2022-02-02, Hi, third, 3, 1000]]";
    assertRowsEquals(result, expected);
  }

  @Test
  void testDropColumns() {
    execInsertSql(tableEnv, INITIALIZE_INSERT_SQL);

    String alterSql = "alter table t1 drop f_date";
    tableEnv.executeSql(alterSql);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    String expected = "["
        + "+I[1, first, 1], "
        + "+I[2, second, 2]]";
    assertRowsEquals(result, expected);
  }

  @Test
  void testRenameColumns() {
    execInsertSql(tableEnv, INITIALIZE_INSERT_SQL);

    String alterSql = "alter table t1 rename f_str to f_string";
    tableEnv.executeSql(alterSql);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select f_int, f_date, f_string, f_par from t1").execute().collect());
    String expected = "["
        + "+I[1, 2022-02-02, first, 1], "
        + "+I[2, 2022-02-02, second, 2]]";
    assertRowsEquals(result, expected);
  }

  @Test
  void testModifyColumn() {
    execInsertSql(tableEnv, INITIALIZE_INSERT_SQL);
    String alterSql = "alter table t1 modify (f_int bigint, f_str string after f_par)";
    tableEnv.executeSql(alterSql);

    String newInsertSql = ""
        + "insert into t1 values "
        + "(3, TO_DATE('2022-02-02'), '3', 'third'), "
        + "(1, TO_DATE('2022-02-02'), '1', 'first1')";
    execInsertSql(tableEnv, newInsertSql);

    List<Row> result = CollectionUtil.iterableToList(
        () -> tableEnv.sqlQuery("select * from t1").execute().collect());
    String expected = "["
        + "+I[1, 2022-02-02, 1, first1], "
        + "+I[2, 2022-02-02, 2, second], "
        + "+I[3, 2022-02-02, 3, third]]";
    assertRowsEquals(result, expected);
  }

  @Test
  void testIllegalModifyColumnType() {
    // Modify column type now only support:
    // int => long/float/double/String/Decimal
    // long => float/double/String/Decimal
    // float => double/String/Decimal
    // double => String/Decimal
    // Decimal => Decimal/String
    // String => date/decimal
    // date => String
    execInsertSql(tableEnv, INITIALIZE_INSERT_SQL);
    String alterSql = "alter table t1 modify f_str int";
    Exception e = assertThrows(
        TableException.class,
        () -> tableEnv.executeSql(alterSql),
        "Should throw exception when the type update is not allowed ");
    assertTrue(e.getCause() instanceof SchemaCompatibilityException);
    assertTrue(e.getCause().getMessage().contains("Cannot update column 'f_str' from type 'string' to incompatible type 'int'."),
        e.getCause().getMessage());
  }

  @Test
  void testSetAndResetProperty() throws Exception {
    tableEnv.executeSql("alter table t1 set ('k' = 'v')");
    CatalogBaseTable table = catalog.getTable(new ObjectPath("hudi", "t1"));
    assertEquals(table.getOptions().get("k"), "v");

    tableEnv.executeSql("alter table t1 reset ('k')");
    table = catalog.getTable(new ObjectPath("hudi", "t1"));
    assertFalse(table.getOptions().containsKey("k"));
  }

  @Test
  void testAlterTableType() {
    // Alter table type is not supported
    Exception e = assertThrows(
        TableException.class,
        () -> tableEnv.executeSql("alter table t1 set ('table.type' = 'MERGE_ON_READ')"),
        "Should throw exception because alter table type is not supported.");
    assertTrue(e.getCause() instanceof HoodieCatalogException);
    assertTrue(e.getCause().getMessage().contains(
        "Hoodie catalog does not support to alter table type and index type"));
  }

  @Test
  void testAlterIndexType() {
    // Alter index type is not supported
    Exception e = assertThrows(
        TableException.class,
        () -> tableEnv.executeSql("alter table t1 set ('index.type' = 'BUCKET')"),
        "Should throw exception because alter index type is not supported.");
    assertTrue(e.getCause() instanceof HoodieCatalogException);
    assertTrue(e.getCause().getMessage().contains(
        "Hoodie catalog does not support to alter table type and index type"));
  }

  @Test
  void testAddNonPhysicalColumn() {
    // Add non-physical column is not supported
    Exception e = assertThrows(
        TableException.class,
        () -> tableEnv.executeSql("alter table t1 add (ts AS f_int + 1)"),
        "Should throw exception because add non-physical column is not supported.");
    assertTrue(e.getCause() instanceof HoodieNotSupportedException);
    assertTrue(e.getCause().getMessage().contains("Add non-physical column is not supported yet."));
  }

  @Test
  void testDropPrimaryKeyConstraint() {
    // Drop primary key constraint is not supported
    Exception e = assertThrows(
        TableException.class,
        () -> tableEnv.executeSql("alter table t1 drop primary key"),
        "Should throw exception because DropConstraint is not supported.");
    assertTrue(e.getCause() instanceof HoodieNotSupportedException);
    assertTrue(e.getCause().getMessage().contains("DropConstraint is not supported."));
  }

  @Test
  void testAddWatermark() {
    Exception e = assertThrows(
        TableException.class,
        () -> tableEnv.executeSql("alter table t1 add (ts timestamp(3), watermark for ts as ts - interval '1' hour)"),
        "Should throw exception because AddWatermark is not supported.");
    assertTrue(e.getCause() instanceof HoodieNotSupportedException);
    assertTrue(e.getCause().getMessage().contains("AddWatermark is not supported."));
  }

  static boolean supportAdvancedAlterTableSyntax() {
    return TestHoodieCatalogs.supportAdvancedAlterTableSyntax();
  }

  private void execInsertSql(TableEnvironment tEnv, String insert) {
    TableResult tableResult = tEnv.executeSql(insert);
    // wait to finish
    try {
      tableResult.await();
    } catch (InterruptedException | ExecutionException ex) {
      // ignored
    }
  }

  protected abstract Catalog createCatalog();
}
