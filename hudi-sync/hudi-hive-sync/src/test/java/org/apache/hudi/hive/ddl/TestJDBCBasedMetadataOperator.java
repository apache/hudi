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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.hudi.sync.common.model.Partition;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link JDBCBasedMetadataOperator}.
 *
 * <p>Uses Mockito to simulate JDBC responses without requiring
 * a live HiveServer2 instance.
 */
class TestJDBCBasedMetadataOperator {

  private Connection mockConnection;
  private Statement mockStatement;
  private ResultSet mockResultSet;
  private JDBCBasedMetadataOperator operator;

  @BeforeEach
  void setUp() throws Exception {
    mockConnection = mock(Connection.class);
    mockStatement = mock(Statement.class);
    mockResultSet = mock(ResultSet.class);
    when(mockConnection.createStatement()).thenReturn(mockStatement);
    operator = new JDBCBasedMetadataOperator(mockConnection, "test_db");
  }

  @Test
  void testTableExistsReturnsTrue() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);

    assertTrue(operator.tableExists("my_table"));
  }

  @Test
  void testTableExistsReturnsFalse() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);

    assertFalse(operator.tableExists("nonexistent"));
  }

  @Test
  void testDatabaseExists() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);

    assertTrue(operator.databaseExists("test_db"));
  }

  @Test
  void testGetFieldSchemas() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(1)).thenReturn("id", "name");
    when(mockResultSet.getString(2)).thenReturn("string", "string");
    when(mockResultSet.getString(3)).thenReturn(null, "user name");

    List<FieldSchema> fields = operator.getFieldSchemas("my_table");
    assertEquals(2, fields.size());
    assertEquals("id", fields.get(0).getName());
    assertEquals("string", fields.get(0).getType());
    assertEquals("name", fields.get(1).getName());
  }

  @Test
  void testGetTableProperty() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(2)).thenReturn("20260219");

    Option<String> value = operator.getTableProperty("my_table", "last_commit");
    assertTrue(value.isPresent());
    assertEquals("20260219", value.get());
  }

  @Test
  void testGetTablePropertyMissing() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getString(2)).thenReturn("Table my_table does not exist");

    Option<String> value = operator.getTableProperty("my_table", "missing_key");
    assertFalse(value.isPresent());
  }

  @Test
  void testSetTableProperties() throws Exception {
    when(mockStatement.execute(anyString())).thenReturn(true);

    operator.setTableProperties("my_table", Map.of("key1", "val1", "key2", "val2"));
    verify(mockStatement).execute(anyString());
  }

  @Test
  void testGetAllPartitions() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, false);
    when(mockResultSet.getString(1))
        .thenReturn("datestamp=2025-01-15", "datestamp=2025-01-16");

    List<Partition> partitions = operator.getAllPartitions(
        "my_table", "s3a://bucket/table");
    assertEquals(2, partitions.size());
    assertEquals("2025-01-15", partitions.get(0).getValues().get(0));
    assertEquals("s3a://bucket/table/datestamp=2025-01-15",
        partitions.get(0).getStorageLocation());
    assertEquals("2025-01-16", partitions.get(1).getValues().get(0));
  }

  @Test
  void testGetTableLocation() throws Exception {
    when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true, true, true, true, false);
    when(mockResultSet.getString(1))
        .thenReturn("id", "# Detailed Table Information", "Database:", "Location:");
    // getString(2) is only called when column 1 is "Location:"
    when(mockResultSet.getString(2))
        .thenReturn("s3a://bucket/warehouse/table");

    String location = operator.getTableLocation("my_table");
    assertEquals("s3a://bucket/warehouse/table", location);
  }

  @Test
  void testDropTable() throws Exception {
    when(mockStatement.execute(anyString())).thenReturn(true);
    operator.dropTable("my_table");
    verify(mockStatement).execute("DROP TABLE IF EXISTS `test_db`.`my_table`");
  }

  @Test
  void testRenameTable() throws Exception {
    when(mockStatement.execute(anyString())).thenReturn(true);
    operator.renameTable("old_name", "new_name");
    verify(mockStatement).execute(
        "ALTER TABLE `test_db`.`old_name` RENAME TO `test_db`.`new_name`");
  }
}
