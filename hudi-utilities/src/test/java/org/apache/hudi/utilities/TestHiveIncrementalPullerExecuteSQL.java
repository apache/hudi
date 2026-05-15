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

package org.apache.hudi.utilities;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link HiveIncrementalPuller#executeIncrementalSQL}.
 *
 * These tests mock the JDBC {@link Statement} to avoid requiring a live Hive server,
 * focusing on SQL template rendering and execution behaviour.
 */
class TestHiveIncrementalPullerExecuteSQL {

  @TempDir
  Path tempDir;

  private HiveIncrementalPuller.Config config;

  @BeforeEach
  void setUp() {
    config = new HiveIncrementalPuller.Config();
    config.sourceDb = "testdb";
    config.sourceTable = "test1";
    config.targetDb = "tgtdb";
    config.targetTable = "test2";
    config.tmpDb = "tmp_db";
    config.fromCommitTime = "100";
    config.hoodieTmpDir = tempDir.toAbsolutePath().toString();
    // hiveJDBCUrl is not used since we mock the Statement directly
  }

  private void writeSqlFile(String sql) throws IOException {
    Path sqlFile = tempDir.resolve("incremental_pull.sql");
    Files.createFile(sqlFile);
    try (FileWriter fw = new FileWriter(new File(sqlFile.toUri()))) {
      fw.write(sql);
    }
    config.incrementalSQLFile = sqlFile.toString();
  }

  @Test
  void testExecuteIncrementalSQLRendersAndExecutesCorrectSQL() throws IOException, SQLException {
    writeSqlFile("select name from testdb.test1 where `_hoodie_commit_time` > '%s'");
    HiveIncrementalPuller puller = new HiveIncrementalPuller(config);

    Statement mockStmt = mock(Statement.class);
    String tempDbTable = "tmp_db.test2__test1";
    String tempDbTablePath = "/tmp/hoodie/test2__test1/101";

    puller.executeIncrementalSQL(tempDbTable, tempDbTablePath, mockStmt);

    ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockStmt).execute(sqlCaptor.capture());
    String renderedSql = sqlCaptor.getValue();
    assertTrue(renderedSql.contains("CREATE TABLE " + tempDbTable),
        "SQL should create the temp table");
    assertTrue(renderedSql.contains("STORED AS AVRO"),
        "SQL should include STORED AS AVRO");
    assertTrue(renderedSql.contains("LOCATION '" + tempDbTablePath + "'"),
        "SQL should include the target location");
    assertTrue(renderedSql.contains(config.fromCommitTime),
        "SQL should substitute fromCommitTime in place of '%s'");
    assertTrue(!renderedSql.contains("%s"),
        "SQL should not contain the unsubstituted '%s' placeholder");
  }

  @Test
  void testExecuteIncrementalSQLFileNotFound() throws IOException {
    writeSqlFile("select name from testdb.test1 where `_hoodie_commit_time` > '%s'");
    HiveIncrementalPuller puller = new HiveIncrementalPuller(config);
    config.incrementalSQLFile = "/nonexistent/path/to/file.sql";

    Statement mockStmt = mock(Statement.class);
    assertThrows(FileNotFoundException.class, () ->
        puller.executeIncrementalSQL("tmp_db.test2__test1", "/tmp/path", mockStmt));
  }

  @Test
  void testExecuteIncrementalSQLStatementExecutionFailure() throws IOException, SQLException {
    writeSqlFile("select name from testdb.test1 where `_hoodie_commit_time` > '%s'");
    HiveIncrementalPuller puller = new HiveIncrementalPuller(config);

    Statement mockStmt = mock(Statement.class);
    doThrow(new SQLException("Hive execution failed")).when(mockStmt).execute(anyString());

    assertThrows(SQLException.class, () ->
        puller.executeIncrementalSQL("tmp_db.test2__test1", "/tmp/path", mockStmt));
  }
}
