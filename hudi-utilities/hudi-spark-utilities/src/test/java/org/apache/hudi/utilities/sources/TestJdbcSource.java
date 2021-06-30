/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.testutils.JdbcTestUtils.clearAndInsert;
import static org.apache.hudi.utilities.testutils.JdbcTestUtils.close;
import static org.apache.hudi.utilities.testutils.JdbcTestUtils.count;
import static org.apache.hudi.utilities.testutils.JdbcTestUtils.insert;
import static org.apache.hudi.utilities.testutils.JdbcTestUtils.update;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests {@link JdbcSource}.
 */
public class TestJdbcSource extends UtilitiesTestBase {

  private static final TypedProperties PROPS = new TypedProperties();
  private static final HoodieTestDataGenerator DATA_GENERATOR = new HoodieTestDataGenerator();
  private static Connection connection;

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    PROPS.setProperty("hoodie.deltastreamer.jdbc.url", "jdbc:h2:mem:test_mem");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.driver.class", "org.h2.Driver");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.user", "test");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.password", "jdbc");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.name", "triprec");
    connection = DriverManager.getConnection("jdbc:h2:mem:test_mem", "test", "jdbc");
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    close(connection);
  }

  @Test
  public void testSingleCommit() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      int numRecords = 100;
      String commitTime = "000";

      // Insert 100 records with commit time
      clearAndInsert(commitTime, numRecords, connection, DATA_GENERATOR, PROPS);

      // Validate if we have specified records in db
      assertEquals(numRecords, count(connection, "triprec"));

      // Start JdbcSource
      Dataset<Row> rowDataset = runSource(Option.empty(), numRecords).getBatch().get();
      assertEquals(numRecords, rowDataset.count());
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testInsertAndUpdate() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      final String commitTime = "000";
      final int numRecords = 100;

      // Add 100 records. Update half of them with commit time "007".
      update("007",
          clearAndInsert(commitTime, numRecords, connection, DATA_GENERATOR, PROPS)
              .stream()
              .limit(50)
              .collect(Collectors.toList()),
          connection, DATA_GENERATOR, PROPS
      );
      // Check if database has 100 records
      assertEquals(numRecords, count(connection, "triprec"));

      // Start JdbcSource
      Dataset<Row> rowDataset = runSource(Option.empty(), 100).getBatch().get();
      assertEquals(100, rowDataset.count());

      Dataset<Row> firstCommit = rowDataset.where("commit_time=000");
      assertEquals(50, firstCommit.count());

      Dataset<Row> secondCommit = rowDataset.where("commit_time=007");
      assertEquals(50, secondCommit.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testTwoCommits() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      // Add 10 records with commit time "000"
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      Dataset<Row> rowDataset = runSource(Option.empty(), 10).getBatch().get();
      assertEquals(10, rowDataset.where("commit_time=000").count());

      // Add 10 records with commit time 001
      insert("001", 5, connection, DATA_GENERATOR, PROPS);
      rowDataset = runSource(Option.empty(), 15).getBatch().get();
      assertEquals(15, rowDataset.count());
      assertEquals(5, rowDataset.where("commit_time=001").count());
      assertEquals(10, rowDataset.where("commit_time=000").count());

      // Start second commit and check if all records are pulled
      rowDataset = runSource(Option.empty(), 15).getBatch().get();
      assertEquals(15, rowDataset.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIncrementalFetchWithCommitTime() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      // Add 10 records with commit time "000"
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      InputBatch<Dataset<Row>> batch = runSource(Option.empty(), 10);
      Dataset<Row> rowDataset = batch.getBatch().get();
      assertEquals(10, rowDataset.count());

      // Add 10 records with commit time "001"
      insert("001", 10, connection, DATA_GENERATOR, PROPS);

      // Start incremental scan
      rowDataset = runSource(Option.of(batch.getCheckpointForNextBatch()), 10).getBatch().get();
      assertEquals(10, rowDataset.count());
      assertEquals(10, rowDataset.where("commit_time=001").count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIncrementalFetchWithNoMatchingRows() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      // Add 10 records with commit time "000"
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      InputBatch<Dataset<Row>> batch = runSource(Option.empty(), 10);
      Dataset<Row> rowDataset = batch.getBatch().get();
      assertEquals(10, rowDataset.count());

      // Start incremental scan
      rowDataset = runSource(Option.of(batch.getCheckpointForNextBatch()), 10).getBatch().get();
      assertEquals(0, rowDataset.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIncrementalFetchWhenTableRecordsMoreThanSourceLimit() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "id");

    try {
      // Add 100 records with commit time "000"
      clearAndInsert("000", 100, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      InputBatch<Dataset<Row>> batch = runSource(Option.empty(), 100);
      Dataset<Row> rowDataset = batch.getBatch().get();
      assertEquals(100, rowDataset.count());

      // Add 100 records with commit time "001"
      insert("001", 100, connection, DATA_GENERATOR, PROPS);

      // Start incremental scan. Now there are 100 more records but with sourceLimit set to 60, only fetch 60 records should be fetched.
      // Those 50 records should be of the commit_time=001 because records with commit_time=000 have already been processed.
      batch = runSource(Option.of(batch.getCheckpointForNextBatch()), 60);
      rowDataset = batch.getBatch().get();
      assertEquals(60, rowDataset.count());
      assertEquals(60, rowDataset.where("commit_time=001").count());
      // No more records added, but sourceLimit is now set to 75. Still, only the remaining 40 records should be fetched.
      rowDataset = runSource(Option.of(batch.getCheckpointForNextBatch()), 75).getBatch().get();
      assertEquals(40, rowDataset.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIncrementalFetchWhenLastCheckpointMoreThanTableRecords() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "id");

    try {
      // Add 100 records with commit time "000"
      clearAndInsert("000", 100, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      InputBatch<Dataset<Row>> batch = runSource(Option.empty(), 100);
      Dataset<Row> rowDataset = batch.getBatch().get();
      assertEquals(100, rowDataset.count());
      assertEquals("100", batch.getCheckpointForNextBatch());

      // Add 100 records with commit time "001"
      insert("001", 100, connection, DATA_GENERATOR, PROPS);

      // Start incremental scan. With checkpoint greater than the number of records, there should not be any dataset to fetch.
      batch = runSource(Option.of("200"), 50);
      rowDataset = batch.getBatch().get();
      assertEquals(0, rowDataset.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testIncrementalFetchFallbackToFullFetchWhenError() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "true");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      // Add 10 records with commit time "000"
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      InputBatch<Dataset<Row>> batch = runSource(Option.empty(), 10);
      Dataset<Row> rowDataset = batch.getBatch().get();
      assertEquals(10, rowDataset.count());

      // Add 10 records with commit time "001"
      insert("001", 10, connection, DATA_GENERATOR, PROPS);

      PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "dummy_col");
      assertThrows(HoodieException.class, () -> {
        // Start incremental scan with a dummy column that does not exist.
        // This will throw an exception as the default behavior is to not fallback to full fetch.
        runSource(Option.of(batch.getCheckpointForNextBatch()), -1);
      });

      PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.fallback.to.full.fetch", "true");

      // Start incremental scan with a dummy column that does not exist.
      // This will fallback to full fetch mode but still throw an exception checkpointing will fail.
      Exception exception = assertThrows(HoodieException.class, () -> {
        runSource(Option.of(batch.getCheckpointForNextBatch()), -1);
      });
      assertTrue(exception.getMessage().contains("Failed to checkpoint"));
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFullFetchWithCommitTime() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "false");

    try {
      // Add 10 records with commit time "000"
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      Dataset<Row> rowDataset = runSource(Option.empty(), 10).getBatch().get();
      assertEquals(10, rowDataset.count());
      // Add 10 records with commit time "001"
      insert("001", 10, connection, DATA_GENERATOR, PROPS);

      // Start full fetch
      rowDataset = runSource(Option.empty(), 20).getBatch().get();
      assertEquals(20, rowDataset.count());
      assertEquals(10, rowDataset.where("commit_time=000").count());
      assertEquals(10, rowDataset.where("commit_time=001").count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testFullFetchWithCheckpoint() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "false");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.table.incr.column.name", "last_insert");

    try {
      // Add 10 records with commit time "000"
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);

      // Start JdbcSource
      InputBatch<Dataset<Row>> batch = runSource(Option.empty(), 10);
      Dataset<Row> rowDataset = batch.getBatch().get();
      assertEquals(10, rowDataset.count());
      assertEquals("", batch.getCheckpointForNextBatch());

      // Get max of incremental column
      Column incrementalColumn = rowDataset
          .col(PROPS.getString("hoodie.deltastreamer.jdbc.table.incr.column.name"));
      final String max = rowDataset.agg(functions.max(incrementalColumn).cast(DataTypes.StringType)).first()
          .getString(0);

      // Add 10 records with commit time "001"
      insert("001", 10, connection, DATA_GENERATOR, PROPS);

      // Start incremental scan
      rowDataset = runSource(Option.of(max), 10).getBatch().get();
      assertEquals(10, rowDataset.count());
      assertEquals(10, rowDataset.where("commit_time=001").count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSourceWithPasswordOnFs() {
    try {
      // Write secret string to fs in a file
      writeSecretToFs();
      // Remove secret string from props
      PROPS.remove("hoodie.deltastreamer.jdbc.password");
      // Set property to read secret from fs file
      PROPS.setProperty("hoodie.deltastreamer.jdbc.password.file", "file:///tmp/hudi/config/secret");
      PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "false");
      // Add 10 records with commit time 000
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);
      Dataset<Row> rowDataset = runSource(Option.empty(), 10).getBatch().get();
      assertEquals(10, rowDataset.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSourceWithNoPasswordThrowsException() {
    assertThrows(HoodieException.class, () -> {
      // Write secret string to fs in a file
      writeSecretToFs();
      // Remove secret string from props
      PROPS.remove("hoodie.deltastreamer.jdbc.password");
      PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "false");
      // Add 10 records with commit time 000
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);
      runSource(Option.empty(), 10);
    });
  }

  @Test
  public void testSourceWithExtraOptions() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.extra.options.fetchsize", "10");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "false");
    PROPS.remove("hoodie.deltastreamer.jdbc.table.incr.column.name");
    try {
      // Add 20 records with commit time 000
      clearAndInsert("000", 20, connection, DATA_GENERATOR, PROPS);
      Dataset<Row> rowDataset = runSource(Option.empty(), 10).getBatch().get();
      assertEquals(10, rowDataset.count());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testSourceWithStorageLevel() {
    PROPS.setProperty("hoodie.deltastreamer.jdbc.storage.level", "NONE");
    PROPS.setProperty("hoodie.deltastreamer.jdbc.incr.pull", "false");
    try {
      // Add 10 records with commit time 000
      clearAndInsert("000", 10, connection, DATA_GENERATOR, PROPS);
      Dataset<Row> rowDataset = runSource(Option.empty(), 10).getBatch().get();
      assertEquals(10, rowDataset.count());
      assertEquals(StorageLevel.NONE(), rowDataset.storageLevel());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private void writeSecretToFs() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataOutputStream outputStream = fs.create(new Path("file:///tmp/hudi/config/secret"));
    outputStream.writeBytes("jdbc");
    outputStream.close();
  }

  private InputBatch<Dataset<Row>> runSource(Option<String> lastCkptStr, long sourceLimit) {
    Source<Dataset<Row>> jdbcSource = new JdbcSource(PROPS, jsc, sparkSession, null);
    return jdbcSource.fetchNewData(lastCkptStr, sourceLimit);
  }
}
