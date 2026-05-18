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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test against {@link SqlSource}.
 */
public class TestSqlFileBasedSource extends UtilitiesTestBase {

  private final boolean useFlattenedSchema = false;
  private final String sqlFileSourceConfig = "hoodie.streamer.source.sql.file";
  private final String sqlFileSourceConfigEmitChkPointConf = "hoodie.streamer.source.sql.checkpoint.emit";
  protected FilebasedSchemaProvider schemaProvider;
  protected HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
  private String dfsRoot;
  private TypedProperties props;
  private SqlFileBasedSource sqlFileSource;
  private SourceFormatAdapter sourceFormatAdapter;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices(false, false, false);
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @Override
  @BeforeEach
  public void setup() throws Exception {
    dfsRoot = UtilitiesTestBase.basePath + "/parquetFiles";
    UtilitiesTestBase.fs.mkdirs(new Path(dfsRoot));
    props = new TypedProperties();
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    // Produce new data, extract new data
    generateTestTable("1", "001", 10000);
  }

  @Override
  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  /**
   * Generates a batch of test data and writes the data to a file and register a test table.
   *
   * @param filename    The name of the file.
   * @param instantTime The commit time.
   * @param n           The number of records to generate.
   */
  private void generateTestTable(String filename, String instantTime, int n) throws IOException {
    Path path = new Path(dfsRoot, filename);
    Helpers.saveParquetToDFS(Helpers.toGenericRecords(dataGenerator.generateInserts(instantTime, n, useFlattenedSchema)), path);
    sparkSession.read().parquet(dfsRoot).createOrReplaceTempView("test_sql_table");
  }

  /**
   * Runs the test scenario of reading data from the source in avro format.
   *
   * @throws IOException
   */
  @Test
  public void testSqlFileBasedSourceAvroFormat() throws IOException {
    UtilitiesTestBase.Helpers.copyToDFS(
        "streamer-config/sql-file-based-source.sql", storage,
        UtilitiesTestBase.basePath + "/sql-file-based-source.sql");

    props.setProperty(sqlFileSourceConfig, UtilitiesTestBase.basePath + "/sql-file-based-source.sql");
    sqlFileSource = new SqlFileBasedSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlFileSource);

    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch1 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);

    // Test Avro to Row format
    Dataset<Row> fetch1Rows = AvroConversionUtils
        .createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
            schemaProvider.getSourceHoodieSchema().toString(), sparkSession);
    assertEquals(10000, fetch1Rows.count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source has less records than source limit.
   *
   * @throws IOException
   */
  @Test
  public void testSqlFileBasedSourceRowFormat() throws IOException {
    UtilitiesTestBase.Helpers.copyToDFS(
        "streamer-config/sql-file-based-source.sql", storage,
        UtilitiesTestBase.basePath + "/sql-file-based-source.sql");

    props.setProperty(sqlFileSourceConfig, UtilitiesTestBase.basePath + "/sql-file-based-source.sql");
    sqlFileSource = new SqlFileBasedSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlFileSource);

    // Test fetching Row format
    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(10000, fetch1AsRows.getBatch().get().count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source has more records than source limit.
   *
   * @throws IOException
   */
  @Test
  public void testSqlFileBasedSourceMoreRecordsThanSourceLimit() throws IOException {
    UtilitiesTestBase.Helpers.copyToDFS(
        "streamer-config/sql-file-based-source.sql", storage,
        UtilitiesTestBase.basePath + "/sql-file-based-source.sql");

    props.setProperty(sqlFileSourceConfig, UtilitiesTestBase.basePath + "/sql-file-based-source.sql");
    sqlFileSource = new SqlFileBasedSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlFileSource);

    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), 1000);
    assertEquals(10000, fetch1AsRows.getBatch().get().count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source table doesn't exists.
   *
   * @throws IOException
   */
  @Test
  public void testSqlFileBasedSourceInvalidTable() throws IOException {
    UtilitiesTestBase.Helpers.copyToDFS(
        "streamer-config/sql-file-based-source-invalid-table.sql", storage,
        UtilitiesTestBase.basePath + "/sql-file-based-source-invalid-table.sql");

    props.setProperty(sqlFileSourceConfig, UtilitiesTestBase.basePath + "/sql-file-based-source-invalid-table.sql");
    sqlFileSource = new SqlFileBasedSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlFileSource);

    assertThrows(
        AnalysisException.class,
        () -> sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE));
  }

  @Test
  public void shouldSetCheckpointForSqlFileBasedSourceWithEpochCheckpoint() throws IOException {
    UtilitiesTestBase.Helpers.copyToDFS(
        "streamer-config/sql-file-based-source.sql", storage,
        UtilitiesTestBase.basePath + "/sql-file-based-source.sql");

    props.setProperty(sqlFileSourceConfig, UtilitiesTestBase.basePath + "/sql-file-based-source.sql");
    props.setProperty(sqlFileSourceConfigEmitChkPointConf, "true");

    sqlFileSource = new SqlFileBasedSource(props, jsc, sparkSession, schemaProvider);
    Pair<Option<Dataset<Row>>, Checkpoint> nextBatch = sqlFileSource.fetchNextBatch(Option.empty(), Long.MAX_VALUE);

    assertEquals(10000, nextBatch.getLeft().get().count());
    long currentTimeInMillis = System.currentTimeMillis();
    long checkpointToBeUsed = Long.parseLong(nextBatch.getRight().getCheckpointKey());
    assertTrue((currentTimeInMillis - checkpointToBeUsed) / 1000 < 60);
    assertTrue(currentTimeInMillis > checkpointToBeUsed);
  }
}