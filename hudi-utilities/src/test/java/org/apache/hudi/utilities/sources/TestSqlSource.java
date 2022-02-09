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

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test against {@link SqlSource}.
 */
public class TestSqlSource extends UtilitiesTestBase {

  private final boolean useFlattenedSchema = false;
  private final String sqlSourceConfig = "hoodie.deltastreamer.source.sql.sql.query";
  protected FilebasedSchemaProvider schemaProvider;
  protected HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
  private String dfsRoot;
  private TypedProperties props;
  private SqlSource sqlSource;
  private SourceFormatAdapter sourceFormatAdapter;

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    dfsRoot = UtilitiesTestBase.dfsBasePath + "/parquetFiles";
    UtilitiesTestBase.dfs.mkdirs(new Path(dfsRoot));
    props = new TypedProperties();
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    // Produce new data, extract new data
    generateTestTable("1", "001", 10000);
  }

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
  public void testSqlSourceAvroFormat() throws IOException {
    props.setProperty(sqlSourceConfig, "select * from test_sql_table");
    sqlSource = new SqlSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlSource);

    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch1 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);

    // Test Avro to Row format
    Dataset<Row> fetch1Rows = AvroConversionUtils
        .createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
            schemaProvider.getSourceSchema().toString(), sparkSession);
    assertEquals(10000, fetch1Rows.count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source has less records than source limit.
   *
   * @throws IOException
   */
  @Test
  public void testSqlSourceRowFormat() throws IOException {
    props.setProperty(sqlSourceConfig, "select * from test_sql_table");
    sqlSource = new SqlSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlSource);

    // Test fetching Row format
    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(10000, fetch1AsRows.getBatch().get().count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source has no records.
   *
   * @throws IOException
   */
  @Test
  public void testSqlSourceCheckpoint() throws IOException {
    props.setProperty(sqlSourceConfig, "select * from test_sql_table where 1=0");
    sqlSource = new SqlSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlSource);

    InputBatch<Dataset<Row>> fetch1AsRows =
            sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    assertNull(fetch1AsRows.getCheckpointForNextBatch());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source has more records than source limit.
   *
   * @throws IOException
   */
  @Test
  public void testSqlSourceMoreRecordsThanSourceLimit() throws IOException {
    props.setProperty(sqlSourceConfig, "select * from test_sql_table");
    sqlSource = new SqlSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlSource);

    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), 1000);
    assertEquals(10000, fetch1AsRows.getBatch().get().count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source has no records.
   *
   * @throws IOException
   */
  @Test
  public void testSqlSourceZeroRecord() throws IOException {
    props.setProperty(sqlSourceConfig, "select * from test_sql_table where 1=0");
    sqlSource = new SqlSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlSource);

    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(0, fetch1AsRows.getBatch().get().count());
  }

  /**
   * Runs the test scenario of reading data from the source in row format.
   * Source table doesn't exists.
   *
   * @throws IOException
   */
  @Test
  public void testSqlSourceInvalidTable() throws IOException {
    props.setProperty(sqlSourceConfig, "select * from not_exist_sql_table");
    sqlSource = new SqlSource(props, jsc, sparkSession, schemaProvider);
    sourceFormatAdapter = new SourceFormatAdapter(sqlSource);

    assertThrows(
        AnalysisException.class,
        () -> sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE));
  }
}
