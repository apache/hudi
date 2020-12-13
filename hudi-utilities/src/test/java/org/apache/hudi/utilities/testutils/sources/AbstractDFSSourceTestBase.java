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

package org.apache.hudi.utilities.testutils.sources;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An abstract test base for {@link Source} using DFS as the file system.
 */
public abstract class AbstractDFSSourceTestBase extends UtilitiesTestBase {

  protected FilebasedSchemaProvider schemaProvider;
  protected String dfsRoot;
  protected String fileSuffix;
  protected HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
  protected boolean useFlattenedSchema = false;

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
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  /**
   * Prepares the specific {@link Source} to test, by passing in necessary configurations.
   *
   * @return A {@link Source} using DFS as the file system.
   */
  protected abstract Source prepareDFSSource();

  /**
   * Writes test data, i.e., a {@link List} of {@link HoodieRecord}, to a file on DFS.
   *
   * @param records Test data.
   * @param path The path in {@link Path} of the file to write.
   */
  protected abstract void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException;

  /**
   * Generates a batch of test data and writes the data to a file.
   *
   * @param filename The name of the file.
   * @param instantTime The commit time.
   * @param n The number of records to generate.
   * @return The file path.
   */
  protected Path generateOneFile(String filename, String instantTime, int n) throws IOException {
    Path path = new Path(dfsRoot, filename + fileSuffix);
    writeNewDataToFile(dataGenerator.generateInserts(instantTime, n, useFlattenedSchema), path);
    return path;
  }

  /**
   * Runs the test scenario of reading data from the source.
   *
   * @throws IOException
   */
  @Test
  public void testReadingFromSource() throws IOException {
    dfs.mkdirs(new Path(dfsRoot));
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(prepareDFSSource());

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(),
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    // Test respecting sourceLimit
    int sourceLimit = 10;
    RemoteIterator<LocatedFileStatus> files = dfs.listFiles(generateOneFile("1", "000", 100), true);
    FileStatus file1Status = files.next();
    assertTrue(file1Status.getLen() > sourceLimit);
    assertEquals(Option.empty(),
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), sourceLimit).getBatch());
    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch1 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(100, fetch1.getBatch().get().count());
    // Test fetching Row format
    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(100, fetch1AsRows.getBatch().get().count());
    // Test Avro to Row format
    Dataset<Row> fetch1Rows = AvroConversionUtils
        .createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
            schemaProvider.getSourceSchema().toString(), sparkSession);
    assertEquals(100, fetch1Rows.count());

    // 2. Produce new data, extract new data
    generateOneFile("2", "001", 10000);
    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch2 = sourceFormatAdapter.fetchNewDataInAvroFormat(
        Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(10000, fetch2.getBatch().get().count());
    // Test fetching Row format
    InputBatch<Dataset<Row>> fetch2AsRows = sourceFormatAdapter.fetchNewDataInRowFormat(
        Option.of(fetch1AsRows.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(10000, fetch2AsRows.getBatch().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    InputBatch<Dataset<Row>> fetch3AsRows = sourceFormatAdapter.fetchNewDataInRowFormat(
        Option.of(fetch1AsRows.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(10000, fetch3AsRows.getBatch().get().count());
    assertEquals(fetch2AsRows.getCheckpointForNextBatch(),
        fetch3AsRows.getCheckpointForNextBatch());
    fetch3AsRows.getBatch().get().createOrReplaceTempView("test_dfs_table");
    Dataset<Row> rowDataset = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate()
        .sql("select * from test_dfs_table");
    assertEquals(10000, rowDataset.count());

    // 4. Extract with latest checkpoint => no new data returned
    InputBatch<JavaRDD<GenericRecord>> fetch4 = sourceFormatAdapter.fetchNewDataInAvroFormat(
        Option.of(fetch2.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch4.getBatch());

    // 5. Extract from the beginning
    InputBatch<JavaRDD<GenericRecord>> fetch5 = sourceFormatAdapter.fetchNewDataInAvroFormat(
        Option.empty(), Long.MAX_VALUE);
    assertEquals(10100, fetch5.getBatch().get().count());

    // 6. Should skip files/directories whose names start with prefixes ("_", ".")
    generateOneFile(".checkpoint/3", "002", 100);
    generateOneFile("_checkpoint/3", "002", 100);
    generateOneFile(".3", "002", 100);
    generateOneFile("_3", "002", 100);
    // also work with nested directory
    generateOneFile("foo/.bar/3", "002", 1); // not ok
    generateOneFile("foo/bar/3", "002", 1); // ok
    // fetch everything from the beginning
    InputBatch<JavaRDD<GenericRecord>> fetch6 = sourceFormatAdapter.fetchNewDataInAvroFormat(
        Option.empty(), Long.MAX_VALUE);
    assertEquals(10101, fetch6.getBatch().get().count());
  }
}
