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
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.UtilitiesTestBase;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * An abstract test base for {@link Source} using DFS as the file system.
 */
public abstract class AbstractDFSSourceTestBase extends UtilitiesTestBase {

  FilebasedSchemaProvider schemaProvider;
  String dfsRoot;
  String fileSuffix;
  HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

  @BeforeClass
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    UtilitiesTestBase.cleanupClass();
  }

  @Before
  public void setup() throws Exception {
    super.setup();
    schemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
  }

  @After
  public void teardown() throws Exception {
    super.teardown();
  }

  /**
   * Prepares the specific {@link Source} to test, by passing in necessary configurations.
   *
   * @return A {@link Source} using DFS as the file system.
   */
  abstract Source prepareDFSSource();

  /**
   * Writes test data, i.e., a {@link List} of {@link HoodieRecord}, to a file on DFS.
   *
   * @param records Test data.
   * @param path    The path in {@link Path} of the file to write.
   * @throws IOException
   */
  abstract void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException;

  /**
   * Generates a batch of test data and writes the data to a file.
   *
   * @param filename  The name of the file.
   * @param commitTime  The commit time.
   * @param n  The number of records to generate.
   * @return  The file path.
   * @throws IOException
   */
  Path generateOneFile(String filename, String commitTime, int n) throws IOException {
    Path path = new Path(dfsRoot, filename + fileSuffix);
    writeNewDataToFile(dataGenerator.generateInserts(commitTime, n), path);
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
  }
}
