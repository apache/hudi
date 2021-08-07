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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.CloudObjectTestUtils;
import org.apache.hudi.utilities.testutils.sources.AbstractCloudObjectsSourceTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.QUEUE_REGION;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.QUEUE_URL_PROP;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.SOURCE_QUEUE_FS_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic tests for {@link CloudObjectsDfsSource}.
 */
public class TestCloudObjectsDfsSource extends AbstractCloudObjectsSourceTestBase {

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    this.dfsRoot = dfsBasePath + "/parquetFiles";
    this.fileSuffix = ".parquet";
    dfs.mkdirs(new Path(dfsRoot));
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
  }

  /**
   * Runs the test scenario of reading data from the source.
   *
   * @throws IOException
   */
  @Test
  public void testReadingFromSource() throws IOException {

    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(prepareCloudObjectSource());

    // 1. Extract without any checkpoint => (no data available)
    generateMessageInQueue(null);
    assertEquals(
        Option.empty(),
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());

    // 2. Extract without any checkpoint =>  (adding new file)
    generateMessageInQueue("1");
    generateOneFile("1", "000", 100);

    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch1 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(100, fetch1.getBatch().get().count());
    // Test fetching Row format
    InputBatch<Dataset<Row>> fetch1AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(100, fetch1AsRows.getBatch().get().count());
    // Test Avro to Row format
    Dataset<Row> fetch1Rows =
        AvroConversionUtils.createDataFrame(
            JavaRDD.toRDD(fetch1.getBatch().get()),
            schemaProvider.getSourceSchema().toString(),
            sparkSession);
    assertEquals(100, fetch1Rows.count());

    // 3. Produce new data, extract new data
    generateMessageInQueue("2");
    generateOneFile("2", "001", 10000);
    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch2 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(
            Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(10000, fetch2.getBatch().get().count());
    // Test fetching Row format
    InputBatch<Dataset<Row>> fetch2AsRows =
        sourceFormatAdapter.fetchNewDataInRowFormat(
            Option.of(fetch1AsRows.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(10000, fetch2AsRows.getBatch().get().count());

    // 4. Should skip files/directories whose names start with prefixes ("_", ".")
    generateOneFile("checkpoint/.3", "002", 100); // not ok
    generateMessageInQueue("checkpoint/.3");
    CloudObjectTestUtils.deleteMessagesInQueue(sqs);
    InputBatch<JavaRDD<GenericRecord>> fetch3 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch3.getBatch());

    generateOneFile("_checkpoint/3", "002", 100); // not ok
    generateMessageInQueue("_checkpoint/3");
    CloudObjectTestUtils.deleteMessagesInQueue(sqs);
    InputBatch<JavaRDD<GenericRecord>> fetch4 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch4.getBatch());

    generateOneFile("foo/.bar/3", "002", 1); // not ok
    generateMessageInQueue("foo/.bar/3");
    CloudObjectTestUtils.deleteMessagesInQueue(sqs);
    InputBatch<JavaRDD<GenericRecord>> fetch5 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch5.getBatch());
  }

  @Override
  public Source prepareCloudObjectSource() {
    TypedProperties props = new TypedProperties();
    props.setProperty(QUEUE_URL_PROP, this.sqsUrl);
    props.setProperty(QUEUE_REGION, regionName);
    props.setProperty(SOURCE_QUEUE_FS_PROP, "hdfs");
    CloudObjectsDfsSource dfsSource = new CloudObjectsDfsSource(props, jsc, sparkSession, null);
    dfsSource.sqs = this.sqs;
    return dfsSource;
  }

  @Override
  public void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException {
    Helpers.saveParquetToDFS(Helpers.toGenericRecords(records), path);
  }
}
