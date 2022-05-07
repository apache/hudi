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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.sources.AbstractCloudObjectsSourceTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.S3_SOURCE_QUEUE_REGION;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.S3_SOURCE_QUEUE_URL;
import static org.apache.hudi.utilities.sources.helpers.CloudObjectsSelector.Config.S3_SOURCE_QUEUE_FS;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic tests for {@link S3EventsSource}.
 */
public class TestS3EventsSource extends AbstractCloudObjectsSourceTestBase {

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

    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch1 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1, fetch1.getBatch().get().count());

    // 3. Produce new data, extract new data
    generateMessageInQueue("2");
    // Test fetching Avro format
    InputBatch<JavaRDD<GenericRecord>> fetch2 =
        sourceFormatAdapter.fetchNewDataInAvroFormat(
            Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(1, fetch2.getBatch().get().count());

    GenericRecord s3 = (GenericRecord) fetch2.getBatch().get().rdd().first().get("s3");
    GenericRecord s3Object = (GenericRecord) s3.get("object");
    assertEquals("2.parquet", s3Object.get("key").toString());
  }

  @Override
  public Source prepareCloudObjectSource() {
    TypedProperties props = new TypedProperties();
    props.setProperty(S3_SOURCE_QUEUE_URL, sqsUrl);
    props.setProperty(S3_SOURCE_QUEUE_REGION, regionName);
    props.setProperty(S3_SOURCE_QUEUE_FS, "hdfs");
    S3EventsSource dfsSource = new S3EventsSource(props, jsc, sparkSession, null);
    dfsSource.sqs = this.sqs;
    return dfsSource;
  }

  @Override
  public void writeNewDataToFile(List<HoodieRecord> records, Path path) throws IOException {
    Helpers.saveParquetToDFS(Helpers.toGenericRecords(records), path);
  }
}
