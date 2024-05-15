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

package org.apache.hudi.utilities.applied.common;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.applied.proto.TestPersonProto;
import org.apache.hudi.utilities.applied.sources.TestUrsaBatchS3EventsSource;
import org.apache.hudi.utilities.config.S3SourceConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class TestCommon {

  public static final String TEST_UUID = "018f825b-2767-7dd6-b458-0a307202074f";
  public static final String TEST_BUCKET = "testbucket";
  public static final String TEST_BUCKET_WITH_SCHEME = "s3://testbucket/";
  public static final String TEST_BATCH_BUCKET_KEY = "/ursa/batches/framegen/018f825b-2767-7dd6-b458-0a307202074f.manifest";
  public static final String TEST_BUCKET_REGION = "us-west-1";
  public static final String TEST_BATCH_BUCKET_NAME = "testing-bucket";
  public static final String TEST_SOURCE_TYPE = "test_source";
  public static final String TEST_SCHEMA_SOURCE_TYPE = "test_type";
  public static final String TEST_SCHEME_URL = "test_url";
  public static final String TEST_SCHEMA_SOURCE = "test_schema_soure";
  public static final Long EXPECTED_BATCH_CREATED_TIMESTAMP_SEC = new Long(100);
  public static final Long EXPECTED_ORIGIN_CREATED_TIMESTAMP_SEC = new Long(2);
  public static final Long EXPECTED_ORIGIN_UPDATED_TIMESTAMP_SEC = new Long(0);
  public static final long[] TEST_TIMESTAMPS_N = new long[] {
      1709533521400099840L,
      1709533521500099840L,
      1709533521600099840L,
      1709533521700099840L,
      1709533521800099840L
  };
  public static final TestPersonProto.Person[] PERSONS = new TestPersonProto.Person[5];

  static {
    for (int i = 0; i < 5; i++) {
      PERSONS[i] = TestPersonProto.Person.newBuilder().setName("test_name_" + i).setEmail("test_email_" + i + "@applied.co").setId(i).build();
    }
  }

  public static String getPersonProtoFilePath() {
    return Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("schema/person.proto")).getPath();
  }

  public static String getPersonFileDescriptorPath() {
    return Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("schema/person.pb")).getPath();
  }

  public static String getPersonParquetDirPath() {
    return Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("data/person/raw/")).getPath();
  }

  public static String[] getAbsolutePaths(String parent) {
    return new String[] {
        parent + "raw/file1.parquet",
        parent + "raw/file2.parquet",
        parent + "raw/file3.parquet",
        parent + "raw/file4.parquet",
        parent + "raw/file5.parquet"
    };
  }

  /**
   * Generate Batch info for unit-tests
   * @return UrsaBatchInfo
   */
  public static UrsaBatchInfo getTestBatchInfo() {
    UrsaBatchInfo batchInfo = new UrsaBatchInfo();
    batchInfo.setSourceType(TEST_SOURCE_TYPE);
    batchInfo.setUuid(TEST_UUID);
    batchInfo.setCreatedTimestampSec(100);
    batchInfo.setOriginCreatedTimestampSec(1);
    batchInfo.setOriginCreatedTimestampSec(2);
    batchInfo.setFilesFormat("parquet");
    batchInfo.setSchemaInfo(new UrsaBatchSchemaInfo());
    batchInfo.getSchemaInfo().setSource(TEST_SCHEMA_SOURCE);
    batchInfo.getSchemaInfo().setType(TEST_SCHEMA_SOURCE_TYPE);
    batchInfo.getSchemaInfo().setUrl(TEST_SCHEME_URL);
    String[] expFiles = getAbsolutePaths(TEST_BUCKET_WITH_SCHEME);
    batchInfo.setFiles(Arrays.asList(expFiles));
    return batchInfo;
  }

  public static Pair<Pair<TypedProperties, UrsaBatchInfo>, Dataset<Row>> generateUrsaBatchS3Events(SparkSession sparkSession) {
    TypedProperties props = new TypedProperties();
    props.setProperty(S3SourceConfig.S3_SOURCE_QUEUE_URL.key(), "dummy");
    props.setProperty(S3SourceConfig.S3_SOURCE_QUEUE_REGION.key(), "dummy");
    UrsaBatchInfo batchInfo = TestCommon.getTestBatchInfo();
    List<TestUrsaBatchS3EventsSource.BatchRecord> batches = new ArrayList<>();
    batches.add(new TestUrsaBatchS3EventsSource.BatchRecord(TestCommon.TEST_BUCKET_REGION, TestCommon.TEST_BATCH_BUCKET_NAME, TestCommon.TEST_BATCH_BUCKET_KEY));
    return Pair.of(Pair.of(props, batchInfo), sparkSession.createDataset(batches, Encoders.bean(TestUrsaBatchS3EventsSource.BatchRecord.class)).toDF());
  }
}
