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

package org.apache.hudi.utilities.applied.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.applied.common.TestCommon;
import org.apache.hudi.utilities.applied.common.UrsaBatchInfo;
import org.apache.hudi.utilities.applied.schema.ParquetDataGeneratorWithProtoColumn;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_BUCKET_NAME;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_BUCKET_REGION;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_CREATED_TIMESTAMP_SEC;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_FILES_FORMAT;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_KEY;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_SCHEMA_SOURCE;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_SCHEMA_TYPE;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_SCHEMA_URL;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_SOURCE_TYPE;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.BATCH_UUID;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.FILE_URL;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.IS_VALID;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.ORIGIN_CREATED_TIMESTAMP_SEC;
import static org.apache.hudi.utilities.applied.common.UrsaS3FilesFromBatchRecord.ORIGIN_UPDATED_TIMESTAMP_SEC;

public class TestUrsaBatchS3EventsSource extends UtilitiesTestBase {

  protected static class TestFileValidatorFunction extends UrsaBatchS3EventsSource.FileValidatorFunction {
    @Override
    protected Pair<Long, Option<String>> getS3FileLength(String region, String bucket, String key) {
      return Pair.of(new Long(100), Option.empty());
    }
  }

  protected static class TestBatchRecordGenFunction extends UrsaBatchS3EventsSource.BatchRecordGenFunction {
    private final UrsaBatchInfo batchInfo;

    public TestBatchRecordGenFunction(UrsaBatchInfo batchInfo) {
      this.batchInfo = batchInfo;
    }

    protected UrsaBatchInfo getBatchInfo(String region, String bucket, String key) {
      return batchInfo;
    }

    protected S3Uri getS3Uri(String file) {
      try {
        S3Utilities utils = S3Utilities.builder().region(Region.of("us-west-1")).build();
        S3Uri uri = utils.parseUri(new URI(file));
        return uri;
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class BatchRecord implements Serializable {
    private String awsRegion;
    private String name;
    private String key;

    public BatchRecord() {
    }

    public BatchRecord(String awsRegion, String name, String key) {
      this.awsRegion = awsRegion;
      this.name = name;
      this.key = key;
    }

    public String getAwsRegion() {
      return awsRegion;
    }

    public void setAwsRegion(String awsRegion) {
      this.awsRegion = awsRegion;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getKey() {
      return key;
    }

    public void setKey(String key) {
      this.key = key;
    }
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    UtilitiesTestBase.initTestServices(false, false, false);
  }

  @Test
  public void testSource() {
    Pair<Pair<TypedProperties, UrsaBatchInfo>, Dataset<Row>> res = TestCommon.generateUrsaBatchS3Events(sparkSession);
    TypedProperties props = res.getLeft().getLeft();
    UrsaBatchInfo batchInfo = res.getLeft().getRight();
    Dataset<Row> df = res.getRight();
    df.show(10, false);
    UrsaBatchS3EventsSource s3EventsSource = new UrsaBatchS3EventsSource(props, jsc, sparkSession, null);
    Dataset<Row> result = s3EventsSource.fetchParquetFilesFromBatches(df, new TestBatchRecordGenFunction(batchInfo), new TestFileValidatorFunction());
    Row[] rows = result.collectAsList().toArray(new Row[0]);
    Assertions.assertEquals(5, rows.length);
    for (int i = 0; i < rows.length; i++) {
      Assertions.assertEquals(batchInfo.getUuid(), rows[i].getAs(BATCH_UUID));
      Assertions.assertEquals(batchInfo.getSourceType(), rows[i].getAs(BATCH_SOURCE_TYPE));
      Assertions.assertEquals(batchInfo.getFilesFormat(), rows[i].getAs(BATCH_FILES_FORMAT));
      Assertions.assertEquals(batchInfo.getSchemaInfo().getSource(), rows[i].getAs(BATCH_SCHEMA_SOURCE));
      Assertions.assertEquals(batchInfo.getSchemaInfo().getUrl(), rows[i].getAs(BATCH_SCHEMA_URL));
      Assertions.assertEquals(batchInfo.getSchemaInfo().getType(), rows[i].getAs(BATCH_SCHEMA_TYPE));
      Assertions.assertEquals(batchInfo.getCreatedTimestampSec(), (Long) rows[i].getAs(BATCH_CREATED_TIMESTAMP_SEC));
      Assertions.assertEquals(batchInfo.getOriginCreatedTimestampSec(), (Long) rows[i].getAs(ORIGIN_CREATED_TIMESTAMP_SEC));
      Assertions.assertEquals(batchInfo.getOriginUpdatedTimestampSec(), (Long) rows[i].getAs(ORIGIN_UPDATED_TIMESTAMP_SEC));
      Assertions.assertEquals(Boolean.TRUE, rows[i].getAs(IS_VALID));
      Assertions.assertEquals(TestCommon.TEST_BATCH_BUCKET_KEY, rows[i].getAs(BATCH_KEY));
      Assertions.assertEquals(TestCommon.TEST_BUCKET_REGION, rows[i].getAs(BATCH_BUCKET_REGION));
      Assertions.assertEquals(TestCommon.TEST_BATCH_BUCKET_NAME, rows[i].getAs(BATCH_BUCKET_NAME));
      Assertions.assertEquals(batchInfo.getFiles().get(i), rows[i].getAs(FILE_URL));
      Row s3Column = rows[i].getAs("s3");
      Row s3Bucket = s3Column.getAs("bucket");
      Row objCol = s3Column.getAs("object");
      Assertions.assertEquals(TestCommon.TEST_BUCKET, s3Bucket.getAs("name"));
      Assertions.assertEquals(batchInfo.getFiles().get(i).substring(TestCommon.TEST_BUCKET_WITH_SCHEME.length()), objCol.getAs("key"));
    }
  }

  @Test
  public void testGenerateParquetFiles() throws IOException {
    ParquetDataGeneratorWithProtoColumn.main(new String[]{});
  }
}

