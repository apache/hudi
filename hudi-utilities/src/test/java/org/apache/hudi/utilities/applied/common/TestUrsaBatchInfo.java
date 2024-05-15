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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;

import java.net.URI;
import java.net.URISyntaxException;

public class TestUrsaBatchInfo {

  private final String testBatchInfoStr = "{\n"
      + "    \"created_timestamp_s\": 1716322055,\n"
      + "    \"origin_created_timestamp_s\": 1716322045,\n"
      + "    \"origin_updated_timestamp_s\": 1716322045,\n"
      + "    \"source_type\": \"perception_frame\",\n"
      + "    \"uuid\": \"018f744d-ba23-7fe7-b1ac-143e71d85673\",\n"
      + "    \"total_count\": 100,\n"
      + "    \"schema_info\": {\n"
      + "        \"type\": \"avro\",\n"
      + "        \"source\": \"file\",\n"
      + "        \"url\": \"s3://<some_bucket>/.../schema.json\"\n"
      + "    },\n"
      + "    \"files_format\": \"parquet\",\n"
      + "    \"files\": [\n"
      + "        \"s3://applied-customer-prod-framegen-raw/file1.parquet\",\n"
      + "        \"s3://applied-customer-prod-framegen-raw/file2.parquet\",\n"
      + "        \"s3://applied-customer-prod-framegen-raw/file3.parquet\"\n"
      + "    ]\n"
      + "}";

  @Test
  public void testUrsaBatchInfo() throws Exception {
    UrsaBatchInfo batchInfo = UrsaBatchInfo.fromJsonString(testBatchInfoStr);
    Assertions.assertEquals(1716322055, batchInfo.getCreatedTimestampSec());
    Assertions.assertEquals(1716322045, batchInfo.getOriginCreatedTimestampSec());
    Assertions.assertEquals(1716322045, batchInfo.getOriginUpdatedTimestampSec());
    Assertions.assertEquals("perception_frame", batchInfo.getSourceType());
    Assertions.assertEquals("018f744d-ba23-7fe7-b1ac-143e71d85673", batchInfo.getUuid());
    Assertions.assertEquals("avro", batchInfo.getSchemaInfo().getType());
    Assertions.assertEquals("file", batchInfo.getSchemaInfo().getSource());
    Assertions.assertEquals("s3://<some_bucket>/.../schema.json", batchInfo.getSchemaInfo().getUrl());
    Assertions.assertEquals("parquet", batchInfo.getFilesFormat());
    Assertions.assertEquals(3, batchInfo.getFiles().size());
    Assertions.assertEquals("s3://applied-customer-prod-framegen-raw/file1.parquet", batchInfo.getFiles().get(0));
    Assertions.assertEquals("s3://applied-customer-prod-framegen-raw/file2.parquet", batchInfo.getFiles().get(1));
    Assertions.assertEquals("s3://applied-customer-prod-framegen-raw/file3.parquet", batchInfo.getFiles().get(2));
    batchInfo.getFiles().forEach(file -> {
      try {
        S3Uri s3Uri = S3Utilities.builder().region(Region.US_WEST_2).build().parseUri(new URI(file));
        String bucket = file.substring(5, file.indexOf('/', 5));
        String key = file.substring(file.indexOf('/', 5) + 1);
        testUrsaS3FilesFromBatchRecord(
            UrsaS3FilesFromBatchRecord.fromBatchInfo("batch_bucket_name", "batch_key",
                batchInfo, s3Uri, "us-west-2"), bucket, key, file);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void testUrsaS3FilesFromBatchRecord(UrsaS3FilesFromBatchRecord record, String fileBucket, String fileKey, String filePath) {
    Assertions.assertEquals("batch_bucket_name", record.getBatchBucketName());
    Assertions.assertEquals("batch_key", record.getBatchKey());
    Assertions.assertEquals(1716322055, record.getBatchCreatedTimestampSec());
    Assertions.assertEquals(1716322045, record.getOriginCreatedTimestampSec());
    Assertions.assertEquals(1716322045, record.getOriginUpdatedTimestampSec());
    Assertions.assertEquals("018f744d-ba23-7fe7-b1ac-143e71d85673", record.getBatchUuid());
    Assertions.assertEquals("perception_frame", record.getBatchSourceType());
    Assertions.assertEquals("parquet", record.getBatchFilesFormat());
    Assertions.assertEquals("avro", record.getBatchSchemaType());
    Assertions.assertEquals("file", record.getBatchSchemaSource());
    Assertions.assertEquals("s3://<some_bucket>/.../schema.json", record.getBatchSchemaUrl());
    Assertions.assertEquals(fileBucket, record.getS3().getBucket().getName());
    Assertions.assertEquals(filePath, record.getFileUrl());
    Assertions.assertEquals(fileKey, record.getS3().getObject().getKey());
    Assertions.assertTrue(record.isValid());
  }

  @Test
  public void testEmptyUrsaBatchInfo() throws Exception {
    Assertions.assertNull(UrsaBatchInfo.fromJsonString(""));
  }
}
