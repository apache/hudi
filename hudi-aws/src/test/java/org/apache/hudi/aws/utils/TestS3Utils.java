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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.hudi.aws.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestS3Utils {

  @Test
  void testS3aToS3_AWS() {
    // Test cases for AWS S3 URLs
    assertEquals("s3://my-bucket/path/to/object", S3Utils.s3aToS3("s3a://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket", S3Utils.s3aToS3("s3a://my-bucket"));
    assertEquals("s3://MY-BUCKET/PATH/TO/OBJECT", S3Utils.s3aToS3("s3a://MY-BUCKET/PATH/TO/OBJECT"));
    assertEquals("s3://my-bucket/path/to/object", S3Utils.s3aToS3("S3a://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket/path/to/object", S3Utils.s3aToS3("s3A://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket/path/to/object", S3Utils.s3aToS3("S3A://my-bucket/path/to/object"));
    assertEquals("s3://my-bucket/s3a://another-bucket/another/path", S3Utils.s3aToS3("s3a://my-bucket/s3a://another-bucket/another/path"));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "gs://my-bucket/path/to/object",
      "gs://my-bucket",
      "gs://MY-BUCKET/PATH/TO/OBJECT",
      "https://myaccount.blob.core.windows.net/mycontainer/path/to/blob",
      "https://myaccount.blob.core.windows.net/MYCONTAINER/PATH/TO/BLOB",
      "https://example.com/path/to/resource",
      "http://example.com",
      "ftp://example.com/resource",
      "",
      "gs://my-bucket/path/to/s3a://object",
      "gs://my-bucket s3a://my-object",

  })
  void testUriDoesNotChange(String uri) {
    assertEquals(uri, S3Utils.s3aToS3(uri));
  }
}
