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

package org.apache.hudi.common.testutils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

// Utility for the schema of S3 events listed here (https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html)
public class S3EventsSchemaUtils {
  public static final String DEFAULT_STRING_VALUE = "default_string";
  public static final GenericRecord DEFAULT_S3_BUCKET_RECORD;
  static {
    GenericRecord rec = new GenericData.Record(generateBucketInfoSchema());
    rec.put("name", "default_s3_bucket");
    DEFAULT_S3_BUCKET_RECORD = rec;
  }

  public static String generateSchemaString() {
    return generateS3EventSchema().toString();
  }

  public static Schema generateObjInfoSchema() {
    Schema objInfo = SchemaBuilder.record("objInfo")
        .fields()
        .requiredString("key")
        .requiredLong("size")
        .endRecord();
    return objInfo;
  }

  public static Schema generateBucketInfoSchema() {
    Schema bucketInfo = SchemaBuilder.record("bucketInfo")
        .fields()
        .requiredString("name")
        .endRecord();
    return bucketInfo;
  }

  public static GenericRecord generateObjInfoRecord(String key, Long size) {
    GenericRecord rec = new GenericData.Record(generateObjInfoSchema());
    rec.put("key", key);
    rec.put("size", size);
    return rec;
  }

  public static Schema generateS3MetadataSchema() {
    Schema s3Metadata = SchemaBuilder.record("s3Metadata")
        .fields()
        .requiredString("configurationId")
        .name("object")
        .type(generateObjInfoSchema())
        .noDefault()
        .name("bucket")
        .type(generateBucketInfoSchema())
        .noDefault()
        .endRecord();
    return s3Metadata;
  }

  public static GenericRecord generateS3MetadataRecord(GenericRecord objRecord) {
    GenericRecord rec = new GenericData.Record(generateS3MetadataSchema());
    rec.put("configurationId", DEFAULT_STRING_VALUE);
    rec.put("object", objRecord);
    rec.put("bucket", DEFAULT_S3_BUCKET_RECORD);
    return rec;
  }

  public static Schema generateS3EventSchema() {
    Schema s3Event = SchemaBuilder.record("s3Event")
        .fields()
        .requiredString("eventSource")
        .requiredString("eventName")
        .requiredString("_row_key")
        .name("s3")
        .type(generateS3MetadataSchema())
        .noDefault()
        .endRecord();
    return s3Event;
  }

  public static GenericRecord generateS3EventRecord(String rowKey, GenericRecord objRecord) {
    GenericRecord rec = new GenericData.Record(generateS3EventSchema());
    rec.put("_row_key", rowKey);
    rec.put("eventSource", DEFAULT_STRING_VALUE);
    rec.put("eventName", DEFAULT_STRING_VALUE);
    rec.put("s3", generateS3MetadataRecord(objRecord));
    return rec;
  }
}
