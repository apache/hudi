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

import org.apache.hudi.common.util.collection.Pair;

import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3Uri;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class UrsaS3FilesFromBatchRecord implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String BATCH_CREATED_TIMESTAMP_SEC = "_lake_batch_created_timestamp_sec";
  public static final String ORIGIN_CREATED_TIMESTAMP_SEC = "_lake_origin_created_timestamp_sec";
  public static final String ORIGIN_UPDATED_TIMESTAMP_SEC = "_lake_origin_updated_timestamp_sec";
  public static final String BATCH_SOURCE_TYPE = "_lake_source_type";
  public static final String BATCH_SCHEMA_TYPE = "_lake_batch_schema_type";
  public static final String BATCH_SCHEMA_SOURCE = "_lake_batch_schema_source";
  public static final String BATCH_SCHEMA_URL = "_lake_batch_schema_url";
  public static final String BATCH_FILES_FORMAT = "_lake_file_format";
  public static final String FILE_URL = "_lake_file_url";
  public static final String IS_VALID = "_lake_file_valid";
  public static final String BATCH_BUCKET_NAME = "_lake_bucket";
  public static final String BATCH_BUCKET_REGION = "_lake_region";
  public static final String BATCH_KEY = "_lake_batch_key";
  public static final String BATCH_UUID = "_lake_batch_uuid";
  // Do NOT change S3 column name as it is part of contract how downstream uses.
  public static final String S3 = "s3";

  // Column Order for adding to the files table. New columns need to be appended.
  public static final String[] ORDERED_COLUMNS_IN_FILES_TABLE = {
      BATCH_UUID, BATCH_BUCKET_REGION, BATCH_BUCKET_NAME, BATCH_SOURCE_TYPE,
      BATCH_KEY, BATCH_CREATED_TIMESTAMP_SEC, ORIGIN_CREATED_TIMESTAMP_SEC,
      ORIGIN_UPDATED_TIMESTAMP_SEC, BATCH_SCHEMA_SOURCE, BATCH_SCHEMA_TYPE,
      BATCH_SCHEMA_URL, BATCH_FILES_FORMAT, FILE_URL, IS_VALID, S3
  };

  public static final String[] FILES_COLUMNS_TO_ADD_IN_DATA_SCHEMA = {
      BATCH_UUID, BATCH_BUCKET_REGION, BATCH_BUCKET_NAME, BATCH_SOURCE_TYPE,
      BATCH_KEY, BATCH_CREATED_TIMESTAMP_SEC, ORIGIN_CREATED_TIMESTAMP_SEC,
      ORIGIN_UPDATED_TIMESTAMP_SEC, FILE_URL
  };

  @Nonnull
  private String batchBucketName;
  @Nonnull
  private String batchKey;
  @Nonnull
  private String batchUuid;
  @Nonnull
  private String fileUrl;
  @Nonnull
  private S3Info s3 = new S3Info();
  private String batchBucketRegion;
  private String batchSourceType;
  private long batchCreatedTimestampSec;
  private long originCreatedTimestampSec;
  private long originUpdatedTimestampSec;
  private String batchSchemaType;
  private String batchSchemaSource;
  private String batchSchemaUrl;
  private String batchFilesFormat;
  private boolean isValid;

  public static Map<String, String> FIELD_TO_COLUMN_MAP = Arrays.asList(
        Pair.of("batchBucketName", BATCH_BUCKET_NAME),
        Pair.of("batchKey", BATCH_KEY),
        Pair.of("batchUuid", BATCH_UUID),
        Pair.of("fileUrl", FILE_URL),
        Pair.of("s3", S3),
        Pair.of("batchBucketRegion", BATCH_BUCKET_REGION),
        Pair.of("batchSourceType", BATCH_SOURCE_TYPE),
        Pair.of("batchCreatedTimestampSec", BATCH_CREATED_TIMESTAMP_SEC),
        Pair.of("originCreatedTimestampSec", ORIGIN_CREATED_TIMESTAMP_SEC),
        Pair.of("originUpdatedTimestampSec", ORIGIN_UPDATED_TIMESTAMP_SEC),
        Pair.of("batchSchemaType", BATCH_SCHEMA_TYPE),
        Pair.of("batchSchemaSource", BATCH_SCHEMA_SOURCE),
        Pair.of("batchSchemaUrl", BATCH_SCHEMA_URL),
        Pair.of("batchFilesFormat", BATCH_FILES_FORMAT),
        Pair.of("valid", IS_VALID)
  ).stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));

  public UrsaS3FilesFromBatchRecord() {
  }

  public static UrsaS3FilesFromBatchRecord fromBatchInfo(String batchBucketName, String batchKey,
                                                            UrsaBatchInfo batchInfo, S3Uri inputFile, String region) {
    return fromBatchInfo(batchBucketName, batchKey, batchInfo, inputFile, true, 0, null, region);
  }

  public static UrsaS3FilesFromBatchRecord fromBatchInfo(String batchBucketName, String batchKey,
                                                            UrsaBatchInfo batchInfo, S3Uri inputFile,
                                                            boolean isValid, long contentLength, String errorDetails, String region) {
    UrsaS3FilesFromBatchRecord record = new UrsaS3FilesFromBatchRecord();
    record.setBatchBucketName(batchBucketName);
    record.setBatchKey(batchKey);
    record.setBatchBucketRegion(region);
    record.setBatchCreatedTimestampSec(batchInfo.getCreatedTimestampSec());
    record.setOriginCreatedTimestampSec(batchInfo.getOriginCreatedTimestampSec());
    record.setOriginUpdatedTimestampSec(batchInfo.getOriginUpdatedTimestampSec());
    record.setBatchUuid(batchInfo.getUuid());
    record.setBatchSourceType(batchInfo.getSourceType());
    record.setBatchFilesFormat(batchInfo.getFilesFormat());
    record.setBatchSchemaType(batchInfo.getSchemaInfo().getType());
    record.setBatchSchemaSource(batchInfo.getSchemaInfo().getSource());
    record.setBatchSchemaUrl(batchInfo.getSchemaInfo().getUrl());
    record.setS3(new S3Info());
    if (inputFile.bucket().isPresent()) {
      record.getS3().setBucket(new BucketInfo(inputFile.bucket().get()));
    }
    if (inputFile.key().isPresent()) {
      record.setFileUrl(inputFile.uri().toString());
      ObjectInfo objectInfo = new ObjectInfo(inputFile.key().get(), contentLength, errorDetails);
      record.getS3().setObject(objectInfo);
    }
    record.setValid(isValid);
    return record;
  }

  public String getBatchBucketRegion() {
    return batchBucketRegion;
  }

  public void setBatchBucketRegion(String batchBucketRegion) {
    this.batchBucketRegion = batchBucketRegion;
  }

  public @NotNull String getBatchBucketName() {
    return batchBucketName;
  }

  public void setBatchBucketName(@NotNull String batchBucketName) {
    this.batchBucketName = batchBucketName;
  }

  public @NotNull String getBatchKey() {
    return batchKey;
  }

  public void setBatchKey(@NotNull String batchKey) {
    this.batchKey = batchKey;
  }

  public @NotNull String getBatchUuid() {
    return batchUuid;
  }

  public void setBatchUuid(@NotNull String batchUuid) {
    this.batchUuid = batchUuid;
  }

  public String getBatchSourceType() {
    return batchSourceType;
  }

  public void setBatchSourceType(String batchSourceType) {
    this.batchSourceType = batchSourceType;
  }

  public long getBatchCreatedTimestampSec() {
    return batchCreatedTimestampSec;
  }

  public void setBatchCreatedTimestampSec(long batchCreatedTimestampSec) {
    this.batchCreatedTimestampSec = batchCreatedTimestampSec;
  }

  public String getBatchSchemaType() {
    return batchSchemaType;
  }

  public void setBatchSchemaType(String batchSchemaType) {
    this.batchSchemaType = batchSchemaType;
  }

  public String getBatchSchemaSource() {
    return batchSchemaSource;
  }

  public void setBatchSchemaSource(String batchSchemaSource) {
    this.batchSchemaSource = batchSchemaSource;
  }

  public String getBatchSchemaUrl() {
    return batchSchemaUrl;
  }

  public void setBatchSchemaUrl(String batchSchemaUrl) {
    this.batchSchemaUrl = batchSchemaUrl;
  }

  public String getBatchFilesFormat() {
    return batchFilesFormat;
  }

  public void setBatchFilesFormat(String batchFilesFormat) {
    this.batchFilesFormat = batchFilesFormat;
  }

  public @NotNull String getFileUrl() {
    return fileUrl;
  }

  public void setFileUrl(@NotNull String fileUrl) {
    this.fileUrl = fileUrl;
  }

  public boolean isValid() {
    return isValid;
  }

  public void setValid(boolean valid) {
    isValid = valid;
  }

  public long getOriginCreatedTimestampSec() {
    return originCreatedTimestampSec;
  }

  public long getOriginUpdatedTimestampSec() {
    return originUpdatedTimestampSec;
  }

  @Nonnull
  public S3Info getS3() {
    return s3;
  }

  public void setS3(@Nonnull S3Info s3) {
    this.s3 = s3;
  }

  public void setOriginUpdatedTimestampSec(long originUpdatedTimestampSec) {
    this.originUpdatedTimestampSec = originUpdatedTimestampSec;
  }

  public void setOriginCreatedTimestampSec(long originCreatedTimestampSec) {
    this.originCreatedTimestampSec = originCreatedTimestampSec;
  }
}
