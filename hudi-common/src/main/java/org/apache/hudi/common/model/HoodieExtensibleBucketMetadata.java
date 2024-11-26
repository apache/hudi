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

package org.apache.hudi.common.model;

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * All the metadata that is used for extensible bucket index
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieExtensibleBucketMetadata implements Serializable {

  public static final Short INIT_BUCKET_VERSION = 0;

  public static final String INIT_INSTANT = HoodieTimeline.INIT_INSTANT_TS;

  public static final String BUCKET_METADATA_FILE_SUFFIX = ".bucket_meta";

  public static final String BUCKET_METADATA_COMMIT_FILE_SUFFIX = ".commit";

  /**
   * Bucket layout version, increment when bucket resizing happens, start from 0
   */
  private final short bucketVersion;

  /**
   * Bucket metadata belongs to which partition
   */
  private final String partitionPath;

  /**
   * Instant time of the metadata
   * - For initial bucket layout, it is the instant time of value: {@link HoodieExtensibleBucketMetadata#INIT_INSTANT}
   * - For bucket resizing, it is the instant time of the resizing operation
   */
  private final String instant;

  /**
   * Number of buckets in the current version layout
   */
  private final int bucketNum;

  /**
   * Number of buckets in the previous version layout
   */
  private final int lastVersionBucketNum;

  /**
   * Extra metadata for extensible bucket index
   */
  private final Map<String, String> extraMetadata;

  @JsonCreator
  public HoodieExtensibleBucketMetadata(@JsonProperty("bucket_version") short bucketVersion, @JsonProperty("partition_path") String partitionPath,
                                        @JsonProperty("instant") String instant, @JsonProperty("bucket_num") int bucketNum,
                                        @JsonProperty("last_version_bucket_num") int lastVersionBucketNum,
                                        @JsonProperty("extra_metadata") Map<String, String> extraMetadata) {
    this.bucketVersion = bucketVersion;
    this.partitionPath = partitionPath;
    this.instant = instant;
    this.bucketNum = bucketNum;
    this.lastVersionBucketNum = lastVersionBucketNum;
    this.extraMetadata = extraMetadata;
  }

  public static HoodieExtensibleBucketMetadata initialVersionMetadata(String partitionPath, int bucketNum) {
    return new HoodieExtensibleBucketMetadata(INIT_BUCKET_VERSION, partitionPath, INIT_INSTANT, bucketNum, bucketNum, Collections.emptyMap());
  }

  public static HoodieExtensibleBucketMetadata fromBytes(byte[] bytes) throws IOException {
    try {
      return fromJsonString(fromUTF8Bytes(bytes), HoodieExtensibleBucketMetadata.class);
    } catch (Exception e) {
      throw new IOException("Failed to de-serialize HoodieExtensibleBucketMetadata from bytes", e);
    }
  }

  public byte[] toBytes() throws IOException {
    return getUTF8Bytes(toJsonString());
  }

  /**
   * Get instant time from the extensible bucket metadata filename
   * Pattern of the filename: <instant>.BUCKET_METADATA_FILE_SUFFIX/BUCKET_METADATA_COMMIT_FILE_SUFFIX
   */
  public static String getInstantFromFile(String filename) {
    return filename.split("\\.")[0];
  }

  private String toJsonString() throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  private static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or something bad happen).
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  public String getFilename() {
    return instant + BUCKET_METADATA_FILE_SUFFIX;
  }

  public short getBucketVersion() {
    return bucketVersion;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getInstant() {
    return instant;
  }

  public int getBucketNum() {
    return bucketNum;
  }

  public int getLastVersionBucketNum() {
    return lastVersionBucketNum;
  }

  public Map<String, String> getExtraMetadata() {
    return extraMetadata;
  }

  @Override
  public String toString() {
    return "HoodieExtensibleBucketMetadata{"
        + "bucketVersion=" + bucketVersion
        + ", partitionPath='" + partitionPath + '\''
        + ", instant='" + instant + '\''
        + ", bucketNum=" + bucketNum
        + ", lastVersionBucketNum=" + lastVersionBucketNum
        + ", extraMetadata=" + extraMetadata
        + '}';
  }
}
