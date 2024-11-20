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

package org.apache.hudi.index.bucket;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ExtensibleBucketIdentifier extends BucketIdentifier {

  /**
   * Bucket metadata of a partition
   */
  private final HoodieExtensibleBucketMetadata metadata;

  private final Map<Integer/*bucket id*/, HoodieRecordLocation> bucketLocations;

  private boolean pending = false;

  public ExtensibleBucketIdentifier(HoodieExtensibleBucketMetadata metadata) {
    this(metadata, false);
  }

  public ExtensibleBucketIdentifier(HoodieExtensibleBucketMetadata metadata, boolean pending) {
    this(metadata, pending, Collections.emptyMap());
  }

  public ExtensibleBucketIdentifier(HoodieExtensibleBucketMetadata metadata, boolean pending, Map<Integer, HoodieRecordLocation> bucketLocations) {
    this.metadata = metadata;
    this.pending = pending;
    this.bucketLocations = bucketLocations;
  }

  public int getBucketNum() {
    return metadata.getBucketNum();
  }

  public Option<HoodieRecordLocation> getRecordLocation(String recordKey, List<String> indexKeyFields) {
    int bucketId = getBucketId(recordKey, indexKeyFields);
    return Option.ofNullable(bucketLocations.get(bucketId));
  }

  public Option<HoodieRecordLocation> getRecordLocation(HoodieKey key, List<String> indexKeyFields) {
    return getRecordLocation(key.getRecordKey(), indexKeyFields);
  }

  public HoodieRecordLocation getLogicalRecordLocationByIndex(HoodieKey key, List<String> indexKeyFields) {
    String fileIdPrefix = getFileIdPrefix(key.getRecordKey(), indexKeyFields);
    String fileId = FSUtils.createNewFileId(fileIdPrefix, 0);
    return new HoodieRecordLocation(null, fileId);
  }

  public Stream<String/*file id prefix*/> generateFileIdPrefixForAllBuckets() {
    return Stream.iterate(0, i -> i + 1).limit(metadata.getBucketNum())
        .map(bucketId -> BucketIdentifier.newExtensibleBucketFileIdFixedSuffix(bucketId, metadata.getBucketVersion()));
  }

  public Stream<HoodieRecordLocation> generateLogicalRecordLocationForAllBuckets() {
    return generateFileIdPrefixForAllBuckets().map(fileIdPrefix -> new HoodieRecordLocation(null, FSUtils.createNewFileId(fileIdPrefix, 0)));
  }

  public Stream<HoodieRecordLocation> generateRecordLocationForAllBucketsWithLogical() {
    return generateFileIdPrefixForAllBuckets()
        .map(fileIdPrefix -> {
          int bucketId = bucketIdFromFileId(fileIdPrefix);
          if (bucketLocations.containsKey(bucketId)) {
            return bucketLocations.get(bucketId);
          }
          return new HoodieRecordLocation(null, FSUtils.createNewFileId(fileIdPrefix, 0));
        });
  }

  public int getBucketId(HoodieKey key, List<String> indexKeyFields) {
    return getBucketId(key.getRecordKey(), indexKeyFields);
  }

  public int getBucketId(String recordKey, List<String> indexKeyFields) {
    return getBucketId(recordKey, indexKeyFields, metadata.getBucketNum());
  }

  public int getBucketId(String recordKey, String indexKeyFields) {
    return getBucketId(recordKey, indexKeyFields, metadata.getBucketNum());
  }

  public String getFileIdPrefix(String recordKey, List<String> indexKeyFields) {
    int bucketId = getBucketId(recordKey, indexKeyFields);
    short bucketVersion = metadata.getBucketVersion();
    return BucketIdentifier.newExtensibleBucketFileIdFixedSuffix(bucketId, bucketVersion);
  }

  public static ExtensibleBucketId extensibleBucketIdFromFileId(String fileId) {
    return new ExtensibleBucketId(bucketIdFromFileId(fileId), bucketVersionFromFileId(fileId));
  }

  public static short bucketVersionFromFileId(String fileId) {
    return Short.parseShort(bucketVersionStrFromFileId(fileId));
  }

  public static String bucketVersionStrFromFileId(String fileId) {
    return fileId.substring(9, 13);
  }

  public HoodieExtensibleBucketMetadata getMetadata() {
    return metadata;
  }

  public short getBucketVersion() {
    return metadata.getBucketVersion();
  }

  public String getPartitionPath() {
    return metadata.getPartitionPath();
  }

  public boolean isPending() {
    return this.pending;
  }
}
