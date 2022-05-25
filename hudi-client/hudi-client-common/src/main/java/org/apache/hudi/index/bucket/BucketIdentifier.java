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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BucketIdentifier implements Serializable {
  // Compatible with the spark bucket name
  private static final Pattern BUCKET_NAME = Pattern.compile(".*_(\\d+)(?:\\..*)?$");

  public static int getBucketId(HoodieRecord record, String indexKeyFields, int numBuckets) {
    return getBucketId(record.getKey(), indexKeyFields, numBuckets);
  }

  public static int getBucketId(HoodieKey hoodieKey, String indexKeyFields, int numBuckets) {
    return (getHashKeys(hoodieKey, indexKeyFields).hashCode() & Integer.MAX_VALUE) % numBuckets;
  }

  public static int getBucketId(HoodieKey hoodieKey, List<String> indexKeyFields, int numBuckets) {
    return (getHashKeys(hoodieKey.getRecordKey(), indexKeyFields).hashCode() & Integer.MAX_VALUE) % numBuckets;
  }

  public static int getBucketId(String recordKey, String indexKeyFields, int numBuckets) {
    return getBucketId(getHashKeys(recordKey, indexKeyFields), numBuckets);
  }

  public  static int getBucketId(List<String> hashKeyFields, int numBuckets) {
    return (hashKeyFields.hashCode() & Integer.MAX_VALUE) % numBuckets;
  }

  public static List<String> getHashKeys(HoodieKey hoodieKey, String indexKeyFields) {
    return getHashKeys(hoodieKey.getRecordKey(), indexKeyFields);
  }

  protected static List<String> getHashKeys(String recordKey, String indexKeyFields) {
    return !recordKey.contains(":") ? Collections.singletonList(recordKey) :
        getHashKeysUsingIndexFields(recordKey, Arrays.asList(indexKeyFields.split(",")));
  }

  protected static List<String> getHashKeys(String recordKey, List<String> indexKeyFields) {
    return !recordKey.contains(":") ? Collections.singletonList(recordKey) :
        getHashKeysUsingIndexFields(recordKey, indexKeyFields);
  }

  private static List<String> getHashKeysUsingIndexFields(String recordKey, List<String> indexKeyFields) {
    Map<String, String> recordKeyPairs = Arrays.stream(recordKey.split(","))
        .map(p -> p.split(":"))
        .collect(Collectors.toMap(p -> p[0], p -> p[1]));
    return indexKeyFields.stream()
        .map(recordKeyPairs::get).collect(Collectors.toList());
  }

  public static String partitionBucketIdStr(String partition, int bucketId) {
    return String.format("%s_%s", partition, bucketIdStr(bucketId));
  }

  public static int bucketIdFromFileId(String fileId) {
    return Integer.parseInt(fileId.substring(0, 8));
  }

  public static String bucketIdStr(int n) {
    return String.format("%08d", n);
  }

  public static String newBucketFileIdPrefix(int bucketId) {
    return newBucketFileIdPrefix(bucketIdStr(bucketId));
  }

  public static String newBucketFileIdPrefix(String bucketId) {
    return FSUtils.createNewFileIdPfx().replaceFirst(".{8}", bucketId);
  }

  public static boolean isBucketFileName(String name) {
    return BUCKET_NAME.matcher(name).matches();
  }

  public static int mod(int x, int y) {
    return x % y;
  }
}
