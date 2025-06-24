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
import org.apache.hudi.keygen.KeyGenUtils;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class BucketIdentifier implements Serializable {
  // Ensure the same records keys from different writers are desired to be distributed into the same bucket
  private static final String CONSTANT_FILE_ID_SUFFIX = "-0000-0000-0000-000000000000";

  public static int getBucketId(String recordKey, List<String> indexKeyFields, int numBuckets) {
    return getBucketId(getHashKeys(recordKey, indexKeyFields), numBuckets);
  }

  public static int getBucketId(String recordKey, String indexKeyFields, int numBuckets) {
    return getBucketId(getHashKeys(recordKey, indexKeyFields), numBuckets);
  }

  public static int getBucketId(List<String> hashKeyFields, int numBuckets) {
    return (hashKeyFields.hashCode() & Integer.MAX_VALUE) % numBuckets;
  }

  protected static List<String> getHashKeys(String recordKey, String indexKeyFields) {
    return getHashKeysUsingIndexFields(recordKey, Arrays.asList(indexKeyFields.split(",")));
  }

  protected static List<String> getHashKeys(String recordKey, List<String> indexKeyFields) {
    return getHashKeysUsingIndexFields(recordKey, indexKeyFields);
  }

  private static List<String> getHashKeysUsingIndexFields(String recordKey, List<String> indexKeyFields) {
    return Arrays.asList(KeyGenUtils.extractRecordKeysByFields(recordKey, indexKeyFields));
  }

  public static String partitionBucketIdStr(String partition, int bucketId) {
    // format: {partition}_{bucket_id}, bucket id should be 8 digits long, padded with leading zeros
    StringBuilder sb = new StringBuilder()
        .append(partition)
        .append('_');
    return appendWithPadZero(bucketId, 8, sb).toString();
  }

  public static int bucketIdFromFileId(String fileId) {
    return Integer.parseInt(fileId.substring(0, 8));
  }

  public static String bucketIdStr(int n) {
    // bucket str should be 8 digits long, padded with leading zeros, format like: "00000001" for bucket 1
    return appendWithPadZero(n, 8, new StringBuilder()).toString();
  }

  private static StringBuilder appendWithPadZero(int num, int targetLength, StringBuilder sb) {
    String numStr = Integer.toString(num);
    int zerosNeeded = targetLength - numStr.length();
    for (int i = 0; i < zerosNeeded; i++) {
      sb.append('0');
    }
    sb.append(numStr);
    return sb;
  }

  public static String newBucketFileIdPrefix(int bucketId, boolean fixed) {
    return fixed ? bucketIdStr(bucketId) + CONSTANT_FILE_ID_SUFFIX : newBucketFileIdPrefix(bucketId);
  }

  public static String newBucketFileIdPrefix(int bucketId) {
    return newBucketFileIdPrefix(bucketIdStr(bucketId));
  }

  public static String newBucketFileIdPrefix(String bucketIdStr) {
    return FSUtils.createNewFileIdPfx().replaceFirst(".{8}", bucketIdStr);
  }

  public static String newBucketFileIdForNBCC(int bucketId) {
    return newBucketFileIdForNBCC(bucketIdStr(bucketId));
  }

  /**
   * Generate a new file id for NBCC mode, file id is fixed for each bucket with format: "{bucket_id}-0000-0000-0000-000000000000-0"
   */
  public static String newBucketFileIdForNBCC(String bucketIdStr) {
    return FSUtils.createNewFileId(bucketIdStr + CONSTANT_FILE_ID_SUFFIX, 0);
  }
}
