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

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;
import org.junit.jupiter.api.Test;

public class TestBucketIdentifier {

  @Test
  public void testBucketFileId() {
    for (int i = 0; i < 1000; i++) {
      String bucketId = BucketIdentifier.bucketIdStr(i);
      String fileId = BucketIdentifier.newBucketFileIdPrefix(bucketId);
      assert BucketIdentifier.bucketIdFromFileId(fileId) == i;
    }
  }

  @Test
  public void testBucketIdWithSimpleRecordKey() {
    String recordKeyField = "_row_key";
    String indexKeyField = "_row_key";
    GenericRecord record = KeyGeneratorTestUtilities.getRecord();
    HoodieRecord hoodieRecord = new HoodieRecord(
        new HoodieKey(KeyGenUtils.getRecordKey(record, recordKeyField), ""), null);
    int bucketId = BucketIdentifier.getBucketId(hoodieRecord, indexKeyField, 8);
    assert bucketId == BucketIdentifier.getBucketId(
        Arrays.asList(record.get(indexKeyField).toString()), 8);
  }

  @Test
  public void testBucketIdWithComplexRecordKey() {
    List<String> recordKeyField = Arrays.asList("_row_key","ts_ms");
    String indexKeyField = "_row_key";
    GenericRecord record = KeyGeneratorTestUtilities.getRecord();
    HoodieRecord hoodieRecord = new HoodieRecord(
        new HoodieKey(KeyGenUtils.getRecordKey(record, recordKeyField), ""), null);
    int bucketId = BucketIdentifier.getBucketId(hoodieRecord, indexKeyField, 8);
    assert bucketId == BucketIdentifier.getBucketId(
        Arrays.asList(record.get(indexKeyField).toString()), 8);
  }
}
