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

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.keygen.KeyGenUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class TestBucketIdentifier {

  public static final String NESTED_COL_SCHEMA = "{\"type\":\"record\", \"name\":\"nested_col\",\"fields\": ["
      + "{\"name\": \"prop1\",\"type\": \"string\"},{\"name\": \"prop2\", \"type\": \"long\"}]}";
  public static final String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"long\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"ts_ms\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"nested_col\",\"type\": "
      + NESTED_COL_SCHEMA + "}"
      + "]}";

  public static GenericRecord getRecord() {
    return getRecord(getNestedColRecord("val1", 10L));
  }

  public static GenericRecord getNestedColRecord(String prop1Value, Long prop2Value) {
    GenericRecord nestedColRecord = new GenericData.Record(new Schema.Parser().parse(NESTED_COL_SCHEMA));
    nestedColRecord.put("prop1", prop1Value);
    nestedColRecord.put("prop2", prop2Value);
    return nestedColRecord;
  }

  public static GenericRecord getRecord(GenericRecord nestedColRecord) {
    GenericRecord record = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    record.put("timestamp", 4357686L);
    record.put("_row_key", "key1");
    record.put("ts_ms", "2020-03-21");
    record.put("pii_col", "pi");
    record.put("nested_col", nestedColRecord);
    return record;
  }

  @Test
  public void testBucketFileId() {
    int[] ids = {0, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 1024, 4096, 10000, 100000};
    for (int id : ids) {
      String bucketIdStr = BucketIdentifier.bucketIdStr(id);
      String fileId = BucketIdentifier.newBucketFileIdPrefix(bucketIdStr);
      assert BucketIdentifier.bucketIdFromFileId(fileId) == id;
    }
  }

  @Test
  public void testBucketIdWithSimpleRecordKey() {
    String recordKeyField = "_row_key";
    String indexKeyField = "_row_key";
    GenericRecord record = getRecord();
    HoodieRecord hoodieRecord = new HoodieAvroRecord(
        new HoodieKey(KeyGenUtils.getRecordKey(record, recordKeyField, false), ""), null);
    int bucketId = BucketIdentifier.getBucketId(hoodieRecord, indexKeyField, 8);
    assert bucketId == BucketIdentifier.getBucketId(
        Arrays.asList(record.get(indexKeyField).toString()), 8);
  }

  @Test
  public void testBucketIdWithComplexRecordKey() {
    List<String> recordKeyField = Arrays.asList("_row_key", "ts_ms");
    String indexKeyField = "_row_key";
    GenericRecord record = getRecord();
    HoodieRecord hoodieRecord = new HoodieAvroRecord(
        new HoodieKey(KeyGenUtils.getRecordKey(record, recordKeyField, false), ""), null);
    int bucketId = BucketIdentifier.getBucketId(hoodieRecord, indexKeyField, 8);
    assert bucketId == BucketIdentifier.getBucketId(
        Arrays.asList(record.get(indexKeyField).toString()), 8);
  }

  @Test
  public void testGetHashKeys() {
    BucketIdentifier identifier = new BucketIdentifier();
    List<String> keys = identifier.getHashKeys(new HoodieKey("abc", "partition"), "");
    Assertions.assertEquals(1, keys.size());
    Assertions.assertEquals("abc", keys.get(0));

    keys = identifier.getHashKeys(new HoodieKey("f1:abc", "partition"), "f1");
    Assertions.assertEquals(1, keys.size());
    Assertions.assertEquals("abc", keys.get(0));

    keys = identifier.getHashKeys(new HoodieKey("f1:abc,f2:bcd", "partition"), "f2");
    Assertions.assertEquals(1, keys.size());
    Assertions.assertEquals("bcd", keys.get(0));

    keys = identifier.getHashKeys(new HoodieKey("f1:abc,f2:bcd", "partition"), "f1,f2");
    Assertions.assertEquals(2, keys.size());
    Assertions.assertEquals("abc", keys.get(0));
    Assertions.assertEquals("bcd", keys.get(1));
  }
}
