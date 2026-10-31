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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link JavaHoodieMetadataBulkInsertPartitioner}, which sorts MDT/HFile record keys by raw
 * UTF-8 bytes rather than String (UTF-16) order.
 */
class TestJavaHoodieMetadataBulkInsertPartitioner {

  @Test
  void repartitionRecordsSortsBinaryKeysByUtf8Bytes() {
    // U+E000 (UTF-8 lead byte 0xEE) sorts BEFORE U+20000 (UTF-8 lead byte 0xF0) in raw UTF-8 byte
    // order, but AFTER it under String.compareTo (UTF-16). This is the pathological pair the
    // partitioner's UTF-8 comparator must get right so HFile forward-only seeks stay valid.
    String bmpPrivateUse = new String(Character.toChars(0xE000));
    String supplementary = new String(Character.toChars(0x20000));
    // All records share one file group, so the order is the record key order.
    String fileId = "files-0000";

    // Shuffled input mixing both prefixes plus ascii suffixes.
    List<String> inputKeys = Arrays.asList(
        supplementary + "-b",
        "ascii-key",
        bmpPrivateUse + "-a",
        supplementary + "-a",
        bmpPrivateUse + "-b");

    List<HoodieRecord<EmptyHoodieRecordPayload>> records = new ArrayList<>();
    for (String key : inputKeys) {
      HoodieRecord<EmptyHoodieRecordPayload> record =
          new HoodieAvroRecord<>(new HoodieKey(key, ""), new EmptyHoodieRecordPayload());
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("001", fileId));
      record.seal();
      records.add(record);
    }

    JavaHoodieMetadataBulkInsertPartitioner<EmptyHoodieRecordPayload> partitioner =
        new JavaHoodieMetadataBulkInsertPartitioner<>();
    List<HoodieRecord<EmptyHoodieRecordPayload>> sorted = partitioner.repartitionRecords(records, 1);

    assertTrue(partitioner.arePartitionRecordsSorted(), "Records must be sorted");

    List<String> actualKeys = new ArrayList<>();
    for (HoodieRecord<EmptyHoodieRecordPayload> record : sorted) {
      actualKeys.add(record.getRecordKey());
    }
    List<String> expectedKeys = new ArrayList<>(inputKeys);
    expectedKeys.sort(StringUtils.UTF8_LEXICOGRAPHIC_COMPARATOR);
    assertEquals(expectedKeys, actualKeys, "Records must be sorted by UTF-8 byte order");

    // The divergent pair: every U+E000-prefixed key precedes every U+20000-prefixed key in UTF-8
    // byte order, the opposite of String.compareTo (UTF-16) order.
    int lastBmpIndex = -1;
    int firstSupplementaryIndex = actualKeys.size();
    for (int i = 0; i < actualKeys.size(); i++) {
      if (actualKeys.get(i).startsWith(bmpPrivateUse)) {
        lastBmpIndex = i;
      } else if (actualKeys.get(i).startsWith(supplementary) && firstSupplementaryIndex == actualKeys.size()) {
        firstSupplementaryIndex = i;
      }
    }
    assertTrue(lastBmpIndex < firstSupplementaryIndex,
        "All U+E000-prefixed keys should sort before U+20000-prefixed keys in UTF-8 order");
  }

  @Test
  void repartitionRecordsGroupsRecordsByFileGroup() {
    // Records hash to different file groups of a metadata partition; each file group's records must form one sorted
    // run so the bulk insert writes them into their own file group.
    List<Pair<String, String>> inputs = Arrays.asList(
        Pair.of("record-index-0002-0", "a"),
        Pair.of("record-index-0000-0", "c"),
        Pair.of("record-index-0001-0", "b"),
        Pair.of("record-index-0000-0", "a"),
        Pair.of("record-index-0002-0", "b"),
        Pair.of("record-index-0001-0", "a"));
    List<HoodieRecord<EmptyHoodieRecordPayload>> records = new ArrayList<>();
    for (Pair<String, String> input : inputs) {
      HoodieRecord<EmptyHoodieRecordPayload> record =
          new HoodieAvroRecord<>(new HoodieKey(input.getRight(), ""), new EmptyHoodieRecordPayload());
      record.unseal();
      record.setCurrentLocation(new HoodieRecordLocation("001", input.getLeft()));
      record.seal();
      records.add(record);
    }

    List<HoodieRecord<EmptyHoodieRecordPayload>> sorted =
        new JavaHoodieMetadataBulkInsertPartitioner<EmptyHoodieRecordPayload>().repartitionRecords(records, 1);

    List<String> actual = new ArrayList<>();
    for (HoodieRecord<EmptyHoodieRecordPayload> record : sorted) {
      actual.add(JavaHoodieMetadataBulkInsertPartitioner.getFileIdPrefix(record) + "/" + record.getRecordKey());
    }
    assertEquals(Arrays.asList(
        "record-index-0000/a", "record-index-0000/c",
        "record-index-0001/a", "record-index-0001/b",
        "record-index-0002/a", "record-index-0002/b"), actual);
  }
}
