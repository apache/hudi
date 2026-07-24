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

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieRecordUtils {

  @Test
  void loadHoodieMerge() {
    String mergeClassName = HoodieAvroRecordMerger.class.getName();
    HoodieRecordMerger recordMerger1 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    HoodieRecordMerger recordMerger2 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    assertEquals(recordMerger1.getClass().getName(), mergeClassName);
    assertEquals(recordMerger2.getClass().getName(), mergeClassName);
  }

  @Test
  void loadHoodieMergeWithWrongMerger() {
    String mergeClassName = "wrong.package.MergerName";
    assertThrows(HoodieException.class, () -> HoodieRecordUtils.loadRecordMerger(mergeClassName));
  }

  @Test
  void loadPayload() {
    String payloadClassName = DefaultHoodieRecordPayload.class.getName();
    HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(payloadClassName, null, 0);
    assertEquals(payload.getClass().getName(), payloadClassName);
  }

  @Test
  void sortRecordsByRecordKey() {
    // U+E000 (UTF-8 lead byte 0xEE) sorts BEFORE U+20000 (UTF-8 lead byte 0xF0) in raw UTF-8 byte
    // order, but AFTER it under String.compareTo (UTF-16). The sort feeds HFile-backed writers, so
    // it must produce UTF-8 byte order.
    String bmpPrivateUseKey = new String(Character.toChars(0xE000)) + "key";
    String supplementaryKey = new String(Character.toChars(0x20000)) + "key";
    List<HoodieRecord<DefaultHoodieRecordPayload>> records = Arrays.asList(
        record("key3"),
        record(supplementaryKey),
        record("key1"),
        record(bmpPrivateUseKey),
        record("key2"));

    Iterator<HoodieRecord<DefaultHoodieRecordPayload>> sortedRecords =
        HoodieRecordUtils.sortRecordsByRecordKey(records.iterator());

    List<String> sortedKeys = new ArrayList<>();
    sortedRecords.forEachRemaining(record -> sortedKeys.add(record.getRecordKey()));
    assertEquals(Arrays.asList("key1", "key2", "key3", bmpPrivateUseKey, supplementaryKey), sortedKeys);
  }

  @Test
  void testGetOrderingFields() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    TypedProperties props = new TypedProperties();
    // Assert empty ordering fields for commit time ordering
    assertTrue(HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient).isEmpty());

    // Assert table config precombine fields are returned when props are not set with event time merge mode
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "tbl");
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    assertEquals(Collections.singletonList("tbl"), HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.EVENT_TIME_ORDERING, metaClient));

    // Assert table config's ordering value is still returned even when props are set to another value
    props.setProperty("hoodie.table.ordering.fields", "props");
    assertEquals(Collections.singletonList("tbl"), HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.EVENT_TIME_ORDERING, metaClient));
  }

  private HoodieRecord<DefaultHoodieRecordPayload> record(String recordKey) {
    return new HoodieAvroRecord<>(new HoodieKey(recordKey, "partition"), new DefaultHoodieRecordPayload(Option.empty()));
  }
}
