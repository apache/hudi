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
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Collections;

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
  void testGetOrderingFields() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    TypedProperties props = new TypedProperties();
    // Assert empty ordering fields for commit time ordering
    assertTrue(HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.COMMIT_TIME_ORDERING, props, metaClient).isEmpty());

    // Assert table config precombine fields are returned when props are not set with event time merge mode
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.PRECOMBINE_FIELDS, "tbl");
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    assertEquals(Collections.singletonList("tbl"), HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.EVENT_TIME_ORDERING, props, metaClient));

    // Assert props value is returned for precombine field configuration when it is set with event time merge mode
    props.setProperty("hoodie.datasource.write.precombine.field", "props");
    assertEquals(Collections.singletonList("props"), HoodieRecordUtils.getOrderingFieldNames(RecordMergeMode.EVENT_TIME_ORDERING, props, metaClient));
  }
}