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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.hudi.common.table.read.HoodieFileGroupReaderSchemaHandler.getMandatoryFieldsForMerging;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestHoodieFileGroupReaderSchemaHandler {

  @Mock
  private HoodieTableConfig mockConfig;

  @Mock
  private TypedProperties mockProps;

  @Mock
  private Schema mockSchema;

  @Mock
  private HoodieRecordMerger mockMerger;

  @Test
  void testCustomMergeModeDelegatesToMerger() {
    when(mockConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.CUSTOM);
    when(mockMerger.getMandatoryFieldsForMerging(mockSchema, mockConfig, mockProps))
        .thenReturn(new String[]{"field1", "field2"});

    String[] result = getMandatoryFieldsForMerging(mockConfig, mockProps, mockSchema, Option.of(mockMerger));

    assertArrayEquals(new String[]{"field1", "field2"}, result);
    verify(mockMerger, times(1)).getMandatoryFieldsForMerging(mockSchema, mockConfig, mockProps);
  }

  @Test
  void testMetaFieldsAreIncludedWhenEnabled() {
    when(mockConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    when(mockConfig.populateMetaFields()).thenReturn(true);

    String[] result = getMandatoryFieldsForMerging(mockConfig, mockProps, mockSchema, Option.empty());

    assertArrayEquals(new String[]{HoodieRecord.RECORD_KEY_METADATA_FIELD}, result);
  }

  @Test
  void testRecordKeyFieldsAreUsedWhenMetaFieldsDisabled() {
    when(mockConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    when(mockConfig.populateMetaFields()).thenReturn(false);
    when(mockConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"key1", "key2"}));

    String[] result = getMandatoryFieldsForMerging(mockConfig, mockProps, mockSchema, Option.empty());

    assertArrayEquals(new String[]{"key1", "key2"}, result);
  }

  @Test
  void testPreCombineFieldIsIncludedForEventTimeOrdering() {
    when(mockConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    when(mockConfig.populateMetaFields()).thenReturn(false);
    when(mockConfig.getPreCombineField()).thenReturn("timestamp");
    when(mockConfig.getRecordKeyFields()).thenReturn(Option.empty());

    Triple<RecordMergeMode, String, String> mockMergingConfigs =
        Triple.of(RecordMergeMode.EVENT_TIME_ORDERING, "somePayload", "someStrategy");

    mockStatic(HoodieTableConfig.class);
    when(HoodieTableConfig.inferCorrectMergingBehavior(any(), any(), any(), any(), any()))
        .thenReturn(mockMergingConfigs);

    String[] result = getMandatoryFieldsForMerging(mockConfig, mockProps, mockSchema, Option.empty());

    assertArrayEquals(new String[]{"timestamp"}, result);
  }

  @Test
  void testNoMandatoryFieldsWhenNothingIsSet() {
    when(mockConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    when(mockConfig.populateMetaFields()).thenReturn(false);
    when(mockConfig.getRecordKeyFields()).thenReturn(Option.empty());
    when(mockConfig.getPreCombineField()).thenReturn(null);

    String[] result = getMandatoryFieldsForMerging(mockConfig, mockProps, mockSchema, Option.empty());

    assertArrayEquals(new String[]{}, result);
  }
}

