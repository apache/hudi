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

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HoodieRecordUtilsTest {

  @Test
  void loadHoodieMerge() {
    String mergeClassName = HoodieAvroRecordMerger.class.getName();
    HoodieRecordMerger recordMerger1 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    HoodieRecordMerger recordMerger2 = HoodieRecordUtils.loadRecordMerger(mergeClassName);
    assertEquals(recordMerger1.getClass().getName(), mergeClassName);
    assertEquals(recordMerger1, recordMerger2);
  }

  @Test
  void loadPayload() {
    String payloadClassName = DefaultHoodieRecordPayload.class.getName();
    HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(payloadClassName, new Object[]{null, 0}, GenericRecord.class, Comparable.class);
    assertEquals(payload.getClass().getName(), payloadClassName);
  }
}