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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.CollectionUtils;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestFlinkWriteHelper {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"testrec\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

  @Test
  void testDeduplicateRecordsPreservesInputKeyOrder() {
    List<HoodieRecord<HoodieAvroPayload>> records = Arrays.asList(record("b"), record("a"), record("c"));
    @SuppressWarnings("unchecked")
    FlinkWriteHelper<HoodieAvroPayload, Object> writeHelper = FlinkWriteHelper.newInstance();

    List<String> deduplicatedKeys = CollectionUtils.toStream(
        writeHelper.deduplicateRecords(
            records.iterator(),
            null,
            -1,
            SCHEMA,
            new TypedProperties(),
            null,
            null,
            new String[0]))
        .map(HoodieRecord::getRecordKey)
        .collect(Collectors.toList());

    assertEquals(Arrays.asList("b", "a", "c"), deduplicatedKeys);
  }

  private static HoodieRecord<HoodieAvroPayload> record(String recordKey) {
    return new HoodieAvroRecord<>(new HoodieKey(recordKey, "partition"), null);
  }
}
