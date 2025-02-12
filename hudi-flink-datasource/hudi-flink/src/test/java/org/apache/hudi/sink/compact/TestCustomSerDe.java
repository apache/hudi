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

package org.apache.hudi.sink.compact;

import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.collection.BitCaskDiskMap;
import org.apache.hudi.common.util.collection.RocksDbDiskMap;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Tests for custom SerDe of non-primitive avro types when using Avro versions > 1.10.0.
 * The avro version used by hudi-flink module is 1.10.0, these tests are placed here so that avro 1.10.0 is used,
 * allowing the error caused by anonymous classes to be thrown.
 */
public class TestCustomSerDe extends HoodieCommonTestHarness {

  @BeforeEach
  public void setup() {
    initPath();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testBitCaskDiskMapPutDecimal(boolean isCompressionEnabled) throws IOException {
    try (BitCaskDiskMap<String, HoodieRecord> bitCaskDiskMap = new BitCaskDiskMap<>(basePath, new DefaultSerializer<>(), isCompressionEnabled)) {
      HoodieRecord avroRecord = createAvroRecordWithDecimalOrderingField();
      bitCaskDiskMap.put(avroRecord.getRecordKey(), avroRecord);
      assertDoesNotThrow(() -> bitCaskDiskMap.get(avroRecord.getRecordKey()));
    }
  }

  @Test
  public void testRocksDbDiskMapPutDecimal() throws IOException {
    try (RocksDbDiskMap<String, HoodieRecord> rocksDbBasedMap = new RocksDbDiskMap<>(basePath, new DefaultSerializer<>())) {
      HoodieRecord avroRecord = createAvroRecordWithDecimalOrderingField();
      rocksDbBasedMap.put(avroRecord.getRecordKey(), avroRecord);
      assertDoesNotThrow(() -> rocksDbBasedMap.get(avroRecord.getRecordKey()));
    }
  }

  private static HoodieRecord createAvroRecordWithDecimalOrderingField() {
    Schema precombineFieldSchema = LogicalTypes.decimal(20, 0)
        .addToSchema(Schema.createFixed("fixed", null, "record.precombineField", 9));
    byte[] decimalFieldBytes = new byte[] {0, 0, 0, 1, -122, -16, -116, -90, -32};
    GenericFixed genericFixed = new GenericData.Fixed(precombineFieldSchema, decimalFieldBytes);

    // nullifying the record attribute in EventTimeAvroPayload here as it is not required in the test
    return new HoodieAvroRecord<>(new HoodieKey("recordKey", "partitionPath"),
        new EventTimeAvroPayload(null, (Comparable) genericFixed));
  }

}
