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

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.AvroBinaryTestPayload;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link HoodieRecord}.
 */
public class TestHoodieRecord {

  private HoodieRecord hoodieRecord;

  @BeforeEach
  public void setUp() throws Exception {
    final List<IndexedRecord> indexedRecords = SchemaTestUtil.generateHoodieTestRecords(0, 1);
    final List<HoodieRecord> hoodieRecords =
        indexedRecords.stream().map(r -> new HoodieAvroRecord(new HoodieKey(UUID.randomUUID().toString(), "0000/00/00"),
            new AvroBinaryTestPayload(Option.of((GenericRecord) r)))).collect(Collectors.toList());
    hoodieRecord = hoodieRecords.get(0);
  }

  @Test
  public void testModificationAfterSeal() {
    hoodieRecord.seal();
    final HoodieRecordLocation location = new HoodieRecordLocation("100", "0");
    assertThrows(UnsupportedOperationException.class, () -> {
      hoodieRecord.setCurrentLocation(location);
    }, "should fail since modification after sealed is not allowed");
  }

  @Test
  public void testNormalModification() {
    hoodieRecord.unseal();
    final HoodieRecordLocation location = new HoodieRecordLocation("100", "0");
    hoodieRecord.setCurrentLocation(location);
    hoodieRecord.seal();

    hoodieRecord.unseal();
    hoodieRecord.setNewLocation(location);
    hoodieRecord.seal();
  }
}
