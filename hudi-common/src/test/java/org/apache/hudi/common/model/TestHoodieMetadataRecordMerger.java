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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link HoodieMetadataRecordMerger}.
 */
public class TestHoodieMetadataRecordMerger {

  private HoodieTestDataGenerator dataGen;

  @BeforeEach
  public void setUp() {
    dataGen = new HoodieTestDataGenerator();
  }

  @AfterEach
  public void cleanUp() {
    if (dataGen != null) {
      dataGen = null;
    }
  }

  @Test
  public void testFullOuterMerge() throws IOException {
    List<HoodieRecord> newRecordList = dataGen.generateInserts("000", 1);
    List<HoodieRecord> updateRecordList = dataGen.generateUpdates("0001", newRecordList);
    HoodieMetadataRecordMerger recordMerger = new HoodieMetadataRecordMerger();
    List<Pair<HoodieRecord, Schema>> mergedRecords = recordMerger.fullOuterMerge(newRecordList.get(0), AVRO_SCHEMA, updateRecordList.get(0), AVRO_SCHEMA, new TypedProperties());
    assertEquals(2, mergedRecords.size());
    assertEquals(updateRecordList.get(0), mergedRecords.get(1).getLeft());
  }
}
