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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.BaseTestCommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.apache.hudi.common.table.timeline.TimelineMetadataUtils.convertMetadataToByteArray;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;

public class TestCommitMetadataSerDeV2 extends BaseTestCommitMetadataSerDe {

  @Override
  protected CommitMetadataSerDe getSerDe() {
    return new CommitMetadataSerDeV2();
  }

  @Override
  protected HoodieInstant createTestInstant(String action, String id) {
    return INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, action, id);
  }

  @Test
  public void testLegacyInstant() throws Exception {
    // Create populated commit metadata
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat writeStat = createTestWriteStat();
    metadata.addWriteStat(TEST_PARTITION_PATH, writeStat);

    // Set other metadata fields
    metadata.setOperationType(WriteOperationType.INSERT);
    metadata.setCompacted(true);
    metadata.addMetadata("test-key", "test-value");

    // Create SerDe instance and test instant
    CommitMetadataSerDe serDeV1 = new CommitMetadataSerDeV1();
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.INFLIGHT, "commit", "001", "002", true, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);


    // Serialize and deserialize
    byte[] serialized = convertMetadataToByteArray(metadata, serDeV1);
    HoodieCommitMetadata deserialized = getSerDe().deserialize(instant, new ByteArrayInputStream(serialized), () -> false, HoodieCommitMetadata.class);
    verifyCommitMetadata(deserialized);
    verifyWriteStat(deserialized.getPartitionToWriteStats().get(TEST_PARTITION_PATH).get(0));
  }
}
