/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.agent.architect;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Source-backed facts used by the assessment-only PR7 Writer Contract. */
class TestFlinkArchitectWriterContractFixtures {

  @Test
  void testPinnedMultiWriterModesRequireLazyFailedWritesPolicy() {
    assertTrue(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.supportsMultiWriter());
    assertTrue(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.supportsMultiWriter());

    HoodieCleanConfig eagerCleanConfig = HoodieCleanConfig.newBuilder()
        .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.EAGER)
        .build();
    for (WriteConcurrencyMode mode : new WriteConcurrencyMode[] {
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL,
        WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL}) {
      HoodieWriteConfig config = mode == WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL
          ? validNbccBuilder().withCleanConfig(eagerCleanConfig).build()
          : HoodieWriteConfig.newBuilder()
              .withPath("/tmp/hudi-architect-occ")
              .withCleanConfig(eagerCleanConfig)
              .withWriteConcurrencyMode(mode)
              .build();
      assertEquals(HoodieFailedWritesCleaningPolicy.LAZY, config.getFailedWritesCleanPolicy());
    }
  }

  @Test
  void testPinnedNbccRejectsCowAndNonSimpleBucketIndex() {
    IllegalArgumentException cowFailure = assertThrows(
        IllegalArgumentException.class,
        () -> nbccBuilder(HoodieTableType.COPY_ON_WRITE, HoodieIndex.IndexType.SIMPLE)
            .build());
    assertTrue(cowFailure.getMessage().contains(
        "Non-blocking concurrency control requires the MOR table with simple bucket index"));

    IllegalArgumentException indexFailure = assertThrows(
        IllegalArgumentException.class,
        () -> nbccBuilder(HoodieTableType.MERGE_ON_READ, HoodieIndex.IndexType.FLINK_STATE)
            .build());
    assertTrue(indexFailure.getMessage().contains(
        "Non-blocking concurrency control requires the MOR table with simple bucket index"));
  }

  @Test
  void testPinnedWriteOperationNames() {
    Set<String> operations = Arrays.stream(WriteOperationType.values())
        .map(Enum::name)
        .collect(Collectors.toSet());
    assertTrue(operations.containsAll(Set.of(
        "INSERT", "UPSERT", "BULK_INSERT", "DELETE", "COMPACT", "CLUSTER", "UNKNOWN")));
  }

  private static HoodieWriteConfig.Builder validNbccBuilder() {
    return nbccBuilder(HoodieTableType.MERGE_ON_READ, HoodieIndex.IndexType.BUCKET);
  }

  private static HoodieWriteConfig.Builder nbccBuilder(
      HoodieTableType tableType, HoodieIndex.IndexType indexType) {
    TypedProperties properties = new TypedProperties();
    properties.put(HoodieTableConfig.TYPE.key(), tableType.name());
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    HoodieIndexConfig.Builder indexBuilder = HoodieIndexConfig.newBuilder()
        .fromProperties(properties)
        .withIndexType(indexType);
    if (indexType == HoodieIndex.IndexType.BUCKET) {
      indexBuilder.withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.SIMPLE);
    }
    return HoodieWriteConfig.newBuilder()
        .withPath("/tmp/hudi-architect-nbcc")
        .withIndexConfig(indexBuilder.build())
        .withWriteConcurrencyMode(WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL);
  }
}
