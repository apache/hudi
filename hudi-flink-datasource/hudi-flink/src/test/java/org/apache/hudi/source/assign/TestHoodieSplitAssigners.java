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

package org.apache.hudi.source.assign;

import org.apache.flink.configuration.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieSplitAssigners}.
 */
public class TestHoodieSplitAssigners {

  @Test
  public void testCreateDefaultAssigner() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    config.setString(FlinkOptions.OPERATION.key(), "upsert");

    int parallelism = 5;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for standard COW upsert");
  }

  @Test
  public void testCreateBucketAssignerForMorWithBucketIndex() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.INDEX_TYPE.key(), "BUCKET");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");
    config.setString(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "8");

    int parallelism = 4;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof HoodieSplitBucketAssigner,
        "Should create HoodieSplitBucketAssigner for MOR with bucket index upsert");
  }

  @Test
  public void testCreateNumberAssignerForAppendMode() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    config.setString(FlinkOptions.OPERATION.key(), "insert");

    int parallelism = 10;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof HoodieSplitNumberAssigner,
        "Should create HoodieSplitNumberAssigner for append mode (insert operation)");
  }

  @Test
  public void testCreateDefaultAssignerForCowWithBucketIndex() {
    // COW with bucket index should still use DefaultAssigner (not BucketAssigner)
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    config.setString(FlinkOptions.INDEX_TYPE.key(), "BUCKET");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");
    config.setString(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "8");

    int parallelism = 4;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for COW with bucket index (not MOR)");
  }

  @Test
  public void testCreateDefaultAssignerForMorWithoutBucketIndex() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.INDEX_TYPE.key(), "SIMPLE");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");

    int parallelism = 6;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for MOR without bucket index");
  }

  @Test
  public void testCreateAssignerWithDifferentParallelism() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    config.setString(FlinkOptions.OPERATION.key(), "upsert");

    // Test with various parallelism values
    int[] parallelisms = {1, 5, 10, 50, 100};

    for (int parallelism : parallelisms) {
      HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);
      assertNotNull(assigner, "Assigner should not be null for parallelism " + parallelism);
      assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
          "Should create DefaultHoodieSplitAssigner for parallelism " + parallelism);
    }
  }

  @Test
  public void testFactoryReturnsConsistentAssignerType() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.INDEX_TYPE.key(), "BUCKET");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");
    config.setString(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "16");

    int parallelism = 8;

    // Create multiple assigners with same config
    HoodieSplitAssigner assigner1 = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);
    HoodieSplitAssigner assigner2 = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner1, "First assigner should not be null");
    assertNotNull(assigner2, "Second assigner should not be null");
    assertEquals(assigner1.getClass(), assigner2.getClass(),
        "Same configuration should produce same assigner type");
  }

  @Test
  public void testCreateAssignerForReadOptimizedQuery() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.QUERY_TYPE.key(), "read_optimized");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");

    int parallelism = 4;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for read optimized query");
  }

  @Test
  public void testCreateAssignerForSnapshotQuery() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.QUERY_TYPE.key(), "snapshot");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");

    int parallelism = 4;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for snapshot query without bucket index");
  }

  @Test
  public void testAppendModeWithMor() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.OPERATION.key(), "insert");

    int parallelism = 7;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof HoodieSplitNumberAssigner,
        "Should create HoodieSplitNumberAssigner for MOR in append mode");
  }

  @Test
  public void testBucketIndexWithUpsertDelete() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    config.setString(FlinkOptions.INDEX_TYPE.key(), "BUCKET");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");
    config.setString(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.key(), "32");

    int parallelism = 16;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof HoodieSplitBucketAssigner,
        "Should create HoodieSplitBucketAssigner for bucket index with upsert");
  }

  @Test
  public void testGlobalIndexWithCow() {
    Configuration config = new Configuration();
    config.setString(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
    config.setString(FlinkOptions.INDEX_TYPE.key(), "GLOBAL_SIMPLE");
    config.setString(FlinkOptions.OPERATION.key(), "upsert");

    int parallelism = 12;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for global index with COW");
  }

  @Test
  public void testMinimalConfiguration() {
    // Test with minimal configuration - should use defaults
    Configuration config = new Configuration();

    int parallelism = 3;
    HoodieSplitAssigner assigner = HoodieSplitAssigners.createHoodieSplitAssigner(config, parallelism);

    assertNotNull(assigner, "Assigner should not be null even with minimal config");
    assertTrue(assigner instanceof DefaultHoodieSplitAssigner,
        "Should create DefaultHoodieSplitAssigner for minimal configuration");
  }
}
