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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.utilities.config.HiveIncrPullSourceConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHiveIncrPullSource extends HoodieSparkClientTestHarness {

  private TypedProperties props;
  private String incrPullRoot;

  @BeforeEach
  void setUp() throws Exception {
    initSparkContexts();
    initPath();
    initHoodieStorage();
    incrPullRoot = basePath + "/incrPullRoot";
    FileSystem fs = (FileSystem) storage.getFileSystem();
    fs.mkdirs(new Path(incrPullRoot));
    props = new TypedProperties();
    props.setProperty(HiveIncrPullSourceConfig.ROOT_INPUT_PATH.key(), incrPullRoot);
  }

  @AfterEach
  void teardown() throws Exception {
    cleanupResources();
  }

  private void createCommitDir(String commitTime) throws Exception {
    FileSystem fs = (FileSystem) storage.getFileSystem();
    fs.mkdirs(new Path(incrPullRoot, commitTime));
  }

  @Test
  void findCommitToPullReturnsV1CheckpointForFirstCommitWhenNoLatestTargetCommit() throws Exception {
    createCommitDir("20240101");
    createCommitDir("20240102");
    HiveIncrPullSource source = new HiveIncrPullSource(props, jsc, sparkSession, null);
    Method findCommitToPull = HiveIncrPullSource.class.getDeclaredMethod("findCommitToPull", Option.class);
    findCommitToPull.setAccessible(true);

    @SuppressWarnings("unchecked")
    Option<Checkpoint> result = (Option<Checkpoint>) findCommitToPull.invoke(source, Option.empty());

    assertTrue(result.isPresent());
    assertInstanceOf(StreamerCheckpointV1.class, result.get());
    assertEquals("20240101", result.get().getCheckpointKey());
  }

  @Test
  void readFromCheckpointRewrapsV2LastCheckpointAsV1WhenNoCommitToPull() throws Exception {
    createCommitDir("20240101");
    createCommitDir("20240102");
    HiveIncrPullSource source = new HiveIncrPullSource(props, jsc, sparkSession, null);

    // lastCheckpoint past the last commit so findCommitToPull returns empty and the early return hits.
    InputBatch<?> batch = source.readFromCheckpoint(
        Option.of(new StreamerCheckpointV2("20240103")), Long.MAX_VALUE);

    assertFalse(batch.getBatch().isPresent());
    assertInstanceOf(StreamerCheckpointV1.class, batch.getCheckpointForNextBatch());
    assertEquals("20240103", batch.getCheckpointForNextBatch().getCheckpointKey());
  }
}
