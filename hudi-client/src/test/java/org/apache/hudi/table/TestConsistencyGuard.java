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

package org.apache.hudi.table;

import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestConsistencyGuard extends HoodieClientTestHarness {

  @BeforeEach
  public void setup() {
    initPath();
    initFileSystemWithDefaultConfiguration();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupFileSystem();
  }

  @Test
  public void testCheckPassingAppearAndDisAppear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f2");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f3");

    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig(1, 1000, 1000));
    passing.waitTillFileAppears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
    passing.waitTillFileAppears(new Path(basePath + "/partition/path/f2_1-0-1_000.parquet"));
    passing.waitTillAllFilesAppear(basePath + "/partition/path", Arrays
        .asList(basePath + "/partition/path/f1_1-0-1_000.parquet", basePath + "/partition/path/f2_1-0-1_000.parquet"));

    fs.delete(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"), false);
    fs.delete(new Path(basePath + "/partition/path/f2_1-0-1_000.parquet"), false);
    passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
    passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f2_1-0-1_000.parquet"));
    passing.waitTillAllFilesDisappear(basePath + "/partition/path", Arrays
        .asList(basePath + "/partition/path/f1_1-0-1_000.parquet", basePath + "/partition/path/f2_1-0-1_000.parquet"));
  }

  @Test
  public void testCheckFailingAppear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillAllFilesAppear(basePath + "/partition/path", Arrays
          .asList(basePath + "/partition/path/f1_1-0-2_000.parquet", basePath + "/partition/path/f2_1-0-2_000.parquet"));
    });
  }

  @Test
  public void testCheckFailingAppears() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillFileAppears(new Path(basePath + "/partition/path/f1_1-0-2_000.parquet"));
    });
  }

  @Test
  public void testCheckFailingDisappear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillAllFilesDisappear(basePath + "/partition/path", Arrays
          .asList(basePath + "/partition/path/f1_1-0-1_000.parquet", basePath + "/partition/path/f2_1-0-2_000.parquet"));
    });
  }

  @Test
  public void testCheckFailingDisappears() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
    });
  }

  private ConsistencyGuardConfig getConsistencyGuardConfig() {
    return getConsistencyGuardConfig(3, 10, 10);
  }

  private ConsistencyGuardConfig getConsistencyGuardConfig(int maxChecks, int initalSleep, int maxSleep) {
    return ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
        .withInitialConsistencyCheckIntervalMs(initalSleep).withMaxConsistencyCheckIntervalMs(maxSleep)
        .withMaxConsistencyChecks(maxChecks).build();
  }
}
