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

package org.apache.hudi;

import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.util.ConsistencyGuard;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.FailSafeConsistencyGuard;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class TestConsistencyGuard extends HoodieClientTestHarness {

  @Before
  public void setup() {
    initPath();
    initFileSystemWithDefaultConfiguration();
  }

  @After
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

  @Test(expected = TimeoutException.class)
  public void testCheckFailingAppear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    passing.waitTillAllFilesAppear(basePath + "/partition/path", Arrays
        .asList(basePath + "/partition/path/f1_1-0-2_000.parquet", basePath + "/partition/path/f2_1-0-2_000.parquet"));
  }

  @Test(expected = TimeoutException.class)
  public void testCheckFailingAppears() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    passing.waitTillFileAppears(new Path(basePath + "/partition/path/f1_1-0-2_000.parquet"));
  }

  @Test(expected = TimeoutException.class)
  public void testCheckFailingDisappear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    passing.waitTillAllFilesDisappear(basePath + "/partition/path", Arrays
        .asList(basePath + "/partition/path/f1_1-0-1_000.parquet", basePath + "/partition/path/f2_1-0-2_000.parquet"));
  }

  @Test(expected = TimeoutException.class)
  public void testCheckFailingDisappears() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, getConsistencyGuardConfig());
    passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
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
