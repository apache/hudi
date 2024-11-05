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

import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.fs.OptimisticConsistencyGuard;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests {@link ConsistencyGuard}s.
 */
public class TestConsistencyGuard extends HoodieSparkClientTestHarness {

  private static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();

  // multiple parameters, uses Collection<Object[]>
  public static List<Arguments> consistencyGuardType() {
    return Arrays.asList(
        Arguments.of(FailSafeConsistencyGuard.class.getName()),
        Arguments.of(OptimisticConsistencyGuard.class.getName())
    );
  }

  @BeforeEach
  public void setup() {
    initPath();
    initFileSystemWithDefaultConfiguration();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @MethodSource("consistencyGuardType")
  public void testCheckPassingAppearAndDisAppear(String consistencyGuardType) throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f2");
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f3");

    ConsistencyGuardConfig config = getConsistencyGuardConfig(1, 1000, 1000);
    ConsistencyGuard passing = consistencyGuardType.equals(FailSafeConsistencyGuard.class.getName())
        ? new FailSafeConsistencyGuard(storage, config) :
        new OptimisticConsistencyGuard(storage, config);
    passing.waitTillFileAppears(
        new StoragePath(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION));
    passing.waitTillFileAppears(
        new StoragePath(basePath + "/partition/path/f2_1-0-1_000" + BASE_FILE_EXTENSION));
    passing.waitTillAllFilesAppear(basePath + "/partition/path", Arrays
        .asList(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION,
            basePath + "/partition/path/f2_1-0-1_000" + BASE_FILE_EXTENSION));

    storage.deleteFile(new StoragePath(
        basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION));
    storage.deleteFile(new StoragePath(
        basePath + "/partition/path/f2_1-0-1_000" + BASE_FILE_EXTENSION));
    passing.waitTillFileDisappears(
        new StoragePath(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION));
    passing.waitTillFileDisappears(
        new StoragePath(basePath + "/partition/path/f2_1-0-1_000" + BASE_FILE_EXTENSION));
    passing.waitTillAllFilesDisappear(basePath + "/partition/path", Arrays
        .asList(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION,
            basePath + "/partition/path/f2_1-0-1_000" + BASE_FILE_EXTENSION));
  }

  @Test
  public void testCheckFailingAppearFailSafe() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(storage, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillAllFilesAppear(basePath + "/partition/path", Arrays
          .asList(basePath + "/partition/path/f1_1-0-2_000" + BASE_FILE_EXTENSION,
              basePath + "/partition/path/f2_1-0-2_000" + BASE_FILE_EXTENSION));
    });
  }

  @Test
  public void testCheckFailingAppearTimedWait() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new OptimisticConsistencyGuard(storage, getConsistencyGuardConfig());
    passing.waitTillAllFilesAppear(basePath + "/partition/path", Arrays
          .asList(basePath + "/partition/path/f1_1-0-2_000" + BASE_FILE_EXTENSION,
              basePath + "/partition/path/f2_1-0-2_000" + BASE_FILE_EXTENSION));
  }

  @Test
  public void testCheckFailingAppearsFailSafe() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(storage, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillFileAppears(
          new StoragePath(basePath + "/partition/path/f1_1-0-2_000" + BASE_FILE_EXTENSION));
    });
  }

  @Test
  public void testCheckFailingAppearsTimedWait() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new OptimisticConsistencyGuard(storage, getConsistencyGuardConfig());
    passing.waitTillFileAppears(
        new StoragePath(basePath + "/partition/path/f1_1-0-2_000" + BASE_FILE_EXTENSION));
  }

  @Test
  public void testCheckFailingDisappearFailSafe() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(storage, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillAllFilesDisappear(basePath + "/partition/path", Arrays
          .asList(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION,
              basePath + "/partition/path/f2_1-0-2_000" + BASE_FILE_EXTENSION));
    });
  }

  @Test
  public void testCheckFailingDisappearTimedWait() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new OptimisticConsistencyGuard(storage, getConsistencyGuardConfig());
    passing.waitTillAllFilesDisappear(basePath + "/partition/path", Arrays
          .asList(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION,
              basePath + "/partition/path/f2_1-0-2_000" + BASE_FILE_EXTENSION));
  }

  @Test
  public void testCheckFailingDisappearsFailSafe() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(storage, getConsistencyGuardConfig());
    assertThrows(TimeoutException.class, () -> {
      passing.waitTillFileDisappears(
          new StoragePath(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION));
    });
  }

  @Test
  public void testCheckFailingDisappearsTimedWait() throws Exception {
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    FileCreateUtils.createBaseFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new OptimisticConsistencyGuard(storage, getConsistencyGuardConfig());
    passing.waitTillFileDisappears(
        new StoragePath(basePath + "/partition/path/f1_1-0-1_000" + BASE_FILE_EXTENSION));
  }

  private ConsistencyGuardConfig getConsistencyGuardConfig() {
    return getConsistencyGuardConfig(3, 10, 10);
  }

  private ConsistencyGuardConfig getConsistencyGuardConfig(int maxChecks, int initialSleep, int maxSleep) {
    return ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
        .withInitialConsistencyCheckIntervalMs(initialSleep).withMaxConsistencyCheckIntervalMs(maxSleep)
        .withMaxConsistencyChecks(maxChecks).build();
  }
}
