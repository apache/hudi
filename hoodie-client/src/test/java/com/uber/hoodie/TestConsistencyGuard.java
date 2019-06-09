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

package com.uber.hoodie;

import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.util.ConsistencyGuard;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.FailSafeConsistencyGuard;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestConsistencyGuard {
  private String basePath;
  protected transient FileSystem fs;

  @Before
  public void setup() throws IOException {
    TemporaryFolder testFolder = new TemporaryFolder();
    testFolder.create();
    basePath = testFolder.getRoot().getAbsolutePath();
    fs = FSUtils.getFs(basePath, new Configuration());
    if (fs instanceof LocalFileSystem) {
      LocalFileSystem lfs = (LocalFileSystem) fs;
      // With LocalFileSystem, with checksum disabled, fs.open() returns an inputStream which is FSInputStream
      // This causes ClassCastExceptions in LogRecordScanner (and potentially other places) calling fs.open
      // So, for the tests, we enforce checksum verification to circumvent the problem
      lfs.setVerifyChecksum(true);
    }
  }

  @Test
  public void testCheckPassingAppearAndDisAppear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f2");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f3");

    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, 1, 1000, 1000);
    passing.waitTillFileAppears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
    passing.waitTillFileAppears(new Path(basePath + "/partition/path/f2_1-0-1_000.parquet"));
    passing.waitTillAllFilesAppear(basePath + "/partition/path",
        Arrays.asList(basePath + "/partition/path/f1_1-0-1_000.parquet",
            basePath + "/partition/path/f2_1-0-1_000.parquet"));

    fs.delete(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"), false);
    fs.delete(new Path(basePath + "/partition/path/f2_1-0-1_000.parquet"), false);
    passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
    passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f2_1-0-1_000.parquet"));
    passing.waitTillAllFilesDisappear(basePath + "/partition/path",
        Arrays.asList(basePath + "/partition/path/f1_1-0-1_000.parquet",
            basePath + "/partition/path/f2_1-0-1_000.parquet"));
  }

  @Test(expected = TimeoutException.class)
  public void testCheckFailingAppear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, 3, 10, 10);
    passing.waitTillAllFilesAppear(basePath + "/partition/path",
        Arrays.asList(basePath + "/partition/path/f1_1-0-2_000.parquet",
            basePath + "/partition/path/f2_1-0-2_000.parquet"));
  }


  @Test(expected = TimeoutException.class)
  public void testCheckFailingAppears() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, 3, 10, 10);
    passing.waitTillFileAppears(new Path(basePath + "/partition/path/f1_1-0-2_000.parquet"));
  }

  @Test(expected = TimeoutException.class)
  public void testCheckFailingDisappear() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, 3, 10, 10);
    passing.waitTillAllFilesDisappear(basePath + "/partition/path",
        Arrays.asList(basePath + "/partition/path/f1_1-0-1_000.parquet",
            basePath + "/partition/path/f2_1-0-2_000.parquet"));
  }

  @Test(expected = TimeoutException.class)
  public void testCheckFailingDisappears() throws Exception {
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    HoodieClientTestUtils.fakeDataFile(basePath, "partition/path", "000", "f1");
    ConsistencyGuard passing = new FailSafeConsistencyGuard(fs, 3, 10, 10);
    passing.waitTillFileDisappears(new Path(basePath + "/partition/path/f1_1-0-1_000.parquet"));
  }
}
