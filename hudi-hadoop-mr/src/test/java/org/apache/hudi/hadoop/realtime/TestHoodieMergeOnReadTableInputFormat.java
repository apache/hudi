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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieMergeOnReadTableInputFormat {

  @TempDir
  java.nio.file.Path tempDir;
  private HoodieStorage storage;
  private FileSystem fs;

  @BeforeEach
  void setUp() throws IOException {
    fs = FileSystem.get(tempDir.toUri(), new Configuration());
    storage = HoodieStorageUtils.getStorage(HadoopFSUtils.getStorageConf(fs.getConf()), new Class<?>[] {FileSystem.class}, fs);
  }

  @AfterEach
  void tearDown() throws IOException {
    fs.close();
  }

  @Test
  void pathNotSplitableForBootstrapScenario() throws IOException {
    URI source = Files.createTempFile(tempDir, "source", ".parquet").toUri();
    URI target = Files.createTempFile(tempDir, "target", ".parquet").toUri();
    HoodieRealtimePath rtPath = new HoodieRealtimePath(new Path("foo"), "bar", target.toString(), Collections.emptyList(), "000", false, Option.empty());
    assertTrue(new HoodieMergeOnReadTableInputFormat().isSplitable(fs, rtPath));

    PathWithBootstrapFileStatus path = new PathWithBootstrapFileStatus(new Path(target), fs.getFileStatus(new Path(source)));
    rtPath.setPathWithBootstrapFileStatus(path);
    assertFalse(new HoodieMergeOnReadTableInputFormat().isSplitable(fs, rtPath), "Path for bootstrap should not be splitable.");
  }

  @Test
  void pathNotSplitableIfContainsDeltaFiles() throws IOException {
    URI basePath = Files.createTempFile(tempDir, "target", ".parquet").toUri();
    HoodieRealtimePath rtPath = new HoodieRealtimePath(new Path("foo"), "bar", basePath.toString(), Collections.emptyList(), "000", false, Option.empty());
    assertTrue(new HoodieMergeOnReadTableInputFormat().isSplitable(fs, rtPath), "Path only contains the base file should be splittable");

    URI logPath = Files.createTempFile(tempDir, ".test", ".log.4_1-149-180").toUri();
    HoodieLogFile logFile = new HoodieLogFile(storage.getPathInfo(new StoragePath(logPath)));
    rtPath = new HoodieRealtimePath(new Path("foo"), "bar", basePath.toString(), Collections.singletonList(logFile), "000", false, Option.empty());
    assertFalse(new HoodieMergeOnReadTableInputFormat().isSplitable(fs, rtPath), "Path contains log files should not be splittable.");
  }
}
