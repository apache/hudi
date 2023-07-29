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

package org.apache.hudi.common.fs;

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.testutils.HoodieTestUtils.shouldUseExternalHdfs;
import static org.apache.hudi.common.testutils.HoodieTestUtils.useExternalHdfs;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieWrapperFileSystem {
  private static String basePath;
  private static FileSystem fs;
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;

  @BeforeAll
  public static void setUp() throws IOException {
    if (shouldUseExternalHdfs()) {
      fs = useExternalHdfs();
    } else {
      hdfsTestService = new HdfsTestService(HoodieTestUtils.getDefaultHadoopConf());
      dfsCluster = hdfsTestService.start(true);
      fs = dfsCluster.getFileSystem();
    }
    basePath = fs.getWorkingDirectory() + "/TestHoodieWrapperFileSystem/";
    fs.mkdirs(new Path(basePath));
  }

  @AfterAll
  public static void cleanUp() {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  @Test
  public void testCreateImmutableFileInPath() throws IOException {
    HoodieWrapperFileSystem fs = new HoodieWrapperFileSystem(FSUtils.getFs(basePath, new Configuration()), new NoOpConsistencyGuard());
    String testContent = "test content";
    Path testFile = new Path(basePath + Path.SEPARATOR + "clean.00000001");

    // create same commit twice
    fs.createImmutableFileInPath(testFile, Option.of(testContent.getBytes()));
    fs.createImmutableFileInPath(testFile, Option.of(testContent.getBytes()));

    assertEquals(1, fs.listStatus(new Path(basePath)).length,
        "create same file twice should only have one file exists, files: " + fs.listStatus(new Path(basePath)));
  }
}
