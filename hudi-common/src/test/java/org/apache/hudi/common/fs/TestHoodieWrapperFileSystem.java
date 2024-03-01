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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.testutils.HoodieTestUtils.shouldUseExternalHdfs;
import static org.apache.hudi.common.testutils.HoodieTestUtils.useExternalHdfs;

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
    /*
    HoodieStorage storage = new HoodieHadoopStorage(
        HadoopFSUtils.getFs(basePath, new Configuration())) {
      @Override
      public boolean needCreateTempFile() {
        return true;
      }
    };
    String testContent = "test content";
    StoragePath testFile = new StoragePath(basePath + StoragePath.SEPARATOR + "clean.00000001");

    // create same commit twice
    storage.createImmutableFileInPath(testFile, Option.of(getUTF8Bytes(testContent)));
    storage.createImmutableFileInPath(testFile, Option.of(getUTF8Bytes(testContent)));

    assertEquals(1, storage.listDirectEntries(new HoodieLocation(basePath)).size(),
        "create same file twice should only have one file exists, files: "
            + storage.listDirectEntries(new HoodieLocation(basePath)).toString());
     */
  }
}
