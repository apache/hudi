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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.minicluster.HdfsTestService;
import org.apache.hudi.utilities.model.TableConfig;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestDFSTablePropertiesConfiguration {

  private static String dfsBasePath;
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  @BeforeClass
  public static void initClass() throws Exception {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);

    // Create a temp folder as the base path
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));

    // create some files.
    Path filePath = new Path(dfsBasePath + "/test_props.json");
    writePropertiesFile(filePath, new String[] {"", "[", "{",
        "database:dummy_db", "table_name:dummy_table", "}", "]"});

    filePath = new Path(dfsBasePath + "/invalid_props1.json");
    writePropertiesFile(filePath, new String[] {"database:dummy", "}"});

    filePath = new Path(dfsBasePath + "/invalid_props2.json");
    writePropertiesFile(filePath,
        new String[] {"[", "{", "database:dummy", "}", "}", "]"});
  }

  private static void writePropertiesFile(Path path, String[] lines) throws IOException {
    PrintStream out = new PrintStream(dfs.create(path, true));
    for (String line : lines) {
      out.println(line);
    }
    out.flush();
    out.close();
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  @Test
  public void testValidFileParsing() {
    DFSTablePropertiesConfiguration configuration = new DFSTablePropertiesConfiguration(dfs, new Path(dfsBasePath + "/test_props.json"));
    List<TableConfig> configList = configuration.getConfigs();
    assertEquals(1, configList.size());
    assertEquals(configList.get(0).getDatabase(), "dummy_db");
  }

  @Test
  public void testInvalidFileParsing() {
    try {
      new DFSTablePropertiesConfiguration(dfs, new Path(dfsBasePath + "/invalid_props1.json"));
      fail("Properties cannot be provided without opening braces");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Custom props file is not formatted properly!"));
    }
  }

  @Test
  public void testImproperFormatFile() {
    try {
      new DFSTablePropertiesConfiguration(dfs, new Path(dfsBasePath + "/invalid_props2.json"));
      fail("Should fail due to improper formatting of file");
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains("Custom props file is not formatted properly!"));
    }
  }
}
