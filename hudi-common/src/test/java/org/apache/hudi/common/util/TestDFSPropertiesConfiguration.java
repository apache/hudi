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

package org.apache.hudi.common.util;

import org.apache.hudi.common.minicluster.HdfsTestService;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests basic functionality of {@link DFSPropertiesConfiguration}.
 */
public class TestDFSPropertiesConfiguration {

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
    Path filePath = new Path(dfsBasePath + "/t1.props");
    writePropertiesFile(filePath, new String[] {"", "#comment", "abc", // to be ignored
        "int.prop=123", "double.prop=113.4", "string.prop=str", "boolean.prop=true", "long.prop=1354354354"});

    filePath = new Path(dfsBasePath + "/t2.props");
    writePropertiesFile(filePath, new String[] {"string.prop=ignored", "include=t1.props"});

    filePath = new Path(dfsBasePath + "/t3.props");
    writePropertiesFile(filePath,
        new String[] {"double.prop=838.3", "include = t2.props", "double.prop=243.4", "string.prop=t3.value"});

    filePath = new Path(dfsBasePath + "/t4.props");
    writePropertiesFile(filePath, new String[] {"double.prop=838.3", "include = t4.props"});
  }

  @AfterClass
  public static void cleanupClass() throws Exception {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  private static void writePropertiesFile(Path path, String[] lines) throws IOException {
    PrintStream out = new PrintStream(dfs.create(path, true));
    for (String line : lines) {
      out.println(line);
    }
    out.flush();
    out.close();
  }

  @Test
  public void testParsing() throws IOException {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs, new Path(dfsBasePath + "/t1.props"));
    TypedProperties props = cfg.getConfig();
    assertEquals(5, props.size());
    try {
      props.getString("invalid.key");
      fail("Should error out here.");
    } catch (IllegalArgumentException iae) {
      // ignore
    }

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(113.4, props.getDouble("double.prop"), 0.001);
    assertEquals(true, props.getBoolean("boolean.prop"));
    assertEquals("str", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));

    assertEquals(123, props.getInteger("int.prop", 456));
    assertEquals(113.4, props.getDouble("double.prop", 223.4), 0.001);
    assertEquals(true, props.getBoolean("boolean.prop", false));
    assertEquals("str", props.getString("string.prop", "default"));
    assertEquals(1354354354, props.getLong("long.prop", 8578494434L));

    assertEquals(456, props.getInteger("bad.int.prop", 456));
    assertEquals(223.4, props.getDouble("bad.double.prop", 223.4), 0.001);
    assertEquals(false, props.getBoolean("bad.boolean.prop", false));
    assertEquals("default", props.getString("bad.string.prop", "default"));
    assertEquals(8578494434L, props.getLong("bad.long.prop", 8578494434L));
  }

  @Test
  public void testIncludes() {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs, new Path(dfsBasePath + "/t3.props"));
    TypedProperties props = cfg.getConfig();

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(243.4, props.getDouble("double.prop"), 0.001);
    assertEquals(true, props.getBoolean("boolean.prop"));
    assertEquals("t3.value", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));

    try {
      new DFSPropertiesConfiguration(dfs, new Path(dfsBasePath + "/t4.props"));
      fail("Should error out on a self-included file.");
    } catch (IllegalStateException ise) {
      // ignore
    }
  }
}
