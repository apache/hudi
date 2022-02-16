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

import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests basic functionality of {@link DFSPropertiesConfiguration}.
 */
public class TestDFSPropertiesConfiguration {

  private static String dfsBasePath;
  private static HdfsTestService hdfsTestService;
  private static MiniDFSCluster dfsCluster;
  private static DistributedFileSystem dfs;

  @Rule
  public static final EnvironmentVariables ENVIRONMENT_VARIABLES
      = new EnvironmentVariables();

  @BeforeAll
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

  @AfterAll
  public static void cleanupClass() {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
  }

  @AfterEach
  public void cleanupGlobalConfig() {
    DFSPropertiesConfiguration.clearGlobalProps();
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
  public void testParsing() {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs.getConf(), new Path(dfsBasePath + "/t1.props"));
    TypedProperties props = cfg.getProps();
    assertEquals(5, props.size());
    assertThrows(IllegalArgumentException.class, () -> {
      props.getString("invalid.key");
    }, "Should error out here.");

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(113.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("str", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));

    assertEquals(123, props.getInteger("int.prop", 456));
    assertEquals(113.4, props.getDouble("double.prop", 223.4), 0.001);
    assertTrue(props.getBoolean("boolean.prop", false));
    assertEquals("str", props.getString("string.prop", "default"));
    assertEquals(1354354354, props.getLong("long.prop", 8578494434L));

    assertEquals(456, props.getInteger("bad.int.prop", 456));
    assertEquals(223.4, props.getDouble("bad.double.prop", 223.4), 0.001);
    assertFalse(props.getBoolean("bad.boolean.prop", false));
    assertEquals("default", props.getString("bad.string.prop", "default"));
    assertEquals(8578494434L, props.getLong("bad.long.prop", 8578494434L));
  }

  @Test
  public void testIncludes() {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs.getConf(), new Path(dfsBasePath + "/t3.props"));
    TypedProperties props = cfg.getProps();

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(243.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("t3.value", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));
    assertThrows(IllegalStateException.class, () -> {
      cfg.addPropsFromFile(new Path(dfsBasePath + "/t4.props"));
    }, "Should error out on a self-included file.");
  }

  @Test
  public void testLocalFileSystemLoading() throws IOException {
    DFSPropertiesConfiguration cfg = new DFSPropertiesConfiguration(dfs.getConf(), new Path(dfsBasePath + "/t1.props"));

    cfg.addPropsFromFile(
        new Path(
            String.format(
                "file:%s",
                getClass().getClassLoader()
                    .getResource("props/test.properties")
                    .getPath()
            )
        ));

    TypedProperties props = cfg.getProps();

    assertEquals(123, props.getInteger("int.prop"));
    assertEquals(113.4, props.getDouble("double.prop"), 0.001);
    assertTrue(props.getBoolean("boolean.prop"));
    assertEquals("str", props.getString("string.prop"));
    assertEquals(1354354354, props.getLong("long.prop"));
    assertEquals(123, props.getInteger("some.random.prop"));
  }

  @Test
  public void testNoGlobalConfFileConfigured() {
    ENVIRONMENT_VARIABLES.clear(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME);
    // Should not throw any exception when no external configuration file configured
    DFSPropertiesConfiguration.refreshGlobalProps();
    assertEquals(0, DFSPropertiesConfiguration.getGlobalProps().size());
  }

  @Test
  public void testLoadGlobalConfFile() {
    // set HUDI_CONF_DIR
    String testPropsFilePath = new File("src/test/resources/external-config").getAbsolutePath();
    ENVIRONMENT_VARIABLES.set(DFSPropertiesConfiguration.CONF_FILE_DIR_ENV_NAME, testPropsFilePath);

    DFSPropertiesConfiguration.refreshGlobalProps();
    assertEquals(5, DFSPropertiesConfiguration.getGlobalProps().size());
    assertEquals("jdbc:hive2://localhost:10000", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.datasource.hive_sync.jdbcurl"));
    assertEquals("true", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.datasource.hive_sync.use_jdbc"));
    assertEquals("false", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.datasource.hive_sync.support_timestamp"));
    assertEquals("BLOOM", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.index.type"));
    assertEquals("true", DFSPropertiesConfiguration.getGlobalProps().get("hoodie.metadata.enable"));
  }
}
