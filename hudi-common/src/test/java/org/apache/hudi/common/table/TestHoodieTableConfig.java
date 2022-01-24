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

package org.apache.hudi.common.table;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieTableConfig extends HoodieCommonTestHarness {

  private FileSystem fs;
  private Path metaPath;
  private Path cfgPath;
  private Path backupCfgPath;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    fs = new Path(basePath).getFileSystem(new Configuration());
    metaPath = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.NAME.key(), "test-table");
    HoodieTableConfig.create(fs, metaPath, props);
    cfgPath = new Path(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    backupCfgPath = new Path(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP);
  }

  @Test
  public void testCreate() throws IOException {
    assertTrue(fs.exists(new Path(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE)));
    HoodieTableConfig config = new HoodieTableConfig(fs, metaPath.toString(), null);
    assertEquals(4, config.getProps().size());
  }

  @Test
  public void testUpdate() throws IOException {
    Properties updatedProps = new Properties();
    updatedProps.setProperty(HoodieTableConfig.NAME.key(), "test-table2");
    updatedProps.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key(), "new_field");
    HoodieTableConfig.update(fs, metaPath, updatedProps);

    assertTrue(fs.exists(cfgPath));
    assertFalse(fs.exists(backupCfgPath));
    HoodieTableConfig config = new HoodieTableConfig(fs, metaPath.toString(), null);
    assertEquals(5, config.getProps().size());
    assertEquals("test-table2", config.getTableName());
    assertEquals("new_field", config.getPreCombineField());
  }

  @Test
  public void testDelete() throws IOException {
    Set<String> deletedProps = CollectionUtils.createSet(HoodieTableConfig.ARCHIVELOG_FOLDER.key(), "hoodie.invalid.config");
    HoodieTableConfig.delete(fs, metaPath, deletedProps);

    assertTrue(fs.exists(cfgPath));
    assertFalse(fs.exists(backupCfgPath));
    HoodieTableConfig config = new HoodieTableConfig(fs, metaPath.toString(), null);
    assertEquals(3, config.getProps().size());
    assertNull(config.getProps().getProperty("hoodie.invalid.config"));
    assertFalse(config.getProps().contains(HoodieTableConfig.ARCHIVELOG_FOLDER.key()));
  }

  @Test
  public void testReadsWhenPropsFileDoesNotExist() throws IOException {
    fs.delete(cfgPath, false);
    assertThrows(HoodieIOException.class, () -> {
      new HoodieTableConfig(fs, metaPath.toString(), null);
    });
  }

  @Test
  public void testReadsWithUpdateFailures() throws IOException {
    HoodieTableConfig config = new HoodieTableConfig(fs, metaPath.toString(), null);
    fs.delete(cfgPath, false);
    try (FSDataOutputStream out = fs.create(backupCfgPath)) {
      config.getProps().store(out, "");
    }

    assertFalse(fs.exists(cfgPath));
    assertTrue(fs.exists(backupCfgPath));
    config = new HoodieTableConfig(fs, metaPath.toString(), null);
    assertEquals(4, config.getProps().size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testUpdateRecovery(boolean shouldPropsFileExist) throws IOException {
    HoodieTableConfig config = new HoodieTableConfig(fs, metaPath.toString(), null);
    if (!shouldPropsFileExist) {
      fs.delete(cfgPath, false);
    }
    try (FSDataOutputStream out = fs.create(backupCfgPath)) {
      config.getProps().store(out, "");
    }

    HoodieTableConfig.recoverIfNeeded(fs, cfgPath, backupCfgPath);
    assertTrue(fs.exists(cfgPath));
    assertFalse(fs.exists(backupCfgPath));
    config = new HoodieTableConfig(fs, metaPath.toString(), null);
    assertEquals(4, config.getProps().size());
  }
}
