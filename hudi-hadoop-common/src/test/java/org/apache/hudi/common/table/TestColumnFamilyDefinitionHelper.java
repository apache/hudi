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

import org.apache.hudi.common.model.ColumnFamilyDefinition;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.ColumnFamilyDefinitionHelper.COLUMN_FAMILIES_FILE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.COLUMN_FAMILY_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestColumnFamilyDefinitionHelper extends HoodieCommonTestHarness {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"cf_schema\",\"fields\":["
      + "{\"name\":\"id1\",\"type\":\"long\"},{\"name\":\"id2\",\"type\":\"string\"},{\"name\":\"col1\",\"type\":\"string\"},"
      + "{\"name\":\"col2\",\"type\":\"string\"},{\"name\":\"col3\",\"type\":\"string\"},{\"name\":\"col4\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"par1\",\"type\":\"string\"},{\"name\":\"par2\",\"type\":\"string\"}]}";

  private ColumnFamilyDefinitionHelper cfdHelper;

  @BeforeEach
  public void init() throws IOException {
    if (basePath == null) {
      initPath();
    }
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.CREATE_SCHEMA.key(), SCHEMA);
    properties.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), false);
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ, properties);
    cfdHelper = new ColumnFamilyDefinitionHelper(metaClient);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
    cfdHelper = null;
  }

  @Test
  public void testPersistColumnFamilyDefinitions() throws Exception {
    // prepare some config with column families
    Map<String, String> config = buildColumnFamilyConfigExample();

    // check defaults - no families
    Option<Map<String, ColumnFamilyDefinition>> cfdOpt = cfdHelper.fetchColumnFamilyDefinitions();
    assertTrue(cfdOpt.isEmpty());

    // failed to persist family without fully mentioning primary key
    config.put(COLUMN_FAMILY_PREFIX + "cf1", "col1, col2");
    Exception ex = assertThrows(HoodieValidationException.class, () -> cfdHelper.persistColumnFamilyDefinitions(config));
    assertEquals("Column family 'cf1' doesn't contain primary key [id1,id2]", ex.getMessage());
    config.put(COLUMN_FAMILY_PREFIX + "cf1", "id1,col1, col2");
    ex = assertThrows(HoodieValidationException.class, () -> cfdHelper.persistColumnFamilyDefinitions(config));
    assertEquals("Column family 'cf1' doesn't contain primary key [id1,id2]", ex.getMessage());

    // persist families and get them from file
    config.put(COLUMN_FAMILY_PREFIX + "cf1", "id1,id2,col1, col2");
    cfdHelper.persistColumnFamilyDefinitions(config);
    cfdOpt = cfdHelper.fetchColumnFamilyDefinitions();
    assertFalse(cfdOpt.isEmpty());
    assertColumnFamilyExample(cfdOpt.get());

    // failed to persist once more
    ex = assertThrows(IllegalStateException.class, () -> cfdHelper.persistColumnFamilyDefinitions(config));
    StoragePath cfdPath = new StoragePath(metaClient.getMetaPath(), COLUMN_FAMILIES_FILE_NAME);
    assertEquals("Column families are already defined in " + cfdPath, ex.getMessage());
  }

  @Test
  public void testUpdateColumnFamilyDefinitions() throws Exception {
    // prepare some config with column families
    Map<String, String> config = buildColumnFamilyConfigExample();
    // persist first version of families
    cfdHelper.persistColumnFamilyDefinitions(config);

    // add 1 column to existing family cf1, add new family cf3
    config.put(COLUMN_FAMILY_PREFIX + "cf1", "id1, id2, col1, col2, col5");
    config.put(COLUMN_FAMILY_PREFIX + "cf3", "id1, id2, col6, col7;col7");
    cfdHelper.updateColumnFamilyDefinitions(config);
    // check definitions were updated
    Option<Map<String, ColumnFamilyDefinition>> cfdOpt = cfdHelper.fetchColumnFamilyDefinitions();
    assertFalse(cfdOpt.isEmpty());
    Map<String, ColumnFamilyDefinition> cfDefinitions = cfdOpt.get();
    assertEquals(3, cfDefinitions.size());
    assertEquals(ColumnFamilyDefinition.fromConfig("cf1","id1,id2,col1,col2,col5"), cfDefinitions.get("cf1"));
    assertEquals(ColumnFamilyDefinition.fromConfig("cf3","id1,id2,col6,col7;col7"), cfDefinitions.get("cf3"));

    // delete previously added column and family
    config.put(COLUMN_FAMILY_PREFIX + "cf1", "id1, id2, col1, col2");
    config.put(COLUMN_FAMILY_PREFIX + "cf3", "");
    cfdHelper.updateColumnFamilyDefinitions(config);
    // check definitions were updated
    cfdOpt = cfdHelper.fetchColumnFamilyDefinitions();
    assertFalse(cfdOpt.isEmpty());
    assertColumnFamilyExample(cfdOpt.get());

    // try to delete family that is not exist
    Map<String, String> config1 = new HashMap<>();
    config1.put(COLUMN_FAMILY_PREFIX + "cf8", "");
    Exception ex = assertThrows(HoodieValidationException.class, () -> cfdHelper.updateColumnFamilyDefinitions(config1));
    assertEquals("Column family 'cf8' doesn't exist, unable to delete", ex.getMessage());
    // try to delete all families
    Map<String, String> config2 = new HashMap<>();
    config2.put(COLUMN_FAMILY_PREFIX + "cf1", "");
    config2.put(COLUMN_FAMILY_PREFIX + "cf2", "");
    ex = assertThrows(HoodieValidationException.class, () -> cfdHelper.updateColumnFamilyDefinitions(config2));
    assertEquals("Removing all column families is not allowed", ex.getMessage());
  }

}
