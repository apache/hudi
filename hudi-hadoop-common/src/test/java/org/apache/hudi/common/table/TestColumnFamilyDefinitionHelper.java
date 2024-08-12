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

import org.apache.avro.Schema;
import org.apache.hudi.common.model.ColumnFamilyDefinition;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.ColumnFamilyDefinitionHelper.COLUMN_FAMILIES_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestColumnFamilyDefinitionHelper extends HoodieCommonTestHarness {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"cf_schema\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"col1\",\"type\":\"string\"},{\"name\":\"col2\",\"type\":\"string\"},"
      + "{\"name\":\"col3\",\"type\":\"string\"},{\"name\":\"col4\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"dt\",\"type\":\"string\"}]}";

  private ColumnFamilyDefinitionHelper cfdHelper;

  @BeforeEach
  public void init() throws IOException {
    if (basePath == null) {
      initPath();
    }
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.CREATE_SCHEMA.key(), SCHEMA);
    metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ, properties);
    cfdHelper = new ColumnFamilyDefinitionHelper(metaClient.getStorage(), metaClient.getMetaPath());
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
    Option<Map<String, ColumnFamilyDefinition>> cfdOpt = cfdHelper.getColumnFamilyDefinitions();
    assertTrue(cfdOpt.isEmpty());

    Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    // persist families and get them from file
    cfdHelper.persistColumnFamilyDefinitions(schema, config);
    cfdOpt = cfdHelper.getColumnFamilyDefinitions();
    assertFalse(cfdOpt.isEmpty());
    assertColumnFamilyExample(cfdOpt.get());

    // failed to persist once more
    Exception ex = assertThrows(IllegalStateException.class, () -> cfdHelper.persistColumnFamilyDefinitions(schema, config));
    StoragePath cfdPath = new StoragePath(metaClient.getMetaPath(), COLUMN_FAMILIES_FILE_NAME);
    assertEquals("Column families are already defined in " + cfdPath, ex.getMessage());
  }

  @Test
  public void testUpdateColumnFamilyDefinitions() throws Exception {
    // prepare some config with column families
    Map<String, String> config = buildColumnFamilyConfigExample();
    Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    // persist first version of families
    cfdHelper.persistColumnFamilyDefinitions(schema, config);

    schema = null; // new TableSchemaResolver(metaClient).getTableAvroSchema();
    // add 1 column to existing family cf1, add new family cf3
    config.put("hoodie.columnFamily.cf1", "id, col1, col2, col5");
    config.put("hoodie.columnFamily.cf3", "id, col6, col7;col7");
    cfdHelper.updateColumnFamilyDefinitions(schema, config);
    // check definitions were updated
    Option<Map<String, ColumnFamilyDefinition>> cfdOpt = cfdHelper.getColumnFamilyDefinitions();
    assertFalse(cfdOpt.isEmpty());
    Map<String, ColumnFamilyDefinition> cfDefinitions = cfdOpt.get();
    assertEquals(3, cfDefinitions.size());
    assertEquals(ColumnFamilyDefinition.fromConfig("cf1","id,col1,col2,col5"), cfDefinitions.get("cf1"));
    assertEquals(ColumnFamilyDefinition.fromConfig("cf3","id,col6,col7;col7"), cfDefinitions.get("cf3"));

    schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    // delete previously added column and family
    config.put("hoodie.columnFamily.cf1", "id, col1, col2");
    config.put("hoodie.columnFamily.cf3", "");
    cfdHelper.updateColumnFamilyDefinitions(schema, config);
    // check definitions were updated
    cfdOpt = cfdHelper.getColumnFamilyDefinitions();
    assertFalse(cfdOpt.isEmpty());
    assertColumnFamilyExample(cfdOpt.get());
  }

}
