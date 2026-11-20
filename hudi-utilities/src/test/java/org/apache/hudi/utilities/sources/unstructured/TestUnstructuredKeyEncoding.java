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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestUnstructuredKeyEncoding {
  private static final String VERSION = "hoodie.table.unstructured.key.encoding.version";

  @TempDir
  Path temp;

  private static TypedProperties job(String mode) {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), mode);
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    if ("embedding".equals(mode)) {
      props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    }
    return props;
  }

  @ParameterizedTest
  @ValueSource(strings = {"blob", "embedding"})
  void encodingPinSurvivesVersionNineTableCreation(String mode) throws Exception {
    UnstructuredSourceContract contract = new UnstructuredSourceContract(job(mode));
    String base = temp.resolve(mode).toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableVersion(9).setTableName(mode).setTableType(HoodieTableType.COPY_ON_WRITE)
        .setRecordKeyFields(String.join(",", contract.mode().keyColumns())).setAdditionalProperties(contract.tableProperties())
        .initTable(HoodieTestUtils.getDefaultStorageConf(), base);
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.builder().setConf(HoodieTestUtils.getDefaultStorageConf())
        .setBasePath(base).build();
    assertEquals("1", reloaded.getTableConfig().getProps().getProperty(VERSION));
    contract.checkTable(base, reloaded.getTableConfig());
  }

  @ParameterizedTest
  @ValueSource(strings = {"blob", "embedding"})
  void matchingEncodingPermitsStart(String mode) {
    UnstructuredSourceContract contract = new UnstructuredSourceContract(job(mode));
    HoodieTableConfig table = table(contract);
    table.getProps().setProperty(VERSION, "1");
    contract.checkTable("file:/tables/" + mode, table);
  }

  @ParameterizedTest
  @CsvSource({"blob,missing", "blob,2", "embedding,missing", "embedding,2"})
  void missingOrDifferentEncodingRefusesStart(String mode, String version) {
    UnstructuredSourceContract contract = new UnstructuredSourceContract(job(mode));
    HoodieTableConfig table = table(contract);
    table.getProps().remove(VERSION);
    if (!"missing".equals(version)) {
      table.getProps().setProperty(VERSION, version);
    }
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> contract.checkTable("file:/tables/" + mode, table));
    assertTrue(error.getMessage().contains(VERSION + "=1"), error.getMessage());
    assertTrue(error.getMessage().contains("stores " + ("missing".equals(version) ? "none" : version)), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"blob", "embedding"})
  void jobCannotOverrideTheBuildsEncoding(String mode) {
    TypedProperties props = job(mode);
    props.setProperty(VERSION, "2");
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> new UnstructuredSourceContract(props).prepareJob(props));
    assertTrue(error.getMessage().contains(VERSION), error.getMessage());
    assertEquals("2", props.getProperty(VERSION));
  }

  private static HoodieTableConfig table(UnstructuredSourceContract contract) {
    HoodieTableConfig table = new HoodieTableConfig();
    table.getProps().putAll(contract.tableProperties());
    table.setValue(HoodieTableConfig.RECORDKEY_FIELDS, String.join(",", contract.mode().keyColumns()));
    return table;
  }
}
