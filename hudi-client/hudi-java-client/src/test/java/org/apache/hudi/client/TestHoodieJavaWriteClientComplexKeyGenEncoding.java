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

package org.apache.hudi.client;

import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Java write client receives records its caller keyed with the client's config, so it records a missing
 * {@code hoodie.table.complex.keygenerator.encoding} from the table's data when the write initializes the table, and
 * refuses a write whose config keys records differently from what the table stores.
 */
public class TestHoodieJavaWriteClientComplexKeyGenEncoding extends HoodieJavaClientTestHarness {

  @Test
  public void testRecordsMissingEncodingFromDataAndRefusesMismatchedWriter() throws Exception {
    Properties tableProps = new Properties();
    tableProps.setProperty(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    tableProps.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    tableProps.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, tableProps);
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    // a table holding bare record keys (as 0.14.1 to 1.0.2 stored them), written before the encoding was recorded:
    // the data generator keys records with the bare _row_key value
    String firstCommit = "001";
    List<HoodieRecord> inserts = dataGen.generateInserts(firstCommit, 20);
    try (HoodieJavaWriteClient client = getHoodieWriteClient(writeConfig(true))) {
      client.startCommitWithTime(firstCommit);
      client.insert(inserts, firstCommit);
    }
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), reloadEncoding(), "A never-written table records the writer's encoding");
    HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
        Collections.singleton(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    assertFalse(reloadEncoding().isPresent());

    // a writer configured with the default `<field>:<value>` keys would not match the stored keys: refused, while
    // the encoding the data carries gets recorded
    String secondCommit = "002";
    try (HoodieJavaWriteClient client = getHoodieWriteClient(writeConfig(false))) {
      HoodieException thrown = assertThrows(HoodieException.class, () -> {
        client.startCommitWithTime(secondCommit);
        client.upsert(dataGen.generateUpdates(secondCommit, inserts), secondCommit);
      });
      assertTrue(thrown.getMessage().contains("VALUE_ONLY"), thrown.getMessage());
    }
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), reloadEncoding());

    // a writer keyed the way the table stores its keys proceeds
    String thirdCommit = "003";
    try (HoodieJavaWriteClient client = getHoodieWriteClient(writeConfig(true))) {
      client.startCommitWithTime(thirdCommit);
      client.upsert(dataGen.generateUpdates(thirdCommit, inserts), thirdCommit);
    }
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), reloadEncoding());
  }

  private HoodieWriteConfig writeConfig(boolean bareKeys) {
    Properties props = new Properties();
    props.setProperty(HoodieWriteConfig.COMPLEX_KEYGEN_NEW_ENCODING.key(), String.valueOf(bareKeys));
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withProperties(props)
        .build();
  }

  private Option<ComplexKeyGenEncoding> reloadEncoding() {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return metaClient.getTableConfig().getComplexKeyGenEncoding();
  }
}
