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
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.ComplexKeyGenEncoding;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Java write client receives records its caller has already keyed, so it has no ingestion setup that could
 * record {@code hoodie.table.complex.keygenerator.encoding} ahead of keying; it records a missing encoding from the
 * table's data when the write initializes the table instead of refusing the write.
 */
public class TestHoodieJavaWriteClientComplexKeyGenEncoding extends HoodieJavaClientTestHarness {

  private static final String RECORD_KEY_PREFIX = "_row_key:";

  @Test
  public void testWriteRecordsMissingEncodingFromData() throws Exception {
    // a version 8 table written by a release that stored bare record keys, before the encoding was recorded
    Properties tableProps = new Properties();
    tableProps.setProperty(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), ComplexAvroKeyGenerator.class.getName());
    tableProps.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
    tableProps.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    tableProps.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    tableProps.setProperty(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key(), ComplexKeyGenEncoding.VALUE_ONLY.name());
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, tableProps);
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig().getTableVersion());

    HoodieWriteConfig legacyConfig = writeConfigBuilder()
        .withWriteTableVersion(HoodieTableVersion.EIGHT.versionCode()).build();
    // the data generator keys records with the bare _row_key value, the way 0.14.1 to 1.0.2 stored them
    List<HoodieRecord> inserts = dataGen.generateInserts(makeNewCommitTime(1, "%09d"), 20);
    assertTrue(inserts.stream().noneMatch(r -> r.getRecordKey().startsWith(RECORD_KEY_PREFIX)));
    try (HoodieJavaWriteClient client = getHoodieWriteClient(legacyConfig)) {
      String commitTime = makeNewCommitTime(1, "%09d");
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      client.commit(commitTime, client.insert(inserts, commitTime));
    }
    removeEncodingProperty();
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig().getTableVersion());

    // the upgrading write: the upgrade records the encoding found in the data before any hop runs
    upsert(inserts, 2);
    assertEquals(HoodieTableVersion.current(), metaClient.getTableConfig().getTableVersion());
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), metaClient.getTableConfig().getComplexKeyGenEncoding());
    assertStoredKeysBareAndUnique(inserts.size());

    // a table already at the current version without the property: the client records it on table initialization
    removeEncodingProperty();
    upsert(inserts, 3);
    assertEquals(Option.of(ComplexKeyGenEncoding.VALUE_ONLY), metaClient.getTableConfig().getComplexKeyGenEncoding());
    assertStoredKeysBareAndUnique(inserts.size());
  }

  private HoodieWriteConfig.Builder writeConfigBuilder() {
    return HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withPath(basePath)
        .withSchema(AVRO_SCHEMA.toString());
  }

  private void upsert(List<HoodieRecord> inserts, int commitSeq) throws Exception {
    String commitTime = makeNewCommitTime(commitSeq, "%09d");
    List<HoodieRecord> updates = dataGen.generateUpdates(commitTime, inserts);
    try (HoodieJavaWriteClient client = getHoodieWriteClient(writeConfigBuilder().build())) {
      WriteClientTestUtils.startCommitWithTime(client, commitTime);
      client.commit(commitTime, client.upsert(updates, commitTime));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
  }

  private void removeEncodingProperty() {
    HoodieTableConfig.delete(metaClient.getStorage(), metaClient.getMetaPath(),
        Collections.singleton(HoodieTableConfig.COMPLEX_KEYGEN_ENCODING.key()));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertFalse(metaClient.getTableConfig().getComplexKeyGenEncoding().isPresent());
  }

  /** Every record key stored in the latest base files is bare, and no key was duplicated by an update. */
  private void assertStoredKeysBareAndUnique(int expectedRecords) throws Exception {
    FileFormatUtils fileUtils = getFileUtilsInstance(metaClient);
    List<String> storedKeys = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .lastInstant().map(instant -> {
          try {
            List<String> keys = new ArrayList<>();
            for (StoragePathInfo pathInfo : storage.listFiles(new StoragePath(basePath))) {
              String name = pathInfo.getPath().getName();
              if (name.endsWith(".parquet") && name.contains(instant.requestedTime())
                  && !pathInfo.getPath().toString().contains("/.hoodie/")) {
                for (GenericRecord record : fileUtils.readAvroRecords(storage, pathInfo.getPath())) {
                  keys.add(record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString());
                }
              }
            }
            return keys;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }).orElse(Collections.emptyList());
    assertEquals(expectedRecords, storedKeys.size(), "Every record must be updated in place: " + storedKeys);
    assertEquals(expectedRecords, storedKeys.stream().distinct().collect(Collectors.toList()).size());
    assertTrue(storedKeys.stream().noneMatch(k -> k.startsWith(RECORD_KEY_PREFIX)), "Keys must stay bare: " + storedKeys);
  }
}
