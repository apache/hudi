/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;

public abstract class HoodieFileGroupReaderOnJavaTestBase<T> extends TestHoodieFileGroupReaderBase<T> {

  @Override
  public String getBasePath() {
    return "file://" + tempDir.toAbsolutePath() + "/myTable";
  }

  @Override
  public String getCustomPayload() {
    return CustomPayloadForTesting.class.getName();
  }

  @Override
  public void commitToTable(List<HoodieRecord> recordList, String operation, boolean firstCommit, Map<String, String> writeConfigs, String schemaStr) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withEmbeddedTimelineServerEnabled(false)
        .withProps(writeConfigs)
        .withPath(getBasePath())
        .withSchema(schemaStr)
        .build();

    HoodieJavaClientTestHarness.TestJavaTaskContextSupplier taskContextSupplier = new HoodieJavaClientTestHarness.TestJavaTaskContextSupplier();
    HoodieJavaEngineContext context = new HoodieJavaEngineContext(getStorageConf(), taskContextSupplier);
    //init table if not exists
    StoragePath basePath = new StoragePath(getBasePath());
    try (HoodieStorage storage = new HoodieHadoopStorage(basePath, getStorageConf())) {
      boolean basepathExists = storage.exists(basePath);
      if (!basepathExists || firstCommit) {
        if (basepathExists) {
          storage.deleteDirectory(basePath);
        }
        Map<String, Object> initConfigs = new HashMap<>(writeConfigs);
        HoodieTableMetaClient.TableBuilder builder = HoodieTableMetaClient.newTableBuilder()
            .setTableType(writeConfigs.getOrDefault("hoodie.datasource.write.table.type", "MERGE_ON_READ"))
            .setTableName(writeConfigs.get("hoodie.table.name"))
            .setPartitionFields(writeConfigs.getOrDefault("hoodie.datasource.write.partitionpath.field", ""))
            .setRecordMergeMode(RecordMergeMode.getValue(writeConfigs.get("hoodie.record.merge.mode")))
            .setPopulateMetaFields(Boolean.parseBoolean(writeConfigs.getOrDefault(POPULATE_META_FIELDS.key(), "true")))
            .setKeyGeneratorType(KeyGeneratorType.SIMPLE.name())
            .setRecordKeyFields(writeConfigs.get(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key()))
            .setPartitionFields(writeConfigs.get(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()))
            .setOrderingFields(ConfigUtils.getOrderingFieldsStrDuringWrite(writeConfigs))
            .setBaseFileFormat(writeConfigs.get(HoodieTableConfig.BASE_FILE_FORMAT.key()))
            .set(initConfigs);
        if (writeConfigs.containsKey("hoodie.datasource.write.payload.class")) {
          builder = builder.setPayloadClassName(writeConfigs.get("hoodie.datasource.write.payload.class"));
        }
        builder.initTable(getStorageConf(), getBasePath());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    try (HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient(context, writeConfig)) {
      String instantTime = writeClient.startCommit();
      // Make a copy of the records for writing. The writer will clear out the data field.
      List<HoodieRecord> recordsCopy = new ArrayList<>(recordList.size());
      recordList.forEach(hoodieRecord -> recordsCopy.add(new HoodieAvroRecord<>(hoodieRecord.getKey(), (HoodieRecordPayload) hoodieRecord.getData())));
      if (operation.toLowerCase().equals("insert")) {
        writeClient.commit(instantTime, writeClient.insert(recordsCopy, instantTime), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
      } else if (operation.toLowerCase().equals("bulkInsert")) {
        writeClient.commit(instantTime, writeClient.bulkInsert(recordsCopy, instantTime), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
      } else {
        writeClient.commit(instantTime, writeClient.upsert(recordsCopy, instantTime), Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
      }
    }
  }
}
