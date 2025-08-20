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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.internal.schema.utils.SerDeHelper;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.avro.AvroSchemaUtils.getAvroRecordQualifiedName;
import static org.apache.hudi.common.table.HoodieTableConfig.POPULATE_META_FIELDS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            .setPreCombineFields(writeConfigs.get("hoodie.datasource.write.precombine.field"))
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

  @Override
  public void commitSchemaToTable(InternalSchema schema, Map<String, String> writeConfigs, String historySchemaStr) {
    String tableName = writeConfigs.get(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    Schema avroSchema = AvroInternalSchemaConverter.convert(schema, getAvroRecordQualifiedName(tableName));

    StorageConfiguration<?> storageConf = getStorageConf();
    String basePath = getBasePath();

    Map<String, String> finalWriteConfigs = new HashMap<>(writeConfigs);
    finalWriteConfigs.put(HoodieCleanConfig.AUTO_CLEAN.key(), "false");
    finalWriteConfigs.put(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
        HoodieFailedWritesCleaningPolicy.NEVER.name());
    finalWriteConfigs.put(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "false");

    HoodieJavaClientTestHarness.TestJavaTaskContextSupplier taskContextSupplier = new HoodieJavaClientTestHarness.TestJavaTaskContextSupplier();
    HoodieJavaEngineContext context = new HoodieJavaEngineContext(getStorageConf(), taskContextSupplier);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(avroSchema.toString())
        .withEngineType(EngineType.JAVA)
        .withProps(finalWriteConfigs)
        .build();

    try (HoodieJavaWriteClient<?> client = new HoodieJavaWriteClient<>(context, config)) {

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setConf(storageConf)
          .setBasePath(basePath)
          .setTimeGeneratorConfig(config.getTimeGeneratorConfig())
          .build();

      WriteOperationType operationType = WriteOperationType.ALTER_SCHEMA;
      String commitActionType = CommitUtils.getCommitActionType(operationType, metaClient.getTableType());

      String instantTime = client.startCommit(commitActionType);
      client.setOperationType(operationType);

      HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
      TimelineLayout layout = metaClient.getTimelineLayout();
      HoodieInstant requested = layout.getInstantGenerator()
          .createNewInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime);

      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      metadata.setOperationType(operationType);

      timeline.transitionRequestedToInflight(requested, Option.of(metadata));

      long schemaId = Long.parseLong(instantTime);
      InternalSchema withId = schema.setSchemaId(schemaId);
      Map<String, String> extraMeta  = Collections.singletonMap(SerDeHelper.LATEST_SCHEMA, SerDeHelper.toJson(withId));

      FileBasedInternalSchemaStorageManager schemaManager = new FileBasedInternalSchemaStorageManager(metaClient);
      schemaManager.persistHistorySchemaStr(instantTime,
          SerDeHelper.inheritSchemas(schema, historySchemaStr));

      assertTrue(client.commit(instantTime, Collections.emptyList(), Option.of(extraMeta)));
    }
  }
}
