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

import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;
import org.apache.hudi.keygen.ComplexAvroKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.testutils.Assertions.assertComplexKeyGeneratorValidationThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBaseHoodieWriteClient extends HoodieCommonTestHarness {

  private static Stream<Arguments> testWithComplexKeyGeneratorValidation() {
    List<Arguments> arguments = new ArrayList<>();

    List<Arguments> keyAndPartitionFieldOptions = Arrays.asList(
        Arguments.of("r1", "p1"),
        Arguments.of("r1", "p1,p2"),
        Arguments.of("r1", ""),
        Arguments.of("r1,r2", "p1")
    );

    List<Arguments> booleanOptions = Arrays.asList(
        Arguments.of(false, true),
        Arguments.of(true, true),
        Arguments.of(true, false)
    );

    List<Integer> tableVersionOptions = Arrays.asList(8);

    arguments.addAll(Stream.of("org.apache.hudi.keygen.ComplexAvroKeyGenerator",
            "org.apache.hudi.keygen.ComplexKeyGenerator")
        .flatMap(keyGenClass -> keyAndPartitionFieldOptions.stream()
            .flatMap(keyAndPartitionField -> booleanOptions.stream()
                .flatMap(booleans -> tableVersionOptions.stream()
                    .map(tableVersion -> Arguments.of(
                        keyGenClass,
                        keyAndPartitionField.get()[0],
                        keyAndPartitionField.get()[1],
                        booleans.get()[0],
                        booleans.get()[1],
                        tableVersion
                    ))
                )
            ))
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.SimpleAvroKeyGenerator",
            "org.apache.hudi.keygen.SimpleKeyGenerator",
            "org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator",
            "org.apache.hudi.keygen.TimestampBasedKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "p1",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator",
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));
    arguments.addAll(Stream.of("org.apache.hudi.keygen.CustomAvroKeyGenerator",
            "org.apache.hudi.keygen.CustomKeyGenerator")
        .flatMap(keyGenClass -> booleanOptions.stream()
            .flatMap(booleans -> tableVersionOptions.stream()
                .map(tableVersion -> Arguments.of(
                    keyGenClass,
                    "r1",
                    "p1:SIMPLE",
                    booleans.get()[0],
                    booleans.get()[1],
                    tableVersion
                ))
            )
        )
        .collect(Collectors.toList()));

    return arguments.stream();
  }

  @ParameterizedTest
  @MethodSource
  void testWithComplexKeyGeneratorValidation(String keyGeneratorClass,
                                                        String recordKeyFields,
                                                        String partitionPathFields,
                                                        boolean setComplexKeyGeneratorValidationConfig,
                                                        boolean enableComplexKeyGeneratorValidation,
                                                        int tableVersion) throws IOException {
    if (basePath == null) {
      initPath();
    }
    Properties tableProperties = new Properties();
    tableProperties.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    tableProperties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyFields);
    tableProperties.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionPathFields);
    tableProperties.put(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion));
    Properties writeProperties = new Properties();
    writeProperties.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key(), keyGeneratorClass);
    writeProperties.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), recordKeyFields);
    writeProperties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPathFields);
    if (setComplexKeyGeneratorValidationConfig) {
      writeProperties.put(
          HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key(), enableComplexKeyGeneratorValidation);
    }
    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, getTableType(), tableProperties);
    HoodieWriteConfig.Builder writeConfigBuilder = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProperties(writeProperties);
    HoodieTable<String, String, String, String> table = mock(HoodieTable.class);
    BaseHoodieTableServiceClient<String, String, String> tableServiceClient = mock(BaseHoodieTableServiceClient.class);
    TestWriteClient writeClient = new TestWriteClient(writeConfigBuilder.build(), table, Option.empty(), tableServiceClient);

    if (tableVersion <= 8 && enableComplexKeyGeneratorValidation
        && (ComplexAvroKeyGenerator.class.getCanonicalName().equals(keyGeneratorClass)
        || "org.apache.hudi.keygen.ComplexKeyGenerator".equals(keyGeneratorClass))
        && recordKeyFields.split(",").length == 1) {
      assertComplexKeyGeneratorValidationThrows(() -> writeClient.initTable(WriteOperationType.INSERT, Option.empty()), "ingestion");
    } else {
      writeClient.initTable(WriteOperationType.INSERT, Option.empty());
      String requestedTime = writeClient.startCommit();

      HoodieTimeline writeTimeline = metaClient.getActiveTimeline().getWriteTimeline();
      assertTrue(writeTimeline.lastInstant().isPresent());
      assertEquals("commit", writeTimeline.lastInstant().get().getAction());
      assertEquals(requestedTime, writeTimeline.lastInstant().get().getTimestamp());
    }
  }

  private static class TestWriteClient extends BaseHoodieWriteClient<String, String, String, String> {
    private final HoodieTable<String, String, String, String> table;

    public TestWriteClient(HoodieWriteConfig writeConfig, HoodieTable<String, String, String, String> table, Option<EmbeddedTimelineService> timelineService,
                           BaseHoodieTableServiceClient<String, String, String> tableServiceClient) {
      super(new HoodieLocalEngineContext(getDefaultStorageConf()), writeConfig, timelineService, null);
      this.table = table;
      this.tableServiceClient = tableServiceClient;
    }

    @Override
    protected HoodieIndex<?, ?> createIndex(HoodieWriteConfig writeConfig) {
      return new HoodieSimpleIndex(config, Option.empty());
    }

    @Override
    public boolean commit(String instantTime, String writeStatuses, Option<Map<String, String>> extraMetadata, String commitActionType, Map<String, List<String>> partitionToReplacedFileIds,
                          Option<BiConsumer<HoodieTableMetaClient, HoodieCommitMetadata>> extraPreCommitFunc) {
      return false;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, Configuration hadoopConf) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      return table;
    }

    @Override
    protected HoodieTable<String, String, String, String> createTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
      // table should only be made with remote view config for these tests
      FileSystemViewStorageType storageType = config.getViewStorageConfig().getStorageType();
      Assertions.assertTrue(storageType == FileSystemViewStorageType.REMOTE_FIRST || storageType == FileSystemViewStorageType.REMOTE_ONLY);
      // Ensure the returned table has the correct metaClient
      when(table.getMetaClient()).thenReturn(metaClient);
      return table;
    }

    @Override
    protected void validateTimestamp(HoodieTableMetaClient metaClient, String instantTime) {
    }

    @Override
    public String filterExists(String hoodieRecords) {
      return "";
    }

    @Override
    public String upsert(String records, String instantTime) {
      return "";
    }

    @Override
    public String upsertPreppedRecords(String preppedRecords, String instantTime) {
      return "";
    }

    @Override
    public String insert(String records, String instantTime) {
      return "";
    }

    @Override
    public String insertPreppedRecords(String preppedRecords, String instantTime) {
      return "";
    }

    @Override
    public String bulkInsert(String records, String instantTime) {
      return "";
    }

    @Override
    public String bulkInsert(String records, String instantTime, Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
      return "";
    }

    @Override
    public String bulkInsertPreppedRecords(String preppedRecords, String instantTime, Option<BulkInsertPartitioner> bulkInsertPartitioner) {
      return "";
    }

    @Override
    public String delete(String keys, String instantTime) {
      return "";
    }

    @Override
    public String deletePrepped(String preppedRecords, String instantTime) {
      return "";
    }
  }
}