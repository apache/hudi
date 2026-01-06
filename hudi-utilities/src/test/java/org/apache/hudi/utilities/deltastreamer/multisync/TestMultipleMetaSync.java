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

package org.apache.hudi.utilities.deltastreamer.multisync;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.exception.HoodieMetaSyncException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.sources.TestDataSource;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMultipleMetaSync extends HoodieDeltaStreamerTestBase {

  @Test
  void testMultipleMetaStore() throws Exception {
    String tableBasePath = basePath + "/test_multiple_metastore";
    MockSyncTool1.syncSuccess = false;
    MockSyncTool2.syncSuccess = false;
    // Initial bulk insert to ingest to first hudi table
    HoodieDeltaStreamer.Config cfg = getConfig(tableBasePath, getSyncNames("MockSyncTool1", "MockSyncTool2"));
    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertTrue(MockSyncTool1.syncSuccess);
    assertTrue(MockSyncTool2.syncSuccess);
  }

  @ParameterizedTest
  @MethodSource("withOneException")
  void testWithException(String syncClassNames) {
    String tableBasePath = basePath + "/test_multiple_metastore_exception";
    MockSyncTool1.syncSuccess = false;
    MockSyncTool2.syncSuccess = false;
    HoodieDeltaStreamer.Config cfg = getConfig(tableBasePath, syncClassNames);
    Exception e = assertThrows(HoodieMetaSyncException.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf())));
    assertTrue(e.getMessage().contains(MockSyncToolException1.class.getName()));
    assertTrue(MockSyncTool1.syncSuccess);
    assertTrue(MockSyncTool2.syncSuccess);
  }

  @Test
  void testMultipleExceptions() {
    String tableBasePath = basePath + "/test_multiple_metastore_multiple_exception";
    MockSyncTool1.syncSuccess = false;
    MockSyncTool2.syncSuccess = false;
    HoodieDeltaStreamer.Config cfg = getConfig(tableBasePath, getSyncNames("MockSyncTool1", "MockSyncTool2", "MockSyncToolException1", "MockSyncToolException2"));
    Exception e = assertThrows(HoodieMetaSyncException.class, () -> syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf())));
    assertTrue(e.getMessage().contains(MockSyncToolException1.class.getName()));
    assertTrue(e.getMessage().contains(MockSyncToolException2.class.getName()));
    assertTrue(MockSyncTool1.syncSuccess);
    assertTrue(MockSyncTool2.syncSuccess);
  }

  HoodieDeltaStreamer.Config getConfig(String basePath, String syncClassNames) {
    HoodieDeltaStreamer.Config cfg = new HoodieDeltaStreamer.Config();
    cfg.targetBasePath = basePath;
    cfg.targetTableName = "hoodie_trips";
    cfg.tableType = "COPY_ON_WRITE";
    cfg.sourceClassName = TestDataSource.class.getName();
    cfg.transformerClassNames = Collections.singletonList(SqlQueryBasedTransformer.class.getName());
    cfg.schemaProviderClassName = FilebasedSchemaProvider.class.getName();
    cfg.syncClientToolClassNames = syncClassNames;
    cfg.operation = WriteOperationType.BULK_INSERT;
    cfg.enableHiveSync = true;
    cfg.sourceOrderingFields = "timestamp";
    cfg.propsFilePath = UtilitiesTestBase.basePath + "/test-source.properties";
    cfg.configs.add("hoodie.datasource.hive_sync.partition_fields=year,month,day");
    cfg.sourceLimit =  1000;
    return cfg;
  }

  private static String getSyncNames(String... syncs) {
    return Arrays.stream(syncs).map(s -> "org.apache.hudi.utilities.deltastreamer.multisync." + s).collect(Collectors.joining(","));
  }

  private static Stream<Arguments> withOneException()  {
    return Stream.of(
        Arguments.of(getSyncNames("MockSyncTool1", "MockSyncTool2", "MockSyncToolException1")),
        Arguments.of(getSyncNames("MockSyncTool1", "MockSyncToolException1", "MockSyncTool2")),
        Arguments.of(getSyncNames("MockSyncToolException1", "MockSyncTool1", "MockSyncTool2"))
    );
  }
}
