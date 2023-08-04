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

package org.apache.hudi.functional;

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.JavaSizeBasedClusteringPlanStrategy;
import org.apache.hudi.client.clustering.run.strategy.JavaSortAndSizeExecutionStrategy;
import org.apache.hudi.client.functional.BaseTestHoodieBackedMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadataWriter;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.JavaHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class TestJavaHoodieBackedMetadata extends HoodieJavaClientTestHarness {
  @Nested
  @DisplayName("Standard Suite of Metadata Table Tests")
  public class StandardSuite extends BaseTestHoodieBackedMetadata<List<HoodieRecord>, List<WriteStatus>> {

    @BeforeEach
    public void setup() throws IOException {
      TestJavaHoodieBackedMetadata.this.initResources();
    }

    @Test
    public void foo() {
      System.out.println("foo");
    }

    @Override
    protected BaseHoodieWriteClient<?, List<HoodieRecord>, ?, List<WriteStatus>> getHoodieWriteClient(HoodieEngineContext context, HoodieWriteConfig writeConfig) {
      return new HoodieJavaWriteClient(context, writeConfig);
    }

    @Override
    protected List<HoodieRecord> convertInputRecords(List<HoodieRecord> inputRecords) {
      return inputRecords;
    }

    @Override
    protected List<WriteStatus> collectWriteStatuses(List<WriteStatus> writeOutputs) {
      return writeOutputs;
    }

    @Override
    protected String getClusteringPlanStrategyClass() {
      return JavaSizeBasedClusteringPlanStrategy.class.getName();
    }

    @Override
    protected String getClusteringExecutionStrategyClass() {
      return JavaSortAndSizeExecutionStrategy.class.getName();
    }

    @Override
    public void init(HoodieTableType tableType, Option<HoodieWriteConfig> writeConfig, boolean enableMetadataTable,
                     boolean enableMetrics, boolean validateMetadataPayloadStateConsistency) throws IOException {
      this.tableType = tableType;
      initPath();
      initFileSystem(basePath, hadoopConf);
      // use HoodieJavaClientTestHarness state for file system and engine context
      this.fs = TestJavaHoodieBackedMetadata.this.fs;
      this.engineContext = context;
      fs.mkdirs(new Path(basePath));
      initMetaClient(tableType);
      initTestDataGenerator();
      metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
      this.writeConfig = writeConfig.isPresent()
          ? writeConfig.get() : getWriteConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER, true,
          enableMetadataTable, enableMetrics, true,
          validateMetadataPayloadStateConsistency)
          .build();
      initWriteConfigAndMetatableWriter(this.writeConfig, enableMetadataTable);
    }

    protected void initWriteConfigAndMetatableWriter(HoodieWriteConfig writeConfig, boolean enableMetadataTable) throws IOException {
      this.writeConfig = writeConfig;
      if (enableMetadataTable) {
        metadataWriter = JavaHoodieBackedTableMetadataWriter.create(hadoopConf, writeConfig, context, Option.empty());
        // reload because table configs could have been updated
        metaClient = HoodieTableMetaClient.reload(metaClient);
        testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter, Option.of(context));
      } else {
        testTable = HoodieTestTable.of(metaClient);
      }
    }

    @Override
    public HoodieBackedTableMetadataWriter createTableMetadataWriter(HoodieWriteConfig writeConfig, HoodieEngineContext context) {
      return (HoodieBackedTableMetadataWriter) JavaHoodieBackedTableMetadataWriter.create(hadoopConf, writeConfig, context, Option.empty());
    }

    @Override
    public void cleanupResources() throws Exception {
      TestJavaHoodieBackedMetadata.this.cleanupResources();
    }

    @Override
    public HoodieTable createTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
      return HoodieJavaTable.create(config, context, metaClient);
    }

    @Override
    public HoodieTable createTable(HoodieWriteConfig config, HoodieEngineContext context) {
      return HoodieJavaTable.create(config, context);
    }
  }
}
