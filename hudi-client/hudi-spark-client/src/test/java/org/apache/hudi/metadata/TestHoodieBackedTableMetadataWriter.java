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

package org.apache.hudi.metadata;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.functional.TestHoodieMetadataBase;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieBackedTableMetadataWriter extends TestHoodieMetadataBase {
  private static final Logger LOG = LogManager.getLogger(TestHoodieBackedTableMetadataWriter.class);

  private class TestMetadataWriter extends HoodieBackedTableMetadataWriter {

    /**
     * Hudi backed table metadata writer.
     *
     * @param hadoopConf     - Hadoop configuration to use for the metadata writer
     * @param writeConfig    - Writer config
     * @param engineContext  - Engine context
     * @param actionMetadata - Optional action metadata to help decide bootstrap operations
     */
    protected <T extends SpecificRecordBase> TestMetadataWriter(Configuration hadoopConf,
                                                                HoodieWriteConfig writeConfig,
                                                                HoodieEngineContext engineContext,
                                                                Option<T> actionMetadata) {
      super(hadoopConf, writeConfig, engineContext, actionMetadata);
    }

    @Override
    protected void initRegistry() {
      //
    }

    @Override
    protected <T extends SpecificRecordBase> void initialize(HoodieEngineContext engineContext,
                                                             Option<T> actionMetadata) {
      try {
        bootstrapIfNeeded(engineContext, dataMetaClient, actionMetadata);
      } catch (IOException e) {
        throw new HoodieMetadataException("Failed to initialize metadata table!");
      }
    }

    @Override
    protected void commit(List<HoodieRecord> records, String partitionName, String instantTime) {
      //
    }

    @Override
    protected void commit(String instantTime, Map<MetadataPartitionType, List<HoodieRecord>> partitionRecordsMap) {
      //
    }

  }

  protected HoodieTableMetadataWriter getMetadataWriter(final Configuration hadoopConf,
                                                        HoodieWriteConfig writeConfig,
                                                        HoodieSparkEngineContext context) {
    return new TestMetadataWriter(hadoopConf, writeConfig, context, Option.empty());
  }

  @Test
  public void testMetadataPartitionBloomFilter() throws Exception {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    init(tableType);

    final boolean isTestMetadataWriter = (metadataWriter instanceof TestMetadataWriter);
    assertTrue(isTestMetadataWriter);

    final TestMetadataWriter testMetadataWriter = (TestMetadataWriter) metadataWriter;
    final HoodieBackedTableMetadata testTableMetadata = testMetadataWriter.getTableMetadata();

    assertTrue(testTableMetadata.isBloomFilterMetadataEnabled);
  }
}
