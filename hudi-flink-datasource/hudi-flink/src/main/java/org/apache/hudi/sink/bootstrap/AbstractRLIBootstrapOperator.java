/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.util.StreamerUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Base class for bootstrap operators that load record level index (RLI) data from the metadata
 * table, shared by {@link RLIBootstrapOperator} and {@link TimeBoundedRLIBootstrapOperator}.
 */
@Slf4j
public abstract class AbstractRLIBootstrapOperator
    extends AbstractBootstrapOperator {

  protected transient HoodieBackedTableMetadata tableMetadata;
  protected transient long loadedCnt;

  protected AbstractRLIBootstrapOperator(Configuration conf) {
    super(conf);
  }

  @Override
  public void close() throws Exception {
    closeMetadataTable();
    super.close();
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  protected HoodieBackedTableMetadata createTableMetadata(HoodieTableMetaClient metaClient) {
    return new HoodieBackedTableMetadata(
        HoodieFlinkEngineContext.DEFAULT,
        metaClient.getStorage(),
        StreamerUtil.metadataConfig(conf),
        conf.get(FlinkOptions.PATH));
  }

  protected void emitIndexRecord(String partitionPath, String recordKey, HoodieRecordGlobalLocation location) {
    output.collect(new StreamRecord<>(
        new HoodieFlinkInternalRow(
            recordKey,
            partitionPath,
            location.getFileId(),
            String.valueOf(location.getInstantTime()))));
    loadedCnt += 1;
  }

  protected void closeMetadataTable() {
    if (tableMetadata != null) {
      try {
        tableMetadata.close();
      } catch (Exception e) {
        log.warn("Failed to close metadata table", e);
      }
      tableMetadata = null;
    }
  }
}
