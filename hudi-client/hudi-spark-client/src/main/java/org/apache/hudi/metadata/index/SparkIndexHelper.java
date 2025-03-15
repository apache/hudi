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

package org.apache.hudi.metadata.index;

import org.apache.hudi.client.utils.SparkMetadataWriterUtils;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.expression.HoodieSparkExpressionIndex;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;

public class SparkIndexHelper implements EngineIndexHelper {

  private static final Logger LOG = LoggerFactory.getLogger(SparkIndexHelper.class);
  private final HoodieEngineContext engineContext;
  private final HoodieWriteConfig dataTableWriteConfig;
  private final HoodieWriteConfig metadataTableWriteConfig;

  public SparkIndexHelper(HoodieEngineContext engineContext,
                          HoodieWriteConfig dataTableWriteConfig,
                          HoodieWriteConfig metadataTableWriteConfig) {
    this.engineContext = engineContext;
    this.dataTableWriteConfig = dataTableWriteConfig;
    this.metadataTableWriteConfig = metadataTableWriteConfig;
  }

  @Override
  public HoodieData<HoodieRecord> getExpressionIndexRecords(
      List<Pair<String, Pair<String, Long>>> partitionFilePathAndSizeTriplet, HoodieIndexDefinition indexDefinition,
      HoodieTableMetaClient metaClient, int parallelism, Schema readerSchema, StorageConfiguration<?> storageConf,
      String instantTime) {
    HoodieSparkExpressionIndex.ExpressionIndexComputationMetadata expressionIndexComputationMetadata =
        SparkMetadataWriterUtils.getExprIndexRecords(partitionFilePathAndSizeTriplet, indexDefinition,
            metaClient, parallelism, readerSchema, instantTime, engineContext, dataTableWriteConfig,
            metadataTableWriteConfig,
            Option.of(rangeMetadata ->
                HoodieTableMetadataUtil.collectAndProcessExprIndexPartitionStatRecords(rangeMetadata, true,
                    Option.of(indexDefinition.getIndexName()))));
    HoodieData<HoodieRecord> exprIndexRecords = expressionIndexComputationMetadata.getExpressionIndexRecords();
    if (indexDefinition.getIndexType().equals(PARTITION_NAME_COLUMN_STATS)) {
      exprIndexRecords =
          exprIndexRecords.union(expressionIndexComputationMetadata.getPartitionStatRecordsOption().get());
    }
    return exprIndexRecords;
  }
}
