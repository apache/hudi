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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.parquet.io.ParquetBinaryCopyChecker;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering execution strategy that uses binary stream copy.
 * This strategy extends SparkBinaryCopyClusteringExecutionStrategy and modifies
 * the supportBinaryStreamCopy method to skip schema checks when schema evolution is disabled.
 */
@Slf4j
public class SparkStreamCopyClusteringExecutionStrategy<T>
    extends SparkBinaryCopyClusteringExecutionStrategy<T> {
  
  public SparkStreamCopyClusteringExecutionStrategy(HoodieTable table,
                                                    HoodieEngineContext engineContext,
                                                    HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }
  
  @Override
  public boolean supportBinaryStreamCopy(List<ClusteringGroupInfo> inputGroups, 
                                        Map<String, String> strategyParams) {
    // Check if table type is Copy-on-Write
    if (getHoodieTable().getMetaClient().getTableType() != COPY_ON_WRITE) {
      log.warn("SparkStreamCopyClusteringExecutionStrategy is only supported for COW tables. Will fall back to common clustering execution strategy.");
      return false;
    }
    
    // Check if sorting is requested (not supported in binary copy)
    Option<String[]> orderByColumnsOpt = 
        Option.ofNullable(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()))
            .map(listStr -> listStr.split(","));
    
    if (orderByColumnsOpt.isPresent()) {
      log.warn("SparkStreamCopyClusteringExecutionStrategy does not support sort by columns. Will fall back to common clustering execution strategy.");
      return false;
    }
    
    // Check if base file format is Parquet
    if (!getHoodieTable().getMetaClient().getTableConfig().getBaseFileFormat().equals(PARQUET)) {
      log.warn("SparkStreamCopyClusteringExecutionStrategy only supports parquet base files. Will fall back to common clustering execution strategy.");
      return false;
    }
    
    // Check if schema evolution is enabled
    if (!writeConfig.isBinaryCopySchemaEvolutionEnabled()) {
      // Skip schema checking when schema evolution is disabled
      log.info("Schema evolution disabled, skipping schema compatibility checks for binary stream copy");
      return true;
    }
    
    // Perform schema compatibility checks when schema evolution is enabled
    log.info("Schema evolution enabled, performing schema compatibility checks for binary stream copy");
    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    
    List<ParquetBinaryCopyChecker.ParquetFileInfo> fileStatus = engineContext.parallelize(inputGroups, inputGroups.size())
        .flatMap(group -> group.getOperations().iterator())
        .map(op -> {
          String filePath = op.getDataFilePath();
          return ParquetBinaryCopyChecker.collectFileInfo(
              getHoodieTable().getStorageConf().unwrapAs(Configuration.class), 
              filePath);
        })
        .collect();
    
    boolean compatible = ParquetBinaryCopyChecker.verifyFiles(fileStatus);
    if (!compatible) {
      log.warn("Schema compatibility check failed. Will fall back to common clustering execution strategy.");
    }
    
    return compatible;
  }
}