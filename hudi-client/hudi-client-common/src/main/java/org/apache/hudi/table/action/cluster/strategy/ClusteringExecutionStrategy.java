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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Pluggable implementation for writing data into new file groups based on ClusteringPlan.
 */
public abstract class ClusteringExecutionStrategy<T, I, K, O> implements Serializable {
  private static final Logger LOG = LogManager.getLogger(ClusteringExecutionStrategy.class);

  private final HoodieTable<T, I, K, O> hoodieTable;
  private final transient HoodieEngineContext engineContext;
  private final HoodieWriteConfig writeConfig;
  protected final HoodieRecordType recordType;

  public ClusteringExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
    this.hoodieTable = table;
    this.engineContext = engineContext;
    this.recordType = table.getConfig().getRecordType();
  }

  /**
   * Execute clustering to write inputRecords into new files as defined by rules in strategy parameters. The number of new
   * file groups created is bounded by numOutputGroups.
   * Note that commit is not done as part of strategy. commit is callers responsibility.
   */
  public abstract HoodieWriteMetadata<O> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime);

  protected HoodieTable<T, I, K, O> getHoodieTable() {
    return this.hoodieTable;
  }

  protected HoodieEngineContext getEngineContext() {
    return this.engineContext;
  }

  protected HoodieWriteConfig getWriteConfig() {
    return this.writeConfig;
  }
}
