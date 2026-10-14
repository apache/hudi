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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Immutable parameter bundle passed to a {@link ClusteringGroupWriter} for a single
 * clustering group. Wraps the inputs the SPI needs (clustering group, strategy params,
 * instant time, executor, schema, table, write config) so the SPI signature can grow
 * without breaking implementers.
 */
public final class ClusteringGroupWriteContext {

  private final HoodieClusteringGroup clusteringGroup;
  private final Map<String, String> strategyParams;
  private final boolean shouldPreserveHoodieMetadata;
  private final String instantTime;
  private final ExecutorService clusteringExecutorService;
  private final HoodieSchema schema;
  private final HoodieTable table;
  private final HoodieWriteConfig writeConfig;

  private ClusteringGroupWriteContext(Builder b) {
    this.clusteringGroup = Objects.requireNonNull(b.clusteringGroup, "clusteringGroup");
    this.strategyParams = b.strategyParams != null
        ? Collections.unmodifiableMap(b.strategyParams)
        : Collections.emptyMap();
    this.shouldPreserveHoodieMetadata = b.shouldPreserveHoodieMetadata;
    this.instantTime = Objects.requireNonNull(b.instantTime, "instantTime");
    this.clusteringExecutorService =
        Objects.requireNonNull(b.clusteringExecutorService, "clusteringExecutorService");
    this.schema = Objects.requireNonNull(b.schema, "schema");
    this.table = Objects.requireNonNull(b.table, "table");
    this.writeConfig = Objects.requireNonNull(b.writeConfig, "writeConfig");
  }

  public HoodieClusteringGroup getClusteringGroup() {
    return clusteringGroup;
  }

  public Map<String, String> getStrategyParams() {
    return strategyParams;
  }

  public boolean shouldPreserveHoodieMetadata() {
    return shouldPreserveHoodieMetadata;
  }

  public String getInstantTime() {
    return instantTime;
  }

  public ExecutorService getClusteringExecutorService() {
    return clusteringExecutorService;
  }

  public HoodieSchema getSchema() {
    return schema;
  }

  public HoodieTable getTable() {
    return table;
  }

  public HoodieWriteConfig getWriteConfig() {
    return writeConfig;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {
    private HoodieClusteringGroup clusteringGroup;
    private Map<String, String> strategyParams;
    private boolean shouldPreserveHoodieMetadata;
    private String instantTime;
    private ExecutorService clusteringExecutorService;
    private HoodieSchema schema;
    private HoodieTable table;
    private HoodieWriteConfig writeConfig;

    public Builder clusteringGroup(HoodieClusteringGroup v) {
      this.clusteringGroup = v;
      return this;
    }

    public Builder strategyParams(Map<String, String> v) {
      this.strategyParams = v;
      return this;
    }

    public Builder shouldPreserveHoodieMetadata(boolean v) {
      this.shouldPreserveHoodieMetadata = v;
      return this;
    }

    public Builder instantTime(String v) {
      this.instantTime = v;
      return this;
    }

    public Builder clusteringExecutorService(ExecutorService v) {
      this.clusteringExecutorService = v;
      return this;
    }

    public Builder schema(HoodieSchema v) {
      this.schema = v;
      return this;
    }

    public Builder table(HoodieTable v) {
      this.table = v;
      return this;
    }

    public Builder writeConfig(HoodieWriteConfig v) {
      this.writeConfig = v;
      return this;
    }

    public ClusteringGroupWriteContext build() {
      return new ClusteringGroupWriteContext(this);
    }
  }
}
