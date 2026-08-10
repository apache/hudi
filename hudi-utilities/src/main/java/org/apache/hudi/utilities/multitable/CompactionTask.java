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

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.utilities.HoodieCompactor;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Compaction task to run in TableServicePipeline.
 *
 * @see HoodieMultiTableServicesMain
 */
class CompactionTask extends TableServiceTask {

  /**
   * Mode for running compaction.
   *
   * @see HoodieCompactor.Config#runningMode
   */
  public String compactionRunningMode;

  /**
   * Strategy Class of compaction.
   */
  public String compactionStrategyName;

  /**
   * Parallelism for hoodie clustering.
   */
  private int parallelism;

  /**
   * Meta Client.
   */
  private HoodieTableMetaClient metaClient;

  @Override
  void run() {
    HoodieCompactor.Config compactionCfg = new HoodieCompactor.Config();
    compactionCfg.basePath = basePath;
    compactionCfg.strategyClassName = compactionStrategyName;
    compactionCfg.runningMode = compactionRunningMode;
    compactionCfg.parallelism = parallelism;
    compactionCfg.retry = retry;
    // HoodieWriteClient within HoodieCompactor is closed internally. not closing HoodieCleaner here is not leaking any resources.
    new HoodieCompactor(jsc, compactionCfg, props, metaClient).compact(retry);
  }

  /**
   * Utility to create builder for {@link CompactionTask}.
   *
   * @return Builder for {@link CompactionTask}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link CompactionTask}.
   */
  public static final class Builder {
    /**
     * Properties for running compaction task which are already consolidated w/ CLI provided config-overrides.
     */
    private TypedProperties props;

    /**
     * Mode for running compaction.
     *
     * @see HoodieCompactor.Config#runningMode
     */
    private String compactionRunningMode;

    /**
     * Strategy Class of compaction.
     */
    public String compactionStrategyName;

    /**
     * Parallelism for hoodie compaction.
     */
    private int parallelism;

    /**
     * Number of retries.
     */
    private int retry;

    /**
     * Hoodie table path for running compaction task.
     */
    private String basePath;

    /**
     * JavaSparkContext to run spark job.
     */
    private JavaSparkContext jsc;

    /**
     * Meta Client.
     */
    private HoodieTableMetaClient metaClient;

    public Builder withProps(TypedProperties props) {
      this.props = props;
      return this;
    }

    public Builder withCompactionRunningMode(String compactionRunningMode) {
      this.compactionRunningMode = compactionRunningMode;
      return this;
    }

    public Builder withCompactionStrategyName(String compactionStrategyName) {
      this.compactionStrategyName = compactionStrategyName;
      return this;
    }

    public Builder withParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder withRetry(int retry) {
      this.retry = retry;
      return this;
    }

    public Builder withBasePath(String basePath) {
      this.basePath = basePath;
      return this;
    }

    public Builder withJsc(JavaSparkContext jsc) {
      this.jsc = jsc;
      return this;
    }

    public Builder withMetaclient(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
      return this;
    }

    public CompactionTask build() {
      CompactionTask compactionTask = new CompactionTask();
      compactionTask.basePath = this.basePath;
      compactionTask.jsc = this.jsc;
      compactionTask.parallelism = this.parallelism;
      compactionTask.compactionRunningMode = this.compactionRunningMode;
      compactionTask.compactionStrategyName = this.compactionStrategyName;
      compactionTask.retry = this.retry;
      compactionTask.props = this.props;
      compactionTask.metaClient = this.metaClient;
      return compactionTask;
    }
  }
}
