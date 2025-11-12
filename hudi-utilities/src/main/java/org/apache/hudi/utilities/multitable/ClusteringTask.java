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
import org.apache.hudi.utilities.HoodieClusteringJob;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Clustering task to run in TableServicePipeline.
 *
 * @see HoodieMultiTableServicesMain
 */
class ClusteringTask extends TableServiceTask {

  /**
   * Parallelism for hoodie clustering.
   */
  private int parallelism;

  /**
   * Mode for running clustering.
   *
   * @see HoodieClusteringJob.Config#runningMode
   */
  private String clusteringMode;

  /**
   * Meta Client.
   */
  private HoodieTableMetaClient metaClient;

  @Override
  void run() {
    HoodieClusteringJob.Config clusteringConfig = new HoodieClusteringJob.Config();
    clusteringConfig.basePath = basePath;
    clusteringConfig.parallelism = parallelism;
    clusteringConfig.runningMode = clusteringMode;
    // HoodieWriteClient within HoodieClusteringJob is closed internally. not closing HoodieCleaner here is not leaking any resources.
    new HoodieClusteringJob(jsc, clusteringConfig, props, metaClient).cluster(retry);
  }

  /**
   * Utility to create builder for {@link ClusteringTask}.
   *
   * @return Builder for {@link ClusteringTask}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link ClusteringTask}.
   */
  public static final class Builder {

    /**
     * Properties for running clustering task which are already consolidated w/ CLI provided config-overrides.
     */
    private TypedProperties props;

    /**
     * Parallelism for hoodie clustering.
     */
    private int parallelism;

    /**
     * Clustering mode for running clustering.
     *
     * @see HoodieClusteringJob.Config#runningMode
     */
    private String clusteringMode;

    /**
     * Hoodie table path for running clustering task.
     */
    private String basePath;

    /**
     * JavaSparkContext to run spark job.
     */
    private JavaSparkContext jsc;

    /**
     * Number of retries.
     */
    private int retry;

    /**
     * Meta Client.
     */
    private HoodieTableMetaClient metaClient;

    private Builder() {
    }

    public Builder withProps(TypedProperties props) {
      this.props = props;
      return this;
    }

    public Builder withParallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder withClusteringRunningMode(String clusteringRunningMode) {
      this.clusteringMode = clusteringRunningMode;
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

    public Builder withRetry(int retry) {
      this.retry = retry;
      return this;
    }

    public Builder withMetaclient(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
      return this;
    }

    public ClusteringTask build() {
      ClusteringTask clusteringTask = new ClusteringTask();
      clusteringTask.jsc = this.jsc;
      clusteringTask.parallelism = this.parallelism;
      clusteringTask.clusteringMode = this.clusteringMode;
      clusteringTask.retry = this.retry;
      clusteringTask.basePath = this.basePath;
      clusteringTask.props = this.props;
      clusteringTask.metaClient = this.metaClient;
      return clusteringTask;
    }
  }
}
