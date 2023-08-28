/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.utilities.multitable;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.HoodieClusteringJob;

import org.apache.spark.api.java.JavaSparkContext;

class ClusteringTask extends TableServiceTask {

  private int parallelism;
  private String clusteringMode;

  @Override
  void run() {
    HoodieClusteringJob.Config clusteringConfig = new HoodieClusteringJob.Config();
    clusteringConfig.basePath = basePath;
    clusteringConfig.parallelism = parallelism;
    clusteringConfig.runningMode = clusteringMode;
    new HoodieClusteringJob(jsc, clusteringConfig, props).cluster(retry);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private TypedProperties props;
    private int parallelism;
    private String clusteringMode;
    private String basePath;
    private JavaSparkContext jsc;
    private int retry;

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

    public ClusteringTask build() {
      ClusteringTask clusteringTask = new ClusteringTask();
      clusteringTask.jsc = this.jsc;
      clusteringTask.parallelism = this.parallelism;
      clusteringTask.clusteringMode = this.clusteringMode;
      clusteringTask.retry = this.retry;
      clusteringTask.basePath = this.basePath;
      clusteringTask.props = this.props;
      return clusteringTask;
    }
  }
}
