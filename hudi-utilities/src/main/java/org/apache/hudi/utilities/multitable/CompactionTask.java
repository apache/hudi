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
import org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy;
import org.apache.hudi.utilities.HoodieCompactor;

import org.apache.spark.api.java.JavaSparkContext;

class CompactionTask extends TableServiceTask {

  public String compactionRunningMode = HoodieCompactor.EXECUTE;

  public String compactionStrategyName = LogFileSizeBasedCompactionStrategy.class.getName();

  private int parallelism;

  @Override
  void run() {
    HoodieCompactor.Config compactionCfg = new HoodieCompactor.Config();
    compactionCfg.basePath = basePath;
    compactionCfg.strategyClassName = compactionStrategyName;
    compactionCfg.runningMode = compactionRunningMode;
    compactionCfg.parallelism = parallelism;
    compactionCfg.retry = retry;
    new HoodieCompactor(jsc, compactionCfg, props).compact(retry);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private TypedProperties props;
    private String compactionRunningMode;
    private int parallelism;
    private int retry;
    private String basePath;
    private JavaSparkContext jsc;

    public Builder withProps(TypedProperties props) {
      this.props = props;
      return this;
    }

    public Builder withCompactionRunningMode(String compactionRunningMode) {
      this.compactionRunningMode = compactionRunningMode;
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

    public CompactionTask build() {
      CompactionTask compactionTask = new CompactionTask();
      compactionTask.basePath = this.basePath;
      compactionTask.jsc = this.jsc;
      compactionTask.parallelism = this.parallelism;
      compactionTask.compactionRunningMode = this.compactionRunningMode;
      compactionTask.retry = this.retry;
      compactionTask.props = this.props;
      return compactionTask;
    }
  }
}
