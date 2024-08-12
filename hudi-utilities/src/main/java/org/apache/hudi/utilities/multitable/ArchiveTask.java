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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Archive task to run in TableServicePipeline.
 *
 * @see HoodieMultiTableServicesMain
 */
class ArchiveTask extends TableServiceTask {
  private static final Logger LOG = LoggerFactory.getLogger(ArchiveTask.class);

  @Override
  void run() {
    LOG.info("Run Archive with props: " + props);
    HoodieWriteConfig hoodieCfg = HoodieWriteConfig.newBuilder().withPath(basePath).withProps(props).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), hoodieCfg)) {
      UtilHelpers.retry(retry, () -> {
        client.archive();
        return 0;
      }, "Archive Failed");
    }
  }

  /**
   * Utility to create builder for {@link ArchiveTask}.
   *
   * @return Builder for {@link ArchiveTask}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for {@link ArchiveTask}.
   */
  public static final class Builder {
    /**
     * Properties for running archive task which are already consolidated w/ CLI provided config-overrides.
     */
    private TypedProperties props;

    /**
     * Hoodie table path for running archive task.
     */
    private String basePath;

    /**
     * Number of retries.
     */
    private int retry;

    /**
     * JavaSparkContext to run spark job.
     */
    private JavaSparkContext jsc;

    public Builder withProps(TypedProperties props) {
      this.props = props;
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

    public ArchiveTask build() {
      ArchiveTask archiveTask = new ArchiveTask();
      archiveTask.jsc = this.jsc;
      archiveTask.retry = this.retry;
      archiveTask.props = this.props;
      archiveTask.basePath = this.basePath;
      return archiveTask;
    }
  }
}
