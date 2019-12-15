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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.model.TimelineLayoutVersion;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.api.java.JavaSparkContext;

public class ClientUtils {

  /**
   * Create Consistency Aware MetaClient.
   *
   * @param jsc JavaSparkContext
   * @param config HoodieWriteConfig
   * @param loadActiveTimelineOnLoad early loading of timeline
   */
  public static HoodieTableMetaClient createMetaClient(JavaSparkContext jsc, HoodieWriteConfig config,
      boolean loadActiveTimelineOnLoad) {
    return new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), loadActiveTimelineOnLoad,
        config.getConsistencyGuardConfig(), Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion())));
  }
}
