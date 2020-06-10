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

package org.apache.hudi.client.embedded;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

/**
 * Spark implementation of Timeline Service.
 */
public class SparkEmbeddedTimelineService extends AbstractEmbeddedTimelineService {
  private static final Logger LOG = LogManager.getLogger(SparkEmbeddedTimelineService.class);

  public SparkEmbeddedTimelineService(HoodieEngineContext context, FileSystemViewStorageConfig config) {
    super(context, config);
  }

  @Override
  public void setHostAddrFromContext(HoodieEngineContext context) {
    SparkConf sparkConf = HoodieSparkEngineContext.getSparkContext(context).getConf();
    String hostAddr = sparkConf.get("spark.driver.host", null);
    if (hostAddr != null) {
      LOG.info("Overriding hostIp to (" + hostAddr + ") found in spark-conf. It was " + this.hostAddr);
      this.hostAddr = hostAddr;
    } else {
      LOG.warn("Unable to find driver bind address from spark config");
    }
  }
}
