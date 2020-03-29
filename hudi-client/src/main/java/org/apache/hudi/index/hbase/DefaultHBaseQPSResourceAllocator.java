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

package org.apache.hudi.index.hbase;

import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class DefaultHBaseQPSResourceAllocator implements HBaseIndexQPSResourceAllocator {
  private HoodieWriteConfig hoodieWriteConfig;
  private static final Logger LOG = LogManager.getLogger(DefaultHBaseQPSResourceAllocator.class);

  public DefaultHBaseQPSResourceAllocator(HoodieWriteConfig hoodieWriteConfig) {
    this.hoodieWriteConfig = hoodieWriteConfig;
  }

  @Override
  public float calculateQPSFractionForPutsTime(final long numPuts, final int numRegionServers) {
    // Just return the configured qps_fraction without calculating it runtime
    return hoodieWriteConfig.getHbaseIndexQPSFraction();
  }

  @Override
  public float acquireQPSResources(final float desiredQPSFraction, final long numPuts) {
    // Return the requested QPSFraction in this default implementation
    return desiredQPSFraction;
  }

  @Override
  public void releaseQPSResources() {
    // Do nothing, as there are no resources locked in default implementation
    LOG.info(String.format("Release QPS resources called for %s with default implementation, do nothing",
        this.hoodieWriteConfig.getHbaseTableName()));
  }
}
