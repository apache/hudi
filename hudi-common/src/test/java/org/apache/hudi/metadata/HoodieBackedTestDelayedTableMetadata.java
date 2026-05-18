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

package org.apache.hudi.metadata;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.storage.HoodieStorage;

import lombok.extern.slf4j.Slf4j;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table,
 * with an intentional delay in `reset()` to test concurrent reads and writes.
 */
@Slf4j
public class HoodieBackedTestDelayedTableMetadata extends HoodieBackedTableMetadata {

  public HoodieBackedTestDelayedTableMetadata(HoodieEngineContext engineContext,
                                              HoodieStorage storage,
                                              HoodieMetadataConfig metadataConfig,
                                              String datasetBasePath,
                                              boolean reuse) {
    super(engineContext, storage, metadataConfig, datasetBasePath, reuse);
  }

  @Override
  public void reset() {
    log.info("Sleeping for 5 seconds in reset() to simulate processing ...");
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      log.warn("Sleep is interrupted", e);
    }
    log.info("Sleep in reset() is finished.");
    super.reset();
  }
}
