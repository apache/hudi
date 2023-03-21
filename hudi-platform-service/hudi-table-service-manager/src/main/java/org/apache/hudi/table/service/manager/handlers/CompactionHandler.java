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

package org.apache.hudi.table.service.manager.handlers;

import org.apache.hudi.table.service.manager.common.ServiceContext;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.store.MetadataStore;

import org.jetbrains.annotations.NotNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * REST Handler servicing compaction requests.
 */
public class CompactionHandler {
  private static Logger LOG = LogManager.getLogger(CompactionHandler.class);
  protected boolean cacheEnable;

  public CompactionHandler(boolean cacheEnable) {
    this.cacheEnable = cacheEnable;
  }

  public void scheduleCompaction(MetadataStore metadataStore,
                                 Instance instance) {
    String recordKey = instance.getRecordKey();
    LOG.info("Start register compaction instance: " + recordKey);
    if ((cacheEnable && ServiceContext.containsPendingInstant(recordKey))
        || metadataStore.getInstance(instance) != null) {
      LOG.warn("Instance has existed, instance: " + instance);
    } else {
      metadataStore.saveInstance(instance);
    }
    if (cacheEnable) {
      ServiceContext.refreshPendingInstant(recordKey);
    }
  }

  public void removeCompaction(@NotNull MetadataStore metadataStore,
                               Instance instance) {
    LOG.info("Start remove compaction instance: " + instance.getIdentifier());
    // 1. check instance exist
    Instance result = metadataStore.getInstance(instance);
    if (result == null) {
      throw new RuntimeException("Instance not exist: " + instance);
    }
    // 2. update status
    metadataStore.updateStatus(instance);
  }
}
