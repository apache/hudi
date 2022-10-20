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

package org.apache.hudi.async;

import org.apache.hudi.client.RunsTableService;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class HoodieAsyncTableService<T extends AutoCloseable> extends HoodieAsyncService implements RunsTableService {

  protected final Object writeConfigUpdateLock = new Object();
  protected HoodieWriteConfig writeConfig;
  protected Option<EmbeddedTimelineService> embeddedTimelineService;
  protected AtomicBoolean isWriteConfigUpdated = new AtomicBoolean(false);

  protected HoodieAsyncTableService() {
  }

  protected HoodieAsyncTableService(HoodieWriteConfig writeConfig, Option<EmbeddedTimelineService> embeddedTimelineService) {
    this.writeConfig = writeConfig;
    this.embeddedTimelineService = embeddedTimelineService;
  }

  protected HoodieAsyncTableService(HoodieWriteConfig writeConfig, Option<EmbeddedTimelineService> embeddedTimelineService, boolean runInDaemonMode) {
    super(runInDaemonMode);
    this.embeddedTimelineService = embeddedTimelineService;
    if (embeddedTimelineService.isPresent()) {
      this.writeConfig = EmbeddedTimelineServerHelper.updateWriteConfigWithTimelineServer(embeddedTimelineService.get(), writeConfig);
    } else {
      this.writeConfig = writeConfig;
    }
  }

  protected T getClient(T client, Supplier<T> clientSupplier) throws Exception {
    synchronized (writeConfigUpdateLock) {
      // re-instantiate only for first time or if write config is updated externally
      if (client == null || isWriteConfigUpdated.get()) {
        if (client != null) {
          client.close();
        }
        client = clientSupplier.get();
        isWriteConfigUpdated.set(false);
      }
    }
    return client;
  }

  @Override
  public void start(Function<Boolean, Boolean> onShutdownCallback) {
    if (!tableServicesEnabled(writeConfig)) {
      return;
    }
    super.start(onShutdownCallback);
  }

  public void updateWriteConfig(HoodieWriteConfig writeConfig) {
    synchronized (writeConfigUpdateLock) {
      this.writeConfig = EmbeddedTimelineServerHelper.updateWriteConfigWithTimelineServer(embeddedTimelineService.get(), writeConfig);
      isWriteConfigUpdated.set(true);
    }
  }
}
