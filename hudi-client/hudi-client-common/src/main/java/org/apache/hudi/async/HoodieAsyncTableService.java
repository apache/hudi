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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Function;

@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Slf4j
public abstract class HoodieAsyncTableService extends HoodieAsyncService implements RunsTableService {

  protected HoodieWriteConfig writeConfig;

  protected HoodieAsyncTableService(HoodieWriteConfig writeConfig, boolean runInDaemonMode) {
    super(runInDaemonMode);
    this.writeConfig = writeConfig;
  }

  /**
   * Retry a failed attempt after a client swap only while its instant is still pending.
   * An exception after the commit was completed must not cause the operation to run again.
   */
  protected void retryPendingAsyncServiceInstant(HoodieEngineContext context, String instantTime, Exception failure) {
    HoodieTimeline timeline;
    try {
      timeline = HoodieTableMetaClient.builder()
          .setConf(context.getStorageConf().newInstance())
          .setBasePath(writeConfig.getBasePath())
          .setLoadActiveTimelineOnLoad(true)
          .build().getActiveTimeline().getWriteTimeline();
    } catch (RuntimeException timelineFailure) {
      failure.addSuppressed(timelineFailure);
      throw new HoodieException("Unable to check table service instant " + instantTime + " before retrying", failure);
    }
    if (timeline.filterCompletedInstants().containsInstant(instantTime)) {
      log.warn("Table service instant {} completed before the failure; skipping retry", instantTime, failure);
      return;
    }
    if (!timeline.filterInflightsAndRequested().containsInstant(instantTime)) {
      throw new HoodieException("Cannot retry table service instant " + instantTime + " because it is no longer pending", failure);
    }
    log.warn("Table service instant {} failed after the write client was replaced; retrying with the new client", instantTime, failure);
    enqueuePendingAsyncServiceInstant(instantTime);
  }

  @Override
  public void start(Function<Boolean, Boolean> onShutdownCallback) {
    if (!tableServicesEnabled(writeConfig)) {
      return;
    }
    super.start(onShutdownCallback);
  }
}
