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

package org.apache.hudi.table.service.manager.service;

import org.apache.hudi.table.service.manager.common.ServiceConfig;
import org.apache.hudi.table.service.manager.entity.Action;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.entity.InstanceStatus;
import org.apache.hudi.table.service.manager.exception.HoodieTableServiceManagerException;
import org.apache.hudi.table.service.manager.executor.BaseActionExecutor;
import org.apache.hudi.table.service.manager.executor.CompactionExecutor;
import org.apache.hudi.table.service.manager.store.MetadataStore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleService implements BaseService {

  private static final Logger LOG = LogManager.getLogger(ScheduleService.class);

  private ScheduledExecutorService service;
  private final ExecutorService executionService;
  private final MetadataStore metadataStore;
  private final long scheduleIntervalMs;

  public ScheduleService(ExecutorService executionService,
                         MetadataStore metadataStore) {
    this.executionService = executionService;
    this.metadataStore = metadataStore;
    this.scheduleIntervalMs = metadataStore.getTableServiceManagerConfig().getScheduleIntervalMs();
  }

  @Override
  public void init() {
    LOG.info("Finish init schedule service, scheduleIntervalMs: " + scheduleIntervalMs);
    this.service = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void startService() {
    LOG.info("Start service: " + ScheduleService.class.getName());
    service.scheduleAtFixedRate(new ScheduleRunnable(), 30, 60, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    LOG.info("Stop service: " + ScheduleService.class.getName());
    if (service != null && !service.isShutdown()) {
      service.shutdown();
    }
  }

  private class ScheduleRunnable implements Runnable {

    @Override
    public void run() {
      submitReadyTask();
    }
  }

  public void submitReadyTask() {
    int limitSize = executionService.getFreeSize();
    LOG.info("Start get ready instances, limitSize: " + limitSize);
    if (limitSize > 0) {
      List<Instance> readyInstances = metadataStore.getInstances(
          InstanceStatus.SCHEDULED.getStatus(), limitSize);
      for (Instance readyInstance : readyInstances) {
        if (waitSchedule(readyInstance)) {
          LOG.info("Instance should wait schedule: " + readyInstance.getInstanceRunStatus());
          continue;
        }
        LOG.info("Schedule ready instances: " + readyInstance.getInstanceRunStatus());
        BaseActionExecutor executor = getActionExecutor(readyInstance);
        executionService.submitTask(executor);
      }
    }
  }

  private boolean waitSchedule(Instance instance) {
    return instance.getAction() == Action.COMPACTION.getValue()
        && instance.getUpdateTime().getTime() + scheduleIntervalMs
        > System.currentTimeMillis();
  }

  protected BaseActionExecutor getActionExecutor(Instance instance) {
    if (instance.getAction() == Action.COMPACTION.getValue()) {
      return new CompactionExecutor(instance, metadataStore.getTableServiceManagerConfig());
    } else {
      throw new HoodieTableServiceManagerException("Unsupported action " + instance.getAction());
    }
  }

}
