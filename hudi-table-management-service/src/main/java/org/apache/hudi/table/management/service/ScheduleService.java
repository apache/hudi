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

package org.apache.hudi.table.management.service;

import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.entity.Action;
import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.entity.InstanceStatus;
import org.apache.hudi.table.management.exception.HoodieTableManagementException;
import org.apache.hudi.table.management.executor.BaseActionExecutor;
import org.apache.hudi.table.management.executor.CompactionExecutor;
import org.apache.hudi.table.management.store.MetadataStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleService implements BaseService {

  private static final Logger LOG = LoggerFactory.getLogger(ScheduleService.class);

  private ScheduledExecutorService service;
  private ExecutorService executionService;
  private MetadataStore metadataStore;
  private int compactionWaitInterval;

  public ScheduleService(ExecutorService executionService,
                         MetadataStore metadataStore) {
    this.executionService = executionService;
    this.metadataStore = metadataStore;
    this.compactionWaitInterval = ServiceConfig.getInstance()
        .getInt(ServiceConfig.ServiceConfVars.CompactionScheduleWaitInterval);
  }

  @Override
  public void init() {
    LOG.info("Finish init schedule service, compactionWaitInterval: " + compactionWaitInterval);
    //ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("Schedule-Service-%d").build();
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
        && instance.getUpdateTime().getTime() + compactionWaitInterval
        > System.currentTimeMillis();
  }

  protected BaseActionExecutor getActionExecutor(Instance instance) {
    if (instance.getAction() == Action.COMPACTION.getValue()) {
      return new CompactionExecutor(instance);
    } else {
      throw new HoodieTableManagementException("Unsupported action " + instance.getAction());
    }
  }

}
