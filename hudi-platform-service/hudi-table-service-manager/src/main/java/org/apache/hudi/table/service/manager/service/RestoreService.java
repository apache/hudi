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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.table.service.manager.common.ServiceContext;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.entity.InstanceStatus;
import org.apache.hudi.table.service.manager.store.MetadataStore;
import org.apache.hudi.table.service.manager.util.DateTimeUtils;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RestoreService implements BaseService {

  private static final Logger LOG = LogManager.getLogger(RestoreService.class);

  private final MetadataStore metadataStore;
  private ScheduledExecutorService service;
  private final int submitJobTimeoutSec;
  private YarnClient yarnClient;

  public RestoreService(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
    this.submitJobTimeoutSec = metadataStore.getTableServiceManagerConfig().getInstanceSubmitTimeoutSec();
  }

  @Override
  public void init() {
    LOG.info("Init service: " + this.getClass().getName());
    this.service = Executors.newSingleThreadScheduledExecutor();
    this.yarnClient = ServiceContext.getYarnClient();
    this.yarnClient.init(new YarnConfiguration());
    this.yarnClient.start();
    LOG.info("Finish init service: " + this.getClass().getName());
  }

  @Override
  public void startService() {
    service.scheduleAtFixedRate(new RestoreRunnable(), 30, 360, TimeUnit.SECONDS);
    LOG.info("Finish start service: " + this.getClass().getName());
  }

  @Override
  public void stop() {
    LOG.info("Stop service: " + this.getClass().getName());
    if (service != null && !service.isShutdown()) {
      service.shutdown();
    }
    LOG.info("Finish stop service: " + this.getClass().getName());
  }

  private class RestoreRunnable implements Runnable {

    @Override
    public void run() {
      try {
        LOG.info("Start process restore service");
        restoreRunningInstances();
        LOG.info("Finish process restore service");
      } catch (Throwable e) {
        LOG.error("Fail process restore service", e);
      }
    }
  }

  private void restoreRunningInstances() {
    Date curTime = new Date();
    List<Instance> runningInstances = metadataStore.getInstances(InstanceStatus.RUNNING.getStatus(), -1);

    for (Instance instance : runningInstances) {
      String applicationId = instance.getApplicationId();
      try {
        if (curTime.before(DateTimeUtils.addSecond(instance.getScheduleTime(), 300))) {
          continue;
        }

        if (StringUtils.isNullOrEmpty(applicationId)) {
          if (curTime.after(DateTimeUtils.addSecond(instance.getScheduleTime(), submitJobTimeoutSec))) {
            LOG.warn("Submit job timeout, should kill  " + instance + ", curTime: " + curTime);
            instance.setStatus(InstanceStatus.FAILED.getStatus());
            metadataStore.updateStatus(instance);
          }
          continue;
        }

        ApplicationReport applicationReport = yarnClient.getApplicationReport(ApplicationId.fromString(applicationId));
        if (isFinished(applicationReport.getYarnApplicationState())) {
          LOG.info("Job has done " + applicationReport.getYarnApplicationState() + ", id: " + applicationId + ", instance " + instance);
          if (applicationReport.getYarnApplicationState() == YarnApplicationState.FINISHED) {
            instance.setStatus(InstanceStatus.COMPLETED.getStatus());
          } else {
            instance.setStatus(InstanceStatus.FAILED.getStatus());
          }
          metadataStore.updateStatus(instance);
        } else {
          LOG.info("Job is running: " + applicationId + ", instance: " + instance.getRecordKey());
        }
      } catch (Exception e) {
        LOG.error("Fail restore Job: " + instance.getInstanceRunStatus(), e);
      }
    }
  }

  private boolean isFinished(YarnApplicationState state) {
    switch (state) {
      case FINISHED:
      case FAILED:
      case KILLED:
        return true;
    }

    return false;
  }
}
