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

package org.apache.hudi.table.management.executor;

import org.apache.hudi.table.management.common.ServiceConfig;
import org.apache.hudi.table.management.common.ServiceContext;
import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.entity.InstanceStatus;
import org.apache.hudi.table.management.executor.submitter.ExecutionEngine;
import org.apache.hudi.table.management.executor.submitter.SparkEngine;
import org.apache.hudi.table.management.store.jdbc.InstanceDao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseActionExecutor implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseActionExecutor.class);

  protected InstanceDao instanceDao;
  protected Instance instance;
  public int maxFailTolerance;
  protected ExecutionEngine engine;

  public BaseActionExecutor(Instance instance) {
    this.instance = instance;
    this.instanceDao = ServiceContext.getInstanceDao();
    this.maxFailTolerance = ServiceConfig.getInstance()
        .getInt(ServiceConfig.ServiceConfVars.IntraMaxFailTolerance);
    String mainClass = ServiceConfig.getInstance()
        .getString(ServiceConfig.ServiceConfVars.CompactionMainClass);
    switch (instance.getExecutionEngine()) {
      case SPARK:
        engine = new SparkEngine(getJobName(instance), instance, mainClass);
        break;
      case FLINK:
      default:
        throw new IllegalStateException("Unexpected value: " + instance.getExecutionEngine());
    }
  }

  @Override
  public void run() {
    ServiceContext.addRunningInstance(instance.getRecordKey(), getThreadIdentifier());
    try {
      execute();
    } finally {
      ServiceContext.removeRunningInstance(instance.getRecordKey());
      if (ServiceConfig.getInstance()
          .getBool(ServiceConfig.ServiceConfVars.CompactionCacheEnable)) {
        ServiceContext.removePendingInstant(instance.getRecordKey());
      }
    }
  }

  public abstract boolean doExecute();

  public abstract String getJobName(Instance instance);

  public void execute() {
    try {
      boolean success = doExecute();
      if (success) {
        instance.setStatus(InstanceStatus.COMPLETED.getStatus());
        LOG.info("Success exec instance: " + instance.getIdentifier());
      } else {
        instance.setStatus(InstanceStatus.FAILED.getStatus());
        LOG.info("Fail exec instance: " + instance.getIdentifier());
      }
    } catch (Exception e) {
      instance.setStatus(InstanceStatus.FAILED.getStatus());
      LOG.error("Fail exec instance: " + instance.getIdentifier() + ", errMsg: ", e);
    }
    instanceDao.updateStatus(instance);
  }

  public String getThreadIdentifier() {
    return Thread.currentThread().getId() + "." + Thread.currentThread().getName() + "."
        + Thread.currentThread().getState();
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ", instance: " + instance.getIdentifier();
  }
}
