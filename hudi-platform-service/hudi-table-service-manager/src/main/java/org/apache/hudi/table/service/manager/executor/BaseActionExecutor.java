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

package org.apache.hudi.table.service.manager.executor;

import org.apache.hudi.table.service.manager.common.HoodieTableServiceManagerConfig;
import org.apache.hudi.table.service.manager.common.ServiceConfig;
import org.apache.hudi.table.service.manager.common.ServiceContext;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.executor.submitter.ExecutionEngine;
import org.apache.hudi.table.service.manager.executor.submitter.SparkEngine;
import org.apache.hudi.table.service.manager.store.impl.InstanceService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class BaseActionExecutor implements Runnable {

  private static final Logger LOG = LogManager.getLogger(BaseActionExecutor.class);

  protected InstanceService instanceDao;
  protected Instance instance;
  protected ExecutionEngine engine;
  protected HoodieTableServiceManagerConfig config;

  public BaseActionExecutor(Instance instance, HoodieTableServiceManagerConfig config) {
    this.instance = instance;
    this.config = config;
    this.instanceDao = ServiceContext.getInstanceDao();
    switch (instance.getExecutionEngine()) {
      case SPARK:
        engine = new SparkEngine(instanceDao, config);
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
      doExecute();
    } finally {
      ServiceContext.removeRunningInstance(instance.getRecordKey());
      if (config.getInstanceCacheEnable()) {
        ServiceContext.removePendingInstant(instance.getRecordKey());
      }
    }
  }

  public abstract void doExecute();

  public abstract String getJobName(Instance instance);

  public String getThreadIdentifier() {
    return Thread.currentThread().getId() + "." + Thread.currentThread().getName() + "."
        + Thread.currentThread().getState();
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ", instance: " + instance.getIdentifier();
  }
}
