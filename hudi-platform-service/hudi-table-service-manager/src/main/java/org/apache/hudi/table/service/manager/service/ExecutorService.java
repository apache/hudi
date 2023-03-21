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

import org.apache.hudi.table.service.manager.common.ServiceContext;
import org.apache.hudi.table.service.manager.executor.BaseActionExecutor;
import org.apache.hudi.table.service.manager.store.MetadataStore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorService implements BaseService {

  private static final Logger LOG = LogManager.getLogger(ExecutorService.class);

  private ThreadPoolExecutor executorService;
  private ScheduledExecutorService service;
  private BlockingQueue<BaseActionExecutor> taskQueue;
  private final MetadataStore metadataStore;

  public ExecutorService(MetadataStore metadataStore) {
    this.metadataStore = metadataStore;
  }

  public void init() {
    service = Executors.newSingleThreadScheduledExecutor();
    int coreExecuteSize = metadataStore.getTableServiceManagerConfig().getScheduleCoreExecuteSize();
    int maxExecuteSize = metadataStore.getTableServiceManagerConfig().getScheduleMaxExecuteSize();
    executorService = new ThreadPoolExecutor(coreExecuteSize, maxExecuteSize, 60,
        TimeUnit.SECONDS, new SynchronousQueue<>());
    taskQueue = new LinkedBlockingQueue<>();
    LOG.info("Init service: " + ExecutorService.class.getName() + ", coreExecuteSize: "
        + coreExecuteSize + ", maxExecuteSize: " + maxExecuteSize);
  }

  @Override
  public void startService() {
    LOG.info("Start service: " + ExecutorService.class.getName());
    service.submit(new ExecutionTask());
  }

  @Override
  public void stop() {
    LOG.info("Stop service: " + ExecutorService.class.getName());
    if (executorService != null && !executorService.isShutdown()) {
      executorService.shutdown();
    }
    if (service != null && service.isShutdown()) {
      service.shutdown();
    }
    LOG.info("Finish stop service: " + ExecutorService.class.getName());
  }

  private class ExecutionTask implements Runnable {

    @Override
    public void run() {
      while (true) {
        try {
          System.out.println("SPARK_HOME = " + System.getenv("SPARK_HOME"));
          BaseActionExecutor executor = taskQueue.take();
          LOG.info("Start execute: " + executor);
          executorService.execute(executor);
        } catch (InterruptedException interruptedException) {
          LOG.error("Occur exception when exec job: " + interruptedException);
        }
      }
    }
  }

  public void submitTask(BaseActionExecutor task) {
    taskQueue.add(task);
  }

  public int getFreeSize() {
    return metadataStore.getTableServiceManagerConfig().getScheduleMaxExecuteSize() - ServiceContext.getRunningInstanceNum();
  }

}
