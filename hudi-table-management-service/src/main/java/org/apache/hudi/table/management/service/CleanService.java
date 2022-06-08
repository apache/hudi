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

import org.apache.hudi.table.management.common.ServiceContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CleanService implements BaseService {

  private static final Logger LOG = LoggerFactory.getLogger(CleanService.class);
  private ScheduledExecutorService service;
  private long cacheInterval = 3600 * 1000; //ms

  @Override
  public void init() {
    LOG.info("Init service: " + CleanService.class.getName());
    //ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("Clean-Service-%d").build();
    this.service = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void startService() {
    LOG.info("Start service: " + CleanService.class.getName());
    service.scheduleAtFixedRate(new RetryRunnable(), 30, 300, TimeUnit.SECONDS);
  }

  @Override
  public void stop() {
    LOG.info("Stop service: " + CleanService.class.getName());
    if (service != null && !service.isShutdown()) {
      service.shutdown();
    }
  }

  private class RetryRunnable implements Runnable {

    @Override
    public void run() {
      cleanCache();
    }
  }

  private void cleanCache() {
    long currentTime = System.currentTimeMillis();
    ConcurrentHashMap<String, Long> pendingInstances = ServiceContext.getPendingInstances();
    for (Map.Entry<String, Long> instance : pendingInstances.entrySet()) {
      if (currentTime - instance.getValue() > cacheInterval) {
        LOG.info("Instance has expired: " + instance.getKey());
        pendingInstances.remove(instance.getKey());
      }
    }
  }

}
