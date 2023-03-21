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
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.entity.InstanceStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CompactionExecutor extends BaseActionExecutor {

  private static final Logger LOG = LogManager.getLogger(CompactionExecutor.class);

  public static final String COMPACT_JOB_NAME = "Hoodie compact %s.%s %s";

  public CompactionExecutor(Instance instance, HoodieTableServiceManagerConfig config) {
    super(instance, config);
  }

  @Override
  public void doExecute() {
    String jobName = getJobName(instance);
    LOG.info("Start exec : " + jobName);
    instance.setStatus(InstanceStatus.RUNNING.getStatus());
    instanceDao.updateStatus(instance);
    engine.execute(jobName, instance);
  }

  @Override
  public String getJobName(Instance instance) {
    return String.format(COMPACT_JOB_NAME, instance.getDbName(), instance.getTableName(),
        instance.getInstant());
  }
}
