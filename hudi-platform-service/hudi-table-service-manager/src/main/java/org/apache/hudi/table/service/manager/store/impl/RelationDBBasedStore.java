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

package org.apache.hudi.table.service.manager.store.impl;

import org.apache.hudi.table.service.manager.common.HoodieTableServiceManagerConfig;
import org.apache.hudi.table.service.manager.common.ServiceContext;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.store.MetadataStore;

import java.util.List;

public class RelationDBBasedStore implements MetadataStore {

  private final InstanceService instanceDao;
  private final HoodieTableServiceManagerConfig config;

  public RelationDBBasedStore(HoodieTableServiceManagerConfig config) {
    this.config = config;
    this.instanceDao = ServiceContext.getInstanceDao();
  }

  public HoodieTableServiceManagerConfig getTableServiceManagerConfig() {
    return config;
  }

  @Override
  public void saveInstance(Instance instance) {
    instanceDao.saveInstance(instance);
  }

  @Override
  public void updateStatus(Instance instance) {
    instanceDao.updateStatus(instance);
  }

  @Override
  public void init() {
    instanceDao.createInstance();
  }

  @Override
  public Instance getInstance(Instance instance) {
    return instanceDao.getInstance(instance);
  }

  @Override
  public List<Instance> getInstances(int status, int limit) {
    return instanceDao.getInstances(status, limit);
  }

  @Override
  public List<Instance> getRetryInstances() {
    return instanceDao.getRetryInstances(config.getInstanceMaxRetryNum());
  }
}
