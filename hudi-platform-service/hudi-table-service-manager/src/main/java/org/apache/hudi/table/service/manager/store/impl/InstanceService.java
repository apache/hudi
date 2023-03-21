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

import org.apache.hudi.table.service.manager.common.ServiceContext;
import org.apache.hudi.table.service.manager.entity.AssistQueryEntity;
import org.apache.hudi.table.service.manager.entity.Instance;
import org.apache.hudi.table.service.manager.entity.InstanceStatus;

import org.apache.hudi.table.service.manager.store.jdbc.JdbcMapper;

import org.apache.ibatis.session.RowBounds;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class InstanceService {

  private static Logger LOG = LogManager.getLogger(InstanceService.class);

  private JdbcMapper jdbcMapper = ServiceContext.getJdbcMapper();

  private static final String NAMESPACE = "Instance";

  public void createInstance() {
    try {
      jdbcMapper.updateObject(statement(NAMESPACE, "createInstance"), null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void saveInstance(Instance instance) {
    try {
      jdbcMapper.saveObject(statement(NAMESPACE, "saveInstance"), instance);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void updateStatus(Instance instance) {
    try {
      int ret = jdbcMapper.updateObject(statement(NAMESPACE, getUpdateStatusSqlId(instance)), instance);
      if (ret != 1) {
        LOG.error("Fail update status instance: " + instance);
        throw new RuntimeException("Fail update status instance: " + instance.getIdentifier());
      }
      LOG.info("Success update status instance: " + instance.getIdentifier());
    } catch (Exception e) {
      LOG.error("Fail update status, instance: " + instance.getIdentifier() + ", errMsg: ", e);
      throw new RuntimeException(e);
    }
  }

  public void updateExecutionInfo(Instance instance) {
    int retryNum = 0;
    try {
      while (retryNum++ < 3) {
        int ret = jdbcMapper.updateObject(statement(NAMESPACE, "updateExecutionInfo"), instance);
        if (ret != 1) {
          LOG.warn("Fail update execution info instance: " + instance);
          TimeUnit.SECONDS.sleep(5);
        } else {
          LOG.info("Success update execution info, instance: " + instance.getIdentifier());
          return;
        }
      }
      throw new RuntimeException("Fail update execution info: " + instance.getIdentifier());
    } catch (Exception e) {
      LOG.error("Fail update status, instance: " + instance.getIdentifier() + ", errMsg: ", e);
      throw new RuntimeException(e);
    }
  }

  public Instance getInstance(Instance instance) {
    try {
      return jdbcMapper.getObject(statement(NAMESPACE, "getInstance"), instance);
    } catch (Exception e) {
      LOG.error("Fail get Instance: " + instance.getIdentifier() + ", errMsg: ", e);
      throw new RuntimeException(e);
    }
  }

  private String getUpdateStatusSqlId(Instance instance) {
    switch (InstanceStatus.getInstance(instance.getStatus())) {
      case SCHEDULED:
        return "retryInstance";
      case RUNNING:
        return "runningInstance";
      case COMPLETED:
        return "successInstance";
      case FAILED:
        return "failInstance";
      case INVALID:
        return "invalidInstance";
      default:
        throw new RuntimeException("Invalid instance: " + instance.getIdentifier());
    }
  }

  public List<Instance> getInstances(int status, int limit) {
    try {
      if (limit > 0) {
        return jdbcMapper.getObjects(statement(NAMESPACE, "getInstances"), status,
            new RowBounds(0, limit));
      } else {
        return jdbcMapper.getObjects(statement(NAMESPACE, "getInstances"), status);
      }
    } catch (Exception e) {
      LOG.error("Fail get instances, status: " + status + ", errMsg: ", e);
      throw new RuntimeException("Fail get instances, status: " + status);
    }
  }

  public List<Instance> getRetryInstances(int maxRetry) {
    try {
      return jdbcMapper.getObjects(statement(NAMESPACE, "getRetryInstances"),
          new AssistQueryEntity(maxRetry));
    } catch (Exception e) {
      LOG.error("Fail get retry instances, errMsg: ", e);
      throw new RuntimeException("Fail get retry instances");
    }
  }

  public List<Instance> getInstanceAfterTime(AssistQueryEntity queryEntity) {
    try {
      return jdbcMapper.getObjects(statement(NAMESPACE, "getInstanceAfterTime"), queryEntity);
    } catch (Exception e) {
      LOG.error("Fail get instances after time, errMsg: ", e);
      throw new RuntimeException("Fail get alert instances");
    }
  }

  private String statement(String namespace, String sqlID) {
    return namespace + "." + sqlID;
  }
}