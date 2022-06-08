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

package org.apache.hudi.table.management.executor.submitter;

import org.apache.hudi.table.management.entity.Instance;
import org.apache.hudi.table.management.exception.HoodieTableManagementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public abstract class ExecutionEngine {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionEngine.class);

  protected static final String YARN_SUBMITTED = "Submitted application";

  public String execute(String jobName, Instance instance) throws HoodieTableManagementException {
    try {
      LOG.info("Submitting instance {}:{}", jobName, instance.getIdentifier());
      beforeExecuteCommand();
      return executeCommand(jobName, instance);
    } catch (Exception e) {
      throw new HoodieTableManagementException("Failed submit instance " + instance, e);
    }
  }

  protected String executeCommand(String jobName, Instance instance) {
    String command = "";
    try {
      command = getCommand();
      LOG.info("Execute command: {}", command);
      Map<String, String> env = setProcessEnv();
      LOG.info("Execute env: {}", env);

      return "-1";

//      ExecuteHelper executeHelper = new ExecuteHelper(command, jobName, env);
//      CompletableFuture<Void> executeFuture = executeHelper.getExecuteThread();
//      executeFuture.whenComplete((Void ignored, Throwable throwable) -> executeHelper.closeProcess());
//      while (!executeFuture.isDone()) {
//        LOG.info("Waiting for execute job " + jobName);
//        TimeUnit.SECONDS.sleep(5);
//      }
//      if (executeHelper.isSuccess) {
//        LOG.info("Execute job {} command success", jobName);
//      } else {
//        LOG.info("Execute job {} command failed", jobName);
//      }
//      return executeHelper.applicationId;
    } catch (Exception e) {
      LOG.error("Execute command error with exception: ", e);
      throw new HoodieTableManagementException("Execute " + command + " command error", e);
    }
  }

  protected abstract String getCommand() throws IOException;

  protected abstract void beforeExecuteCommand();

  public abstract Map<String, String> setProcessEnv();
}
