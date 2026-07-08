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

package org.apache.hudi.integ2.testcontainers.service;

import org.apache.hudi.integ2.testcontainers.ContainerProvider;
import org.apache.hudi.integ2.testcontainers.TestcontainersConfig;
import org.apache.hudi.integ2.testcontainers.command.CommandExecutor;
import org.apache.hudi.integ2.testcontainers.command.CommandResult;

import java.util.ArrayList;
import java.util.List;

/**
 * A service wrapper for the Hive container.
 * This class is responsible for all interactions with the Hive service,
 * including executing commands and managing files.
 */
public class HiveService {

  private final CommandExecutor executor;
  private final boolean verbose;

  public HiveService(ContainerProvider provider) {
    this(provider, Boolean.getBoolean(TestcontainersConfig.SystemProps.HIVE_VERBOSE));
  }

  /**
   * Visible-for-tests overload so callers can toggle verbose mode without
   * setting the system property at JVM start time.
   */
  public HiveService(ContainerProvider provider, boolean verbose) {
    this.executor = new CommandExecutor(provider.getContainer(TestcontainersConfig.Containers.HIVESERVER));
    this.verbose = verbose;
  }

  /**
   * Execute a Hive command and return the result.
   */
  public CommandResult execute(String hiveCommand) throws Exception {
    List<String> hiveCmd = new ArrayList<>();
    hiveCmd.add("hive");
    hiveCmd.add("--hiveconf");
    hiveCmd.add("hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
    hiveCmd.add("--hiveconf");
    hiveCmd.add("hive.stats.autogather=false");
    if (verbose) {
      for (String kv : TestcontainersConfig.SystemProps.VERBOSE_HIVECONFS) {
        hiveCmd.add("--hiveconf");
        hiveCmd.add(kv);
      }
    }
    hiveCmd.add("-e");
    hiveCmd.add(hiveCommand);
    return executor.executeCommand(hiveCmd.toArray(new String[0]));
  }
}
