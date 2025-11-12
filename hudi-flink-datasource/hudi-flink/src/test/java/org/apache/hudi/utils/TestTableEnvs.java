/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TableEnv for test goals.
 */
public class TestTableEnvs {

  public static TableEnvironment getBatchTableEnv() {
    Configuration conf = new Configuration();
    // for batch upsert use cases: current suggestion is to disable these 2 options,
    // from 1.14, flink runtime execution mode has switched from streaming
    // to batch for batch execution mode(before that, both streaming and batch use streaming execution mode),
    // current batch execution mode has these limitations:
    //
    // 1. the keyed stream default to always sort the inputs by key;
    // 2. the batch state-backend requires the inputs sort by state key
    //
    // For our hudi batch pipeline upsert case, we rely on the consuming sequence for index records and data records,
    // the index records must be loaded first before data records for BucketAssignFunction to keep upsert semantics correct,
    // so we suggest disabling these 2 options to use streaming state-backend for batch execution mode
    // to keep the strategy before 1.14.
    conf.setString("execution.sorted-inputs.enabled", "false");
    conf.setString("execution.batch-state-backend.enabled", "false");
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    return StreamTableEnvironment.create(execEnv, settings);
  }
}
