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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Map;

import static org.apache.flink.configuration.PipelineOptions.AUTO_WATERMARK_INTERVAL;

/**
 * Adapter utils for {@code RuntimeContext} to solve API compatibilities issue.
 */
public class RuntimeContextUtils {

  public static JobID getJobId(RuntimeContext runtimeContext) {
    return runtimeContext.getJobInfo().getJobId();
  }

  public static int getAttemptNumber(RuntimeContext runtimeContext) {
    return runtimeContext.getTaskInfo().getAttemptNumber();
  }

  public static int getIndexOfThisSubtask(RuntimeContext runtimeContext) {
    return runtimeContext.getTaskInfo().getIndexOfThisSubtask();
  }

  public static int getMaxNumberOfParallelSubtasks(RuntimeContext runtimeContext) {
    return runtimeContext.getTaskInfo().getMaxNumberOfParallelSubtasks();
  }

  public static int getNumberOfParallelSubtasks(RuntimeContext runtimeContext) {
    return runtimeContext.getTaskInfo().getNumberOfParallelSubtasks();
  }

  public static long getWatermarkInternal(RuntimeContext runtimeContext) {
    Map<String, String> jobParameters = runtimeContext.getGlobalJobParameters();
    return Long.parseLong(jobParameters.getOrDefault(AUTO_WATERMARK_INTERVAL.key(),
        AUTO_WATERMARK_INTERVAL.defaultValue().toMillis() + ""));
  }
}
