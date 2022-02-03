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

package org.apache.hudi;

import org.apache.hudi.async.AsyncCompactService;

/**
 * Utility Class to hold spark scheduler configs.
 */
public class SparkSchedulerConfigs {

  public static final String COMPACT_POOL_NAME = AsyncCompactService.COMPACT_POOL_NAME;
  public static final String SPARK_SCHEDULER_MODE_KEY = "spark.scheduler.mode";
  public static final String SPARK_SCHEDULER_FAIR_MODE = "FAIR";
  public static final String SPARK_SCHEDULER_ALLOCATION_FILE_KEY = "spark.scheduler.allocation.file";

  public static final String SPARK_SCHEDULING_PATTERN =
      "<?xml version=\"1.0\"?>\n<allocations>\n  <pool name=\"%s\">\n"
          + "    <schedulingMode>%s</schedulingMode>\n    <weight>%s</weight>\n    <minShare>%s</minShare>\n"
          + "  </pool>\n  <pool name=\"%s\">\n    <schedulingMode>%s</schedulingMode>\n"
          + "    <weight>%s</weight>\n    <minShare>%s</minShare>\n  </pool>\n</allocations>";
}
