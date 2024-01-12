/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

object SparkConfigs {

  // spark data source write pool name. Incase of streaming sink, users might be interested to set custom scheduling configs
  // for regular writes and async compaction. In such cases, this pool name will be used for spark datasource writes.
  val SPARK_DATASOURCE_WRITER_POOL_NAME = "sparkdatasourcewrite"

  /*
  When async compaction is enabled (Hudi Streamer or streaming sink), users might be interested to set custom
  scheduling configs for regular writes and async table services like compaction and clustering. This is the property
  used to set custom scheduler config file with spark. In Hudi Streamer, the file is generated within hudi and set if
  necessary. Where as in case of streaming sink, users have to set this property when they invoke spark shell.
  Sample format of the file contents.
  <?xml version="1.0"?>
  <allocations>
    <pool name="sparkdatasourcewrite">
      <schedulingMode>FAIR</schedulingMode>
      <weight>4</weight>
      <minShare>2</minShare>
    </pool>
    <pool name="hoodiecompact">
      <schedulingMode>FAIR</schedulingMode>
      <weight>3</weight>
      <minShare>1</minShare>
    </pool>
    <pool name="hoodiecluster">
      <schedulingMode>FAIR</schedulingMode>
      <weight>2</weight>
      <minShare>1</minShare>
    </pool>
  </allocations>
   */
  val SPARK_SCHEDULER_ALLOCATION_FILE_KEY = "spark.scheduler.allocation.file"

}
