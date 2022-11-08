/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.config.HoodieCommonConfig

/**
 * running all the UT TestHoodieFileIndex test in cluster parallelism mode
 */
class TestHoodieFileIndexClusterParallelLoad extends TestHoodieFileIndex {

  queryOpts = Map(
    DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
    HoodieCommonConfig.TABLE_LOADING_MODE.key -> "CLUSTER",
    HoodieCommonConfig.TABLE_LOADING_PARALLELISM.key -> "10"
  )
}


