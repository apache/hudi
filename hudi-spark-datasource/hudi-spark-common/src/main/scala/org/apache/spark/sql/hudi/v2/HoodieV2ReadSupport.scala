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

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.util.SparkConfigUtils

import org.apache.spark.sql.SparkSession

/**
 * Supportability gate for the DSv2 read path.
 *
 * The scan in [[HoodieScanBuilder]] is base-file-only, Parquet-only, single-format, and
 * does not implement incremental / CDC / bootstrap. Schema evolution is supported by
 * threading the internal schema from the commit timeline into the columnar reader.
 */
object HoodieV2ReadSupport {

  def isSupportedByDSv2(metaClient: HoodieTableMetaClient,
                        options: Map[String, String],
                        spark: SparkSession): Boolean = {
    val tableConfig = metaClient.getTableConfig
    val queryType = SparkConfigUtils.getStringWithAltKeys(options, DataSourceReadOptions.QUERY_TYPE)
    val incrementalFormat = options.getOrElse(
      DataSourceReadOptions.INCREMENTAL_FORMAT.key,
      DataSourceReadOptions.INCREMENTAL_FORMAT.defaultValue)

    val isReadOptimized = queryType == DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL
    val isIncremental = queryType == DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    val isCdc = incrementalFormat == DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL
    val isMor = metaClient.getTableType == MERGE_ON_READ

    // Comment #1: MOR snapshot needs log-file merging; only COW or read_optimized are base-file-only.
    val baseFileOnlySemantics = !isMor || isReadOptimized

    // Comment #2: the scan hard-codes the Parquet reader.
    val isParquetOnly =
      !tableConfig.isMultipleBaseFileFormatsEnabled &&
        tableConfig.getBaseFileFormat == HoodieFileFormat.PARQUET

    val notIncrementalOrCdc = !isIncremental && !isCdc
    val notBootstrap = !tableConfig.getBootstrapBasePath.isPresent

    baseFileOnlySemantics && isParquetOnly && notIncrementalOrCdc && notBootstrap
  }
}
