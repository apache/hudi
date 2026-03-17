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

import org.apache.hudi.{DataSourceWriteOptions, HoodieEmptyRelation, HoodieSparkSqlWriter}
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * DSv2 data source for Hudi, registered with short name "hudi_v2".
 *
 * Activation via DataFrame API:
 * - Read: `spark.read.format("hudi_v2").load(path)`
 * - Write: `df.write.format("hudi_v2").save(path)`
 *
 * The SQL/Catalog path is handled by [[org.apache.spark.sql.hudi.catalog.HoodieCatalog.loadTable]].
 *
 * Write via DataFrame API is supported through [[CreatableRelationProvider]], which Spark uses
 * as V1 fallback when the data source implements [[TableProvider]] + [[DataSourceRegister]].
 */
class HoodieDataSourceV2 extends TableProvider with DataSourceRegister with CreatableRelationProvider {

  override def shortName(): String = "hudi_v2"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = new StructType()

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) {
      throw new HoodieException("'path' cannot be null, missing 'path' from table properties")
    }

    HoodieSparkV2Table(SparkSession.active, path, options = options)
  }

  override def createRelation(sqlContext: SQLContext,
                               mode: SaveMode,
                               optParams: Map[String, String],
                               df: DataFrame): BaseRelation = {
    try {
      if (optParams.get(DataSourceWriteOptions.OPERATION.key)
        .contains(DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)) {
        HoodieSparkSqlWriter.bootstrap(sqlContext, mode, optParams, df)
      } else {
        val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(sqlContext, mode, optParams, df)
        if (!success) {
          throw new HoodieException("Failed to write to Hudi")
        }
      }
    } finally {
      HoodieSparkSqlWriter.cleanup()
    }

    new HoodieEmptyRelation(sqlContext, df.schema)
  }
}
