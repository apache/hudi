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

package org.apache.hudi.util

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object IncrementalRelationUtil {

  def getPrunedSchema(requiredColumns: Array[String],
                      usedSchema: StructType,
                      metaClient: HoodieTableMetaClient) = {
    var prunedSchema = StructType(Seq())

    // _hoodie_commit_time is a required field. using which query filters are applied.
    if (!requiredColumns.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)) {
      prunedSchema = prunedSchema.add(usedSchema(HoodieRecord.COMMIT_TIME_METADATA_FIELD))
    }

    // Add all the required columns as part of pruned schema
    requiredColumns.foreach(col => {
      val field = usedSchema.find(_.name == col)
      if (field.isDefined) {
        prunedSchema = prunedSchema.add(field.get)
      }
    })

    // All the partition fields are required columns while querying the data.
    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionFields
    if (partitionColumns.isPresent) {
      partitionColumns.get().foreach(col => {
        if (!requiredColumns.contains(col)) {
          val field = usedSchema.find(_.name == col)
          if (field.isDefined) {
            prunedSchema = prunedSchema.add(field.get)
          }
        }
      })
    }
    prunedSchema
  }

  def filterRequiredColumnsFromDF(df: DataFrame,
                                  requiredColumns: Array[String],
                                  metaClient: HoodieTableMetaClient) = {
    var updatedDF = df
    // If _hoodie_commit_time is not part of the required columns remove it from the resultant Dataframe
    if (!requiredColumns.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD)) {
      updatedDF = updatedDF.toDF().drop(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    }

    // Also remove partition fields if they are not part of the required columns
    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionFields
    if (partitionColumns.isPresent) {
      partitionColumns.get().foreach(col => {
        if (!requiredColumns.contains(col)) {
          updatedDF = updatedDF.toDF().drop(col)
        }
      })
    }
    updatedDF
  }
}
