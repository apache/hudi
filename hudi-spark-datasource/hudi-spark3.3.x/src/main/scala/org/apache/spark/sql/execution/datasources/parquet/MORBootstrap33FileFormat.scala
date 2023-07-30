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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.{HoodieTableSchema, HoodieTableState}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.BootstrapMORIteratorFactory.MORBootstrapFileFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{BootstrapMORIteratorFactory, SparkSession}

class MORBootstrap33FileFormat(shouldAppendPartitionValues: Boolean,
                               tableState: Broadcast[HoodieTableState],
                               tableSchema: Broadcast[HoodieTableSchema],
                               tableName: String,
                               mergeType: String,
                               mandatoryFields: Seq[String],
                               isMOR: Boolean,
                               isBootstrap: Boolean) extends Spark33HoodieParquetFileFormat(shouldAppendPartitionValues) with MORBootstrapFileFormat {

  //Used so that the planner only projects once and does not stack overflow
  var isProjected = false


  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    if (isMOR) {
      false
    } else {
      super.supportBatch(sparkSession, schema)
    }
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val iteratorFactory = new BootstrapMORIteratorFactory(tableState, tableSchema, tableName,
      mergeType, mandatoryFields, isMOR, isBootstrap, this.supportBatch, super.buildReaderWithPartitionValuesInternal)
    iteratorFactory.buildReaderWithPartitionValues(sparkSession, dataSchema, partitionSchema, requiredSchema, filters, options, hadoopConf)
  }
}

