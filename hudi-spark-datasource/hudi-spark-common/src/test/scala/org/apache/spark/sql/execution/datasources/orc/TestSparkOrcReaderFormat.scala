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

package org.apache.spark.sql.execution.datasources.orc

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * Class used to test [[SparkColumnarFileReader]]
 * This class should have the same functionality as [[OrcFileFormat]]
 */
class TestSparkOrcReaderFormat extends OrcFileFormat with SparkAdapterSupport {

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    //reader must be created outsize of the lambda. This happens on the driver
    val reader = sparkAdapter.createOrcFileReader(supportBatch(sparkSession,
      StructType(partitionSchema.fields ++ requiredSchema.fields)),
      sparkSession.sqlContext.conf, options, hadoopConf, dataSchema)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      //code inside the lambda will run on the executor
      reader.read(file, requiredSchema, partitionSchema, util.Option.empty(), filters,
        HadoopFSUtils.getStorageConf(broadcastedHadoopConf.value.value))
    }
  }
}
