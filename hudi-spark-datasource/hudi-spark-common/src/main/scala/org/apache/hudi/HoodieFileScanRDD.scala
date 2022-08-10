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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD, PartitionedFile}

/**
 * This is an extension of [[FileScnaRDD]], implementing [[HoodieUnsafeRDD]]
 */
class HoodieFileScanRDD(@transient private val sparkSession: SparkSession,
                        read: PartitionedFile => Iterator[InternalRow],
                        @transient filePartitions: Seq[FilePartition])
  extends FileScanRDD(sparkSession, read, filePartitions)
    with HoodieUnsafeRDD {

  override final def collect(): Array[InternalRow] = super[HoodieUnsafeRDD].collect()
}
