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

import org.apache.spark.sql.connector.read.InputPartition

/**
 * Input partition for the DSv2 read path, representing a contiguous byte range of a
 * single base file. Large files are sub-divided per `spark.sql.files.maxPartitionBytes`
 * so executors can read distinct ranges in parallel; sum of `length` across all
 * partitions produced from one base file equals the file size.
 *
 * @param index           partition index
 * @param baseFilePath    full path to the base (Parquet) file
 * @param start           byte offset within the base file where this split begins
 * @param length          byte length of this split (matches Spark's `PartitionedFile.length`)
 * @param partitionValues partition column values in Spark internal format (e.g. UTF8String),
 *                        ordered to match the requiredPartitionSchema
 */
case class HoodieInputPartition(index: Int,
                                baseFilePath: String,
                                start: Long,
                                length: Long,
                                partitionValues: Array[AnyRef]) extends InputPartition
