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

package org.apache.spark.sql.execution.datasources
import java.util.TimeZone

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.util.{DateFormatter, TimestampFormatter}
import org.apache.spark.sql.execution.datasources.PartitioningUtils.{PartitionValues, timestampPartitionPattern}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType

class Spark3ParsePartitionUtil(conf: SQLConf) extends SparkParsePartitionUtil {

  override def parsePartition(path: Path, typeInference: Boolean,
                              basePaths: Set[Path], userSpecifiedDataTypes: Map[String, DataType],
                              timeZone: TimeZone): Option[PartitionValues] = {
    val dateFormatter = DateFormatter(timeZone.toZoneId)
    val timestampFormatter = TimestampFormatter(timestampPartitionPattern,
      timeZone.toZoneId, isParsing = true)

    PartitioningUtils.parsePartition(path, typeInference, basePaths, userSpecifiedDataTypes,
      conf.validatePartitionColumns, timeZone.toZoneId, dateFormatter, timestampFormatter)._1
  }
}
