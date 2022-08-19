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

package org.apache.spark.sql.hudi.command.procedures

import com.codahale.metrics.{Histogram, Snapshot, UniformReservoir}
import com.google.common.collect.{Lists, Maps}
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.spark.sql.Row
import org.apache.spark.sql.hudi.command.procedures.StatsFileSizeProcedure.MAX_FILES
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

class StatsFileSizeProcedure extends BaseProcedure with ProcedureBuilder {

  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "partition_path", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10)
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("commit_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("min", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("10th", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("50th", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("avg", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("95th", DataTypes.DoubleType, nullable = true, Metadata.empty),
    StructField("max", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("num_files", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("std_dev", DataTypes.DoubleType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    checkArgs(parameters, args)
    val table = getArgValueOrDefault(args, parameters(0))
    val globRegex = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[String]
    val limit: Int = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[Int]
    val basePath = getBasePath(table)
    val fs = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build.getFs
    val globPath = String.format("%s/%s/*", basePath, globRegex)
    val statuses = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(globPath))

    val globalHistogram = new Histogram(new UniformReservoir(MAX_FILES))
    val commitHistogramMap: java.util.Map[String, Histogram] = Maps.newHashMap()
    statuses.asScala.foreach(
      status => {
        val instantTime = FSUtils.getCommitTime(status.getPath.getName)
        val len = status.getLen
        commitHistogramMap.putIfAbsent(instantTime, new Histogram(new UniformReservoir(MAX_FILES)))
        commitHistogramMap.get(instantTime).update(len)
        globalHistogram.update(len)
      }
    )
    val rows: java.util.List[Row] = Lists.newArrayList()
    commitHistogramMap.asScala.foreach {
      case (instantTime, histogram) =>
        val snapshot = histogram.getSnapshot
        rows.add(printFileSizeHistogram(instantTime, snapshot))
    }
    val snapshot = globalHistogram.getSnapshot
    rows.add(printFileSizeHistogram("ALL", snapshot))
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  def printFileSizeHistogram(instantTime: String, snapshot: Snapshot): Row = {
    Row(
      instantTime,
      snapshot.getMin,
      snapshot.getValue(0.1),
      snapshot.getMedian,
      snapshot.getMean,
      snapshot.get95thPercentile,
      snapshot.getMax,
      snapshot.size,
      snapshot.getStdDev
    )
  }

  override def build: Procedure = new StatsFileSizeProcedure
}

object StatsFileSizeProcedure {
  val MAX_FILES = 1000000
  val NAME = "stats_file_sizes"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new StatsFileSizeProcedure()
  }
}
