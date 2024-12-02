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

package org.apache.hudi

import org.apache.hudi.common.model.{FileSlice, HoodieLogFile}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.util.JFunction

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.PartitionDirectory
import org.apache.spark.sql.types.StructType

import java.util.stream.Collectors

import scala.collection.JavaConverters._

class HoodieFixedFileIndex(metaClient: HoodieTableMetaClient,
                           fileSlices: Map[InternalRow, Seq[FileSlice]],
                           schema: StructType,
                           dSchema: StructType,
                           pSchema: StructType) extends HoodieSparkFileIndex with SparkAdapterSupport {

  override def rootPaths: Seq[Path] = Seq(new Path(metaClient.getBasePath.toUri))

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    assert(partitionFilters.isEmpty)
    assert(dataFilters.isEmpty)
    fileSlices.map(u => HoodieFileIndex.slicesToPartitionDirectory(u._1, u._2, sparkAdapter.getSparkPartitionedFileUtils)).toSeq
  }

  override def inputFiles: Array[String] = {
    fileSlices.values.toStream
      .flatMap(f => f.toStream)
      .flatMap(f => {
        val logFilesStatus = f.getLogFiles.map[String](JFunction.toJavaFunction[HoodieLogFile, String](lf => lf.getPath.toString))
        val files = logFilesStatus.collect(Collectors.toList[String]).asScala
        if (f.getBaseFile.isPresent) {
          files.append(f.getBaseFile.get().getPath)
        }
        files
      }).toArray
  }

  override def refresh(): Unit = {
  }

  override def sizeInBytes: Long = {
    fileSlices.values.toStream
      .flatMap(f => f.toStream)
      .map(f => BaseHoodieTableFileIndex.fileSliceSize(f))
      .sum
  }

  override def partitionSchema: StructType = pSchema

  override def dataSchema: StructType = dSchema

  def getSchema: StructType = schema
}
