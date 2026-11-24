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

package org.apache.spark.sql.comet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileStatusWithMetadata, PartitionDirectory}

object ScanListings {

  /**
   * The partition directories a file scan lists. `selectedPartitions` is an array of them up to
   * Spark 3.5 and a `ScanFileListing` from Spark 4, which this module compiles against alike.
   */
  def partitionDirectories(scan: FileSourceScanExec): Seq[PartitionDirectory] =
    (scan.selectedPartitions: Any) match {
      case directories: Array[_] => directories.toSeq.asInstanceOf[Seq[PartitionDirectory]]
      case listing =>
        val partitions = listing.getClass.getMethod("filePartitionIterator").invoke(listing)
          .asInstanceOf[Iterator[AnyRef]]
        partitions.map { partition =>
          val values = partition.getClass.getMethod("values").invoke(partition)
            .asInstanceOf[InternalRow]
          val files = partition.getClass.getMethod("files").invoke(partition)
            .asInstanceOf[Iterator[FileStatusWithMetadata]].toVector
          PartitionDirectory(values, files)
        }.toVector
    }
}
