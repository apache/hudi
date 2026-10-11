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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionDirectory

import scala.util.Try

/**
 * Compatibility helper for Databricks Spark runtime.
 */
object DatabricksRuntimeHelper {
  private lazy val fileStatusWithMetadataClass: Option[Class[_]] =
    Try(Class.forName("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata")).toOption
  private lazy val fileStatusWithMetadataModuleClass =
    Try(Class.forName("org.apache.spark.sql.execution.datasources.FileStatusWithMetadata$")).toOption
  private lazy val fileStatusWithMetadataModule = fileStatusWithMetadataModuleClass.map { cls =>
    cls.getField("MODULE$").get(null) // the singleton companion object instance
  }
  private lazy val fileStatusWithMetadataInstantiationMethod = fileStatusWithMetadataClass.flatMap { cls =>
    Try(cls.getMethod("apply", classOf[FileStatus])).toOption
  }

  private lazy val partitionDirectoryConstructor: Option[java.lang.reflect.Constructor[_]] =
    fileStatusWithMetadataClass.flatMap { _ =>
      Try(classOf[PartitionDirectory].getConstructor(classOf[InternalRow], classOf[Seq[_]])).toOption
    }

  /**
   * Creates a PartitionDirectory, using reflection on Databricks runtime where
   * PartitionDirectory expects (InternalRow, Seq[FileStatusWithMetadata]) instead of
   * (InternalRow, Seq[FileStatus]).
   *
   * Databricks Spark 3.4 runtime backports FileStatusWithMetadata from Spark 3.5.
   * On Databricks Spark 3.4, PartitionDirectory expects FileStatusWithMetadata
   * instead of plain FileStatus.
   *
   * FileStatusWithMetadata does NOT extend FileStatus (it wraps it via composition),
   * so individual element casts are impossible. We use reflection to construct it.
   *
   * On Databricks, constructs FileStatusWithMetadata.apply(fileStatus) for each file
   * status, then reflectively constructs PartitionDirectory.
   * On standard Spark, falls back to normal PartitionDirectory construction.
   */
  def newPartitionDirectory(internalRow: InternalRow,
                            statuses: Seq[FileStatus],
                            fallback: (InternalRow, Seq[FileStatus]) => PartitionDirectory): PartitionDirectory = {
    (fileStatusWithMetadataInstantiationMethod, fileStatusWithMetadataModule, partitionDirectoryConstructor) match {
      case (Some(method), Some(module), Some(ctor)) =>
        val wrappedStatuses = statuses.map(e => method.invoke(module, e))
        ctor.newInstance(internalRow, wrappedStatuses).asInstanceOf[PartitionDirectory]
      case _ =>
        fallback(internalRow, statuses)
    }
  }
}
