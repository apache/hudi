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

import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.model.{HoodieCommitMetadata, WriteOperationType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * A datasource write resolved by the writer's control plane: the operation, the merged write
 * options, the table, and the deduced writer schema.
 *
 * @param writerSchema schema the rows are written with; empty for deletes, which write no rows.
 * @param keysFromMetaColumns whether the input rows carry their record key and partition path
 *                            in the Hudi meta columns, as prepped writes do.
 * @param locationFromMetaColumns whether the input rows carry their file group in the Hudi meta
 *                                columns, as SQL UPDATE, DELETE and MERGE produce them.
 * @param streaming whether the write is a micro-batch of the streaming sink.
 */
case class DataFrameWriteSpec(operation: WriteOperationType,
                              parameters: Map[String, String],
                              metaClient: HoodieTableMetaClient,
                              writerSchema: Option[HoodieSchema],
                              keysFromMetaColumns: Boolean,
                              locationFromMetaColumns: Boolean,
                              streaming: Boolean)

/**
 * Result of the record write of the DataFrame write path.
 *
 * @param extraCommitMetadata entries the write wants recorded in the commit metadata.
 */
case class DataFrameWriteResult(writeResult: HoodieWriteResult,
                                extraCommitMetadata: Map[String, String] = Map.empty) {

  def addExtraMetadata(commitMetadata: HoodieCommitMetadata): Unit =
    extraCommitMetadata.foreach { case (key, value) => commitMetadata.addMetadata(key, value) }
}

/**
 * The record write of a datasource write as DataFrame transformations: everything between a
 * started instant and the write statuses handed to the commit. The implementation lives in the
 * hudi-spark-dataframe module, which depends on this one, so it is resolved by name.
 */
trait HoodieDataFrameWritePath {

  /** Why the write cannot run on this path, if it cannot. */
  def unsupportedReason(spec: DataFrameWriteSpec): Option[String]

  /** Options the write client must be created with for the path to write the records. */
  def clientOptions(spec: DataFrameWriteSpec): Map[String, String]

  /**
   * Writes the rows of `df` under the already started `instantTime`. The returned write statuses
   * are not committed.
   */
  def write(spec: DataFrameWriteSpec,
            client: SparkRDDWriteClient[_],
            instantTime: String,
            df: DataFrame): DataFrameWriteResult
}

object HoodieDataFrameWritePath {

  private val log = LoggerFactory.getLogger(getClass)

  val IMPLEMENTATION_CLASS = "org.apache.hudi.dataframe.HoodieDataFrameWriter"

  /** Prefix of the log line that reports a write staying on the RDD write path. */
  val FALLBACK_LOG_PREFIX = "DataFrame write path not used: "

  private lazy val implementation: Option[HoodieDataFrameWritePath] = {
    try {
      Some(getClass.getClassLoader.loadClass(IMPLEMENTATION_CLASS)
        .getDeclaredConstructor().newInstance().asInstanceOf[HoodieDataFrameWritePath])
    } catch {
      case _: ClassNotFoundException => None
    }
  }

  def isAvailable: Boolean = implementation.isDefined

  /**
   * The DataFrame write path for the write, when it is enabled and supports the write;
   * otherwise the write stays on the RDD write path.
   */
  def select(spec: DataFrameWriteSpec): Option[HoodieDataFrameWritePath] = {
    val enabled = spec.parameters.getOrElse(DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key,
      DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.defaultValue.toString).toBoolean
    if (!enabled) {
      None
    } else {
      val reason = implementation match {
        case None => Some("the hudi-spark-dataframe module is not on the classpath")
        case Some(path) => path.unsupportedReason(spec)
      }
      reason match {
        case Some(r) =>
          log.warn(s"$FALLBACK_LOG_PREFIX$r")
          None
        case None =>
          log.info(s"DataFrame write path used for ${spec.operation.value}")
          implementation
      }
    }
  }
}
