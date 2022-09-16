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

package org.apache.spark.sql.adapter

import org.apache.avro.Schema
import org.apache.hadoop.fs.Path
import org.apache.hudi.{AvroConversionUtils, DefaultSource, Spark3RowSerDe}
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedPredicate, Predicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.{HoodieCatalystPlansUtils, HoodieSpark3CatalystPlanUtils, Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.{DISK_ONLY, DISK_ONLY_2, DISK_ONLY_3, MEMORY_AND_DISK, MEMORY_AND_DISK_2, MEMORY_AND_DISK_SER, MEMORY_AND_DISK_SER_2, MEMORY_ONLY, MEMORY_ONLY_2, MEMORY_ONLY_SER, MEMORY_ONLY_SER_2, NONE, OFF_HEAP}

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.util.control.NonFatal

/**
 * Base implementation of [[SparkAdapter]] for Spark 3.x branch
 */
abstract class BaseSpark3Adapter extends SparkAdapter with Logging {

  override def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    new Spark3RowSerDe(encoder)
  }

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil = {
    new Spark3ParsePartitionUtil(conf)
  }

  override def parseMultipartIdentifier(parser: ParserInterface, sqlText: String): Seq[String] = {
    parser.parseMultipartIdentifier(sqlText)
  }

  /**
   * Combine [[PartitionedFile]] to [[FilePartition]] according to `maxSplitBytes`.
   */
  override def getFilePartitions(
      sparkSession: SparkSession,
      partitionedFiles: Seq[PartitionedFile],
      maxSplitBytes: Long): Seq[FilePartition] = {
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
  }

  override def isHoodieTable(table: LogicalPlan, spark: SparkSession): Boolean = {
    unfoldSubqueryAliases(table) match {
      case LogicalRelation(_, _, Some(table), _) => isHoodieTable(table)
      case relation: UnresolvedRelation =>
        try {
          isHoodieTable(getCatalystPlanUtils.toTableIdentifier(relation), spark)
        } catch {
          case NonFatal(e) =>
            logWarning("Failed to determine whether the table is a hoodie table", e)
            false
        }
      case DataSourceV2Relation(table: Table, _, _, _, _) => isHoodieTable(table.properties())
      case _=> false
    }
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }

  override def createRelation(metaClient: HoodieTableMetaClient,
                              sqlContext: SQLContext,
                              schema: Schema,
                              globPaths: Array[Path],
                              parameters: util.Map[String, String]): BaseRelation = {
    val dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    DefaultSource.createRelation(metaClient, sqlContext, dataSchema, globPaths, parameters.asScala.toMap)
  }

  /**
   * Converts instance of [[StorageLevel]] to a corresponding string
   */
  override def convertStorageLevelToString(level: StorageLevel): String = level match {
    case NONE => "NONE"
    case DISK_ONLY => "DISK_ONLY"
    case DISK_ONLY_2 => "DISK_ONLY_2"
    case DISK_ONLY_3 => "DISK_ONLY_3"
    case MEMORY_ONLY => "MEMORY_ONLY"
    case MEMORY_ONLY_2 => "MEMORY_ONLY_2"
    case MEMORY_ONLY_SER => "MEMORY_ONLY_SER"
    case MEMORY_ONLY_SER_2 => "MEMORY_ONLY_SER_2"
    case MEMORY_AND_DISK => "MEMORY_AND_DISK"
    case MEMORY_AND_DISK_2 => "MEMORY_AND_DISK_2"
    case MEMORY_AND_DISK_SER => "MEMORY_AND_DISK_SER"
    case MEMORY_AND_DISK_SER_2 => "MEMORY_AND_DISK_SER_2"
    case OFF_HEAP => "OFF_HEAP"
    case _ => throw new IllegalArgumentException(s"Invalid StorageLevel: $level")
  }
}
