/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.execution

import org.apache.hudi.execution.HudiSQLUtils
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY
import org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS
import org.apache.hudi.common.model.WriteOperationType
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.CastSupport
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, Attribute, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * merge command, execute merge operation for hudi
 */
case class InsertIntoHudiTable(
    table: CatalogTable,
    partitions: Map[String, Option[String]],
    query: LogicalPlan, overwrite: Boolean,
    outputs: Seq[Attribute]) extends RunnableCommand with CastSupport with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val db = table.identifier.database.getOrElse("default")
    val tableName = table.identifier.table
    val savePath = HudiSQLUtils.getTablePath(sparkSession, table)
    val dynamicPartCols = partitions.filterNot(_._2.isDefined).keySet

    // add static partition to query
    val actualQuery = if (partitions.exists(_._2.isDefined)) {
      val projectList = addStaticPartitons(query.output, partitions, outputs, table.partitionSchema)
      Project(projectList, query)
    } else {
      query
    }

    val finalSourceDataFrame: DataFrame = Dataset.ofRows(sparkSession, actualQuery).drop(HOODIE_META_COLUMNS.asScala: _*)

    val overwriteType = if (dynamicPartCols.isEmpty) {
      WriteOperationType.INSERT_OVERWRITE
    } else {
      WriteOperationType.INSERT_OVERWRITE_TABLE
    }
    val tablePath = if (savePath.endsWith("/*")) {
      savePath.dropRight(partitions.size*2 + 1)
    } else {
      savePath
    }

    val enableHive = "hive" == sparkSession.sessionState.conf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)
    val newTableOptions = if (enableHive) {
      val extraOptions =  Map("hoodie.table.name" -> table.identifier.table,
        "path" -> tablePath,
        DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> db,
        DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> tableName,
        PARTITIONPATH_FIELD_OPT_KEY -> table.partitionColumnNames.mkString(","),
        DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> table.partitionColumnNames.mkString(","),
        DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      HudiSQLUtils.buildDefaultParameter(extraOptions ++ table.storage.properties ++
        HudiSQLUtils.getPropertiesFromTableConfigCache(tablePath).asScala.toMap, true)
    } else {
      val extraOptions =  Map("hoodie.table.name" -> table.identifier.table,
        "path" -> tablePath,
        PARTITIONPATH_FIELD_OPT_KEY -> table.partitionColumnNames.mkString(","))
      HudiSQLUtils.buildDefaultParameter(extraOptions ++ table.storage.properties ++
        HudiSQLUtils.getPropertiesFromTableConfigCache(tablePath).asScala.toMap, false)
    }
     HudiSQLUtils.update(finalSourceDataFrame, newTableOptions, sparkSession,
       if (overwrite) overwriteType else WriteOperationType.UPSERT, false)

    sparkSession.catalog.refreshTable(table.identifier.unquotedString)
    Seq.empty[Row]
  }

  def addStaticPartitons(
      sourceAttributes: Seq[Attribute],
      providedPartitions: Map[String, Option[String]],
      targetAttributes: Seq[Attribute],
      targetPartitionSchema: StructType): Seq[NamedExpression] = {
    val staticPartitions = providedPartitions.flatMap {
      case (partKey, Some(partValue)) => (partKey, partValue) :: Nil
      case (_, None) => Nil
    }

    val partitionList = targetPartitionSchema.fields.map { field =>
      val potentialSpecs = staticPartitions.filter {
        case (partKey, partValue) => conf.resolver(field.name, partKey)
      }
      if (potentialSpecs.isEmpty) {
        None
      } else if (potentialSpecs.size == 1) {
        val partValue = potentialSpecs.head._2
        conf.storeAssignmentPolicy match {
          // SPARK-30844: try our best to follow StoreAssignmentPolicy for static partition
          // values but not completely follow because we can't do static type checking due to
          // the reason that the parser has erased the type info of static partition values
          // and converted them to string.
          case StoreAssignmentPolicy.ANSI | StoreAssignmentPolicy.STRICT =>
            Some(Alias(AnsiCast(Literal(partValue), field.dataType,
              Option(conf.sessionLocalTimeZone)), field.name)())
          case _ =>
            Some(Alias(cast(Literal(partValue), field.dataType), field.name)())
        }
      } else {
        throw new AnalysisException(
          s"Partition column ${field.name} have multiple values specified, " +
            s"${potentialSpecs.mkString("[", ", ", "]")}. Please only specify a single value.")
      }
    }

    // We first drop all leading static partitions using dropWhile and check if there is
    // any static partition appear after dynamic partitions.
    partitionList.dropWhile(_.isDefined).collectFirst {
      case Some(_) =>
        throw new AnalysisException(
          s"The ordering of partition columns is " +
            s"${targetPartitionSchema.fields.map(_.name).mkString("[", ",", "]")}. " +
            "All partition columns having constant values need to appear before other " +
            "partition columns that do not have an assigned constant value.")
    }
    assert(partitionList.take(staticPartitions.size).forall(_.isDefined))
    val projectList =
      sourceAttributes.take(targetAttributes.size - targetPartitionSchema.fields.size) ++
        partitionList.take(staticPartitions.size).map(_.get) ++
        sourceAttributes.takeRight(targetPartitionSchema.fields.size - staticPartitions.size)
    projectList
  }
}

