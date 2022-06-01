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

import org.apache.hudi.Spark3RowSerDe
import org.apache.hudi.client.utils.SparkRowSerDe
import org.apache.spark.SPARK_VERSION
import org.apache.hudi.spark3.internal.ReflectUtil
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Expression, InterpretedPredicate, Like, Predicate}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Base implementation of [[SparkAdapter]] for Spark 3.x branch
 */
abstract class BaseSpark3Adapter extends SparkAdapter {

  override def createSparkRowSerDe(encoder: ExpressionEncoder[Row]): SparkRowSerDe = {
    new Spark3RowSerDe(encoder)
  }

  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters

  override def toTableIdentifier(aliasId: AliasIdentifier): TableIdentifier = {
    aliasId match {
      case AliasIdentifier(name, Seq(database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq(_, database)) =>
        TableIdentifier(name, Some(database))
      case AliasIdentifier(name, Seq()) =>
        TableIdentifier(name, None)
      case _=> throw new IllegalArgumentException(s"Cannot cast $aliasId to TableIdentifier")
    }
  }

  override def toTableIdentifier(relation: UnresolvedRelation): TableIdentifier = {
    relation.multipartIdentifier.asTableIdentifier
  }

  override def createJoin(left: LogicalPlan, right: LogicalPlan, joinType: JoinType): Join = {
    Join(left, right, joinType, None, JoinHint.NONE)
  }

  override def isInsertInto(plan: LogicalPlan): Boolean = {
    plan.isInstanceOf[InsertIntoStatement]
  }

  override def getInsertIntoChildren(plan: LogicalPlan):
    Option[(LogicalPlan, Map[String, Option[String]], LogicalPlan, Boolean, Boolean)] = {
    plan match {
      case insert: InsertIntoStatement =>
        Some((insert.table, insert.partitionSpec, insert.query, insert.overwrite, insert.ifPartitionNotExists))
      case _ =>
        None
    }
  }

  override def createInsertInto(table: LogicalPlan, partition: Map[String, Option[String]],
      query: LogicalPlan, overwrite: Boolean, ifPartitionNotExists: Boolean): LogicalPlan = {
    ReflectUtil.createInsertInto(table, partition, Seq.empty[String], query, overwrite, ifPartitionNotExists)
  }

  override def createSparkParsePartitionUtil(conf: SQLConf): SparkParsePartitionUtil = {
    new Spark3ParsePartitionUtil(conf)
  }

  override def createLike(left: Expression, right: Expression): Expression = {
    new Like(left, right)
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
    tripAlias(table) match {
      case LogicalRelation(_, _, Some(tbl), _) => isHoodieTable(tbl)
      case relation: UnresolvedRelation =>
        isHoodieTable(toTableIdentifier(relation), spark)
      case DataSourceV2Relation(table: Table, _, _, _, _) => isHoodieTable(table.properties())
      case _=> false
    }
  }

  /**
   * if the logical plan is a TimeTravelRelation LogicalPlan.
   */
  override def isRelationTimeTravel(plan: LogicalPlan): Boolean = {
    false
  }

  /**
   * Get the member of the TimeTravelRelation LogicalPlan.
   */
  override def getRelationTimeTravel(plan: LogicalPlan): Option[(LogicalPlan, Option[Expression], Option[String])] = {
    throw new IllegalStateException(s"Should not call getRelationTimeTravel for spark3.1.x")
  }
  override def createExtendedSparkParser: Option[(SparkSession, ParserInterface) => ParserInterface] = {
    // since spark3.2.1 support datasourceV2, so we need to a new SqlParser to deal DDL statment
    if (SPARK_VERSION.startsWith("3.1")) {
      val loadClassName = "org.apache.spark.sql.parser.HoodieSpark312ExtendedSqlParser"
      Some {
        (spark: SparkSession, delegate: ParserInterface) => {
          val clazz = Class.forName(loadClassName, true, Thread.currentThread().getContextClassLoader)
          val ctor = clazz.getConstructors.head
          ctor.newInstance(spark, delegate).asInstanceOf[ParserInterface]
        }
      }
    } else {
      None
    }
  }

  override def createInterpretedPredicate(e: Expression): InterpretedPredicate = {
    Predicate.createInterpreted(e)
  }
}
