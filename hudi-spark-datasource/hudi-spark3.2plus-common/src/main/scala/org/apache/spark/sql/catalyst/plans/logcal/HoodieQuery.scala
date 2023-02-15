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

package org.apache.spark.sql.catalyst.plans.logcal

import org.apache.hudi.DefaultSource
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import scala.collection.mutable


case class HoodieQuery(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieQuery {

  val FUNC_NAME = "hudi_query";

  def resolve(spark: SparkSession, func: HoodieQuery): LogicalPlan = {

    val args = func.args

    val identifier = spark.sessionState.sqlParser.parseTableIdentifier(args.head.eval().toString)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(identifier)

    val options = mutable.Map("path" -> catalogTable.location.toString) ++ parseOptions(args.tail)

    val hoodieDataSource = new DefaultSource
    val relation = hoodieDataSource.createRelation(spark.sqlContext, options.toMap)
    new LogicalRelation(
      relation,
      relation.schema.toAttributes,
      Some(catalogTable),
      false
    )
  }

  private def parseOptions(args: Seq[Expression]): Map[String, String] = {
    val options = mutable.Map.empty[String, String]
    val queryMode = args.head.eval().toString
    val instants = args.tail.map(_.eval().toString)
    queryMode match {
      case "read_optimized" =>
        assert(instants.isEmpty, "No expressions have to be provided in read_optimized mode.")
        options += ("hoodie.datasource.query.type" -> "read_optimized")
      case _ =>
        throw new AnalysisException("hudi_query doesn't support other query modes for now.")
    }
    options.toMap
  }
}
