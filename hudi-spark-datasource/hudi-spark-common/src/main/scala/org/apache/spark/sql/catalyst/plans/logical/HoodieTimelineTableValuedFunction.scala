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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.hudi.DataSourceReadOptions

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.hudi.command.exception.HoodieAnalysisException

object HoodieTimelineTableValuedFunctionOptionsParser {
  def parseOptions(exprs: Seq[Expression], funcName: String): (String, Map[String, String]) = {
    val args = exprs.map(_.eval().toString)

    if (args.size < 1 || args.size > 2) {
      throw new HoodieAnalysisException(s"Expect arguments (table_name or table_path, [true|false]) for function `$funcName`")
    }

    val identifier = args.head
    val archivedTimeline = args.drop(1).headOption

    val archivedTimelineOpt = archivedTimeline match {
      case Some(x) => x match {
        case "true" | "TRUE" => Map(DataSourceReadOptions.TIMELINE_RELATION_ARG_ARCHIVED_TIMELINE.key() -> "true")
        case _ => Map(DataSourceReadOptions.TIMELINE_RELATION_ARG_ARCHIVED_TIMELINE.key() -> "false")
      }
      case _ => Map(DataSourceReadOptions.TIMELINE_RELATION_ARG_ARCHIVED_TIMELINE.key() -> "false")
    }

    (identifier, archivedTimelineOpt + (DataSourceReadOptions.CREATE_TIMELINE_RELATION.key() -> "true"))
  }

}


case class HoodieTimelineTableValuedFunction(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieTimelineTableValuedFunction {

  val FUNC_NAME = "hudi_query_timeline";

}

case class HoodieTimelineTableValuedFunctionByPath(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}
