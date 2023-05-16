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

import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

case class HoodieTableChanges(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieTableChanges {

  val FUNC_NAME = "hudi_table_changes";

  def parseOptions(exprs: Seq[Expression]): (String, Map[String, String]) = {
    val args = exprs.map(_.eval().toString)

    if (args.size < 3) {
      throw new AnalysisException(s"Too few arguments for function `$FUNC_NAME`")
    } else if (args.size > 4) {
      throw new AnalysisException(s"Too many arguments for function `$FUNC_NAME`")
    }

    val identifier = args.head
    val incrementalQueryFormat = args(1)
    val startInstantTime = args(2)
    val endInstantTimeOpt = args.drop(3).headOption.map(x => "hoodie.datasource.read.end.instanttime" -> x)

    val incrementalQueryFormatOpt = incrementalQueryFormat match {
      case "latest_state" | "cdc" => Map("hoodie.datasource.query.incremental.format" -> incrementalQueryFormat)
      case _ =>
        throw new AnalysisException(s"'hudi_table_changes' doesn't support `$incrementalQueryFormat`")
    }

    val startInstantTimeOpt = startInstantTime match {
      case "earliest" => Map.empty[String, String]
      case _ => Map("hoodie.datasource.read.start.instanttime" -> startInstantTime)
    }

    val opts: Map[String, String] = incrementalQueryFormatOpt ++ startInstantTimeOpt ++ endInstantTimeOpt

    (identifier, opts)
  }
}
