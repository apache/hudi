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

import org.apache.hudi.common.util.ValidationUtils.checkState

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

case class HoodieQuery(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieQuery {

  val FUNC_NAME = "hudi_query";

  def parseOptions(exprs: Seq[Expression]): (String, Map[String, String]) = {
    val args = exprs.map(_.eval().toString)

    val (Seq(identifier, queryMode), remaining) = args.splitAt(2)

    val opts = queryMode match {
      case "read_optimized" | "snapshot" =>
        checkState(remaining.isEmpty, s"No additional args are expected in `$queryMode` mode")
        Map("hoodie.datasource.query.type" -> queryMode)

      case _ =>
        throw new AnalysisException(s"'hudi_query' doesn't currently support `$queryMode`")
    }

    (identifier, opts)
  }
}
