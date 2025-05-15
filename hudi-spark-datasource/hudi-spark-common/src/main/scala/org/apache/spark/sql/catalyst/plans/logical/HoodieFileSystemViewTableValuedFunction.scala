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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

object HoodieFileSystemViewTableValuedFunctionOptionsParser {
  def parseOptions(exprs: Seq[Expression], funcName: String): (String, Map[String, String]) = {
    val args = exprs.map(_.eval().toString)

    if (args.size < 1 || args.size > 2) {
      throw new AnalysisException(s"Expect arguments (table_name or table_path) for function `$funcName`")
    }

    val identifier = args.head
    val subPathOpts = if (args.size == 2) {
      Map(DataSourceReadOptions.FILESYSTEM_RELATION_ARG_SUBPATH.key() -> args(1))
    } else {
      Map.empty[String, String]
    }
    (identifier, Map(DataSourceReadOptions.CREATE_FILESYSTEM_RELATION.key() -> "true") ++ subPathOpts)
  }
}


case class HoodieFileSystemViewTableValuedFunction(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}

object HoodieFileSystemViewTableValuedFunction {

  val FUNC_NAME = "hudi_filesystem_view";

}

case class HoodieFileSystemViewTableValuedFunctionByPath(args: Seq[Expression]) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false

}
