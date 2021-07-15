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

package org.apache.spark.sql.hudi.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hudi.HoodieSqlUtils.getTableLocation

case class ClusteringHoodieTableCommand(table: CatalogTable,
  orderByColumns: Seq[String], timestamp: Option[Long]) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val basePath = getTableLocation(table, sparkSession)
      .getOrElse(s"missing location for ${table.identifier}")
    val notExistsColumns = orderByColumns.filterNot(table.schema.fieldNames.contains(_))
    assert(notExistsColumns.isEmpty, s"Order by columns:[${notExistsColumns.mkString(",")}] is not exists" +
      s" in table ${table.identifier.unquotedString}.")
    ClusteringHoodiePathCommand(basePath, orderByColumns, timestamp).run(sparkSession)
  }
}
