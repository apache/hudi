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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField

import scala.collection.JavaConverters._

case class UpdateHoodieTableCommand(updateTable: UpdateTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  private val table = updateTable.table
  private val tableId = getTableIdentifier(table)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logInfo(s"start execute update command for $tableId")
    val sqlConf = sparkSession.sessionState.conf
    val name2UpdateValue = updateTable.assignments.map {
      case Assignment(attr: AttributeReference, value) =>
        attr.name -> value
    }.toMap

    val updateExpressions = table.output
      .map(attr => {
        val UpdateValueOption = name2UpdateValue.find(f => sparkSession.sessionState.conf.resolver(f._1, attr.name))
        if(UpdateValueOption.isEmpty) attr else UpdateValueOption.get._2
      })
      .filter { // filter the meta columns
        case attr: AttributeReference =>
          !HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet.contains(attr.name)
        case _=> true
      }

    val projects = updateExpressions.zip(removeMetaFields(table.schema).fields).map {
      case (attr: AttributeReference, field) =>
        Column(cast(attr, field, sqlConf))
      case (exp, field) =>
        Column(Alias(cast(exp, field, sqlConf), field.name)())
    }

    var df = Dataset.ofRows(sparkSession, table)
    if (updateTable.condition.isDefined) {
      df = df.filter(Column(updateTable.condition.get))
    }
    df = df.select(projects: _*)
    val config = buildHoodieConfig(HoodieCatalogTable(sparkSession, tableId))
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    sparkSession.catalog.refreshTable(tableId.unquotedString)
    logInfo(s"Finish execute update command for $tableId")
    Seq.empty[Row]
  }

  def cast(exp:Expression, field: StructField, sqlConf: SQLConf): Expression = {
    castIfNeeded(exp, field.dataType, sqlConf)
  }
}
