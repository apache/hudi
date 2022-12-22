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
import org.apache.hudi.exception.HoodieCatalogException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateTable}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils._
import org.apache.spark.sql.hudi.ProvidesHoodieConfig

import scala.collection.JavaConverters._

case class UpdateHoodieTableCommand(ut: UpdateTable) extends HoodieLeafRunnableCommand
  with SparkAdapterSupport with ProvidesHoodieConfig {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalogTable = sparkAdapter.resolveHoodieTable(ut.table) match {
      case Some(table) => HoodieCatalogTable(sparkSession, table)
      case _ =>
        throw new HoodieCatalogException("Unable to resolve Hudi table in Delete Command")
    }
    val tableId = catalogTable.table.qualifiedName

    logInfo(s"start execute update command for $tableId")

    val sqlConf = sparkSession.sessionState.conf
    val name2UpdateValue = ut.assignments.map {
      case Assignment(attr: AttributeReference, value) =>
        attr.name -> value
    }.toMap

    val updateExpressions = ut.table.output
      .map(attr => {
        val UpdateValueOption = name2UpdateValue.find(f => sparkSession.sessionState.conf.resolver(f._1, attr.name))
        if(UpdateValueOption.isEmpty) attr else UpdateValueOption.get._2
      })
      .filter { // filter the meta columns
        case attr: AttributeReference =>
          !HoodieRecord.HOODIE_META_COLUMNS.asScala.toSet.contains(attr.name)
        case _=> true
      }

    val projects = updateExpressions.zip(removeMetaFields(ut.table.schema).fields).map {
      case (attr: AttributeReference, field) =>
        Column(castIfNeeded(attr, field.dataType))
      case (exp, field) =>
        Column(Alias(castIfNeeded(exp, field.dataType), field.name)())
    }

    var df = Dataset.ofRows(sparkSession, ut.table)
    if (ut.condition.isDefined) {
      df = df.filter(Column(ut.condition.get))
    }
    df = df.select(projects: _*)
    val config = buildHoodieConfig(catalogTable)
    df.write
      .format("hudi")
      .mode(SaveMode.Append)
      .options(config)
      .save()
    sparkSession.catalog.refreshTable(tableId)
    logInfo(s"Finish execute update command for $tableId")
    Seq.empty[Row]
  }

}
