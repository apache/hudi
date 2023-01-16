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

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions.mapAsScalaMap

/**
 * Command for show hudi table's properties.
 */
case class ShowHoodieTablePropertiesCommand(
    tableIdentifier: TableIdentifier,
    propertyKey: Option[String])
  extends HoodieLeafRunnableCommand {

  override val output: Seq[Attribute] = {
    val schema = AttributeReference("value", StringType, nullable = false)() :: Nil
    propertyKey match {
      case None => AttributeReference("key", StringType, nullable = false)() :: schema
      case _ => schema
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!sparkAdapter.isHoodieTable(tableIdentifier, sparkSession)) {
      Seq.empty[Row]
    } else {
      val hoodieCatalogTable = HoodieCatalogTable(sparkSession, tableIdentifier)
      val tableProps = hoodieCatalogTable.metaClient.getTableConfig.getProps
      propertyKey match {
        case Some(p) =>
          val propValue = tableProps
            .getOrElse(p, s"Table ${tableIdentifier.unquotedString} does not have property: $p")
          Seq(Row(propValue))
        case None =>
          tableProps.map(p => Row(p._1, p._2)).toSeq
      }
    }
  }
}
