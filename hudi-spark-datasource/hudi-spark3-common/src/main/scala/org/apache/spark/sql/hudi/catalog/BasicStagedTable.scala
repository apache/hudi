/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.catalog

import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType

import java.util

/**
 * Basic implementation that represents a table which is staged for being committed.
 *
 * @param ident   table ident
 * @param table   table
 * @param catalog table catalog
 */
case class BasicStagedTable(ident: Identifier,
                            table: Table,
                            catalog: TableCatalog) extends SupportsWrite with StagedTable {
  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    info match {
      case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
      case _ => throw new HoodieException(s"Table `${ident.name}` does not support writes.")
    }
  }

  override def abortStagedChanges(): Unit = catalog.dropTable(ident)

  override def commitStagedChanges(): Unit = {}

  override def name(): String = ident.name()

  override def schema(): StructType = table.schema()

  override def partitioning(): Array[Transform] = table.partitioning()

  override def capabilities(): util.Set[TableCapability] = table.capabilities()

  override def properties(): util.Map[String, String] = table.properties()
}
