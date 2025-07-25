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

package org.apache.spark.sql.hudi.catalog

import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.mockito.Mockito.mock

import java.util

/**
 * Mock Polaris Spark Catalog for testing delegation behavior.
 * Only implements essential methods: createTable and loadTable.
 */
class MockPolarisSparkCatalog extends TableCatalog {

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def name(): String = "mock_polaris"

  override def listTables(namespace: Array[String]): Array[Identifier] =
    throw new UnsupportedOperationException("Not implemented in mock")

  override def loadTable(ident: Identifier): Table = {
    mock(classOf[Table])
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    mock(classOf[Table])
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException("Not implemented in mock")

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException("Not implemented in mock")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException("Not implemented in mock")

}
