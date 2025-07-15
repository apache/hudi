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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.types.StructType
import org.mockito.Mockito.mock

import java.util

/**
 * Testable HoodieCatalog that allows delegate injection for testing.
 * Simplified version that focuses on testing the delegation logic.
 */
class TestableHoodieCatalog(private val mockDelegate: TableCatalog) extends HoodieCatalog {

  private var testSparkSession: SparkSession = _

  // Method to inject SparkSession for testing
  def setTestSparkSession(sparkSession: SparkSession): Unit = {
    this.testSparkSession = sparkSession
  }

  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = {
    // For Hudi tables, simulate the HoodieCatalog behavior
    // In the real implementation, this would call createHoodieTable and then super.createTable

    // Use testSparkSession if available, otherwise fall back to spark
    val sessionToUse = if (testSparkSession != null) testSparkSession else spark

    // Check if Polaris catalog is enabled and delegate
    if (HoodieSqlCommonUtils.isUsingPolarisCatalog(sessionToUse)) {
      mockDelegate.createTable(ident, schema, partitions, properties)
    } else {
      mock(classOf[Table])
    }
  }
}
