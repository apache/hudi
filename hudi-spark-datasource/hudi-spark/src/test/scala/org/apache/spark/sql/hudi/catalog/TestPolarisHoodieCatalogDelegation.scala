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
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, spy, times, verify}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util.UUID

import scala.collection.JavaConverters._

/**
 * Test class dedicated to testing Polaris catalog delegation behavior in HoodieCatalog.
 */
class TestPolarisHoodieCatalogDelegation extends AnyFunSuite {

  private def generateTableName: String = s"hudi_test_table_${UUID.randomUUID().toString.replace("-", "_")}"

  private def withTempDir(f: File => Unit): Unit = {
    val tempDir = Utils.createTempDir()
    try {
      f(tempDir)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }

  private def buildCustomSparkSession(tempDir: File, enablePolaris: Boolean = false): (SparkSession, HoodieCatalog, MockPolarisSparkCatalog) = {
    val mockPolarisDelegate = spy[MockPolarisSparkCatalog](new MockPolarisSparkCatalog())

    val sparkBuilder = SparkSession.builder()
      .appName("TestPolarisHoodieCatalogDelegation")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", tempDir.getCanonicalPath)
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

    if (enablePolaris) {
      // In production the class should be org.apache.polaris.spark.SparkCatalog
      // (which is the default value of the config hoodie.spark.polaris.catalog.class)
      // However, in testing, verify if config works by using mock catalog
      val testPolarisCatalogClass = "org.apache.spark.sql.hudi.catalog.MockPolarisSparkCatalog"
      sparkBuilder.config("spark.sql.catalog.polaris_catalog", testPolarisCatalogClass)
      sparkBuilder.config("hoodie.spark.polaris.catalog.class", testPolarisCatalogClass)
    }

    // Create SparkSession first so it becomes the active session
    val customSession = sparkBuilder.getOrCreate()

    // Get the HoodieCatalog instance from the session
    val hoodieCatalog = customSession.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[HoodieCatalog]

    if (enablePolaris) {
      // If enabled, we mimic Polaris's Spark catalog behavior by setting the delegate
      hoodieCatalog.setDelegateCatalog(mockPolarisDelegate)
    }
    (customSession, hoodieCatalog, mockPolarisDelegate)
  }

  test("Test Normal Hudi Catalog Route") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create custom session with HoodieCatalog (with Polaris not enabled)
      val (customSession, hoodieCatalog, mockPolarisDelegate) = buildCustomSparkSession(tmp)

      try {
        // Verify Polaris is not detected
        assertFalse(HoodieSqlCommonUtils.isUsingPolarisCatalog(customSession))

        hoodieCatalog.createTable(
          Identifier.of(Array("default"), tableName),
          StructType(Seq(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("ts", LongType)
          )),
          Array.empty[Transform],
          Map("provider" -> "hudi", "primaryKey" -> "id", "preCombineField" -> "ts", "path" -> tablePath).asJava
        )

        // Verification via filesystem
        assertTrue(new File(s"$tablePath/.hoodie").exists(), "Hudi metadata directory should exist")
        assertTrue(new File(s"$tablePath/.hoodie/hoodie.properties").exists(), "Hudi properties file should exist")

        // Verify delegate was not called
        verify(mockPolarisDelegate, never()).createTable(any(), any(), any(), any())

      } finally {
        customSession.stop()
      }
    }
  }

  test("Test Polaris Detection and Delegation") {
    withTempDir { tmp =>
      // Create custom session with HoodieCatalog (with Polaris enabled)
      val (customSession, hoodieCatalog, mockPolarisDelegate) = buildCustomSparkSession(tmp, enablePolaris = true)

      try {
        // Verify Polaris is detected
        assertTrue(HoodieSqlCommonUtils.isUsingPolarisCatalog(customSession), "Should detect Polaris with correct delegate and config")

        // Verify delegation works by calling createTable API
        val tableName = generateTableName
        hoodieCatalog.createTable(
          Identifier.of(Array("default"), tableName),
          StructType(Seq(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("ts", LongType)
          )),
          Array.empty[Transform],
          Map("provider" -> "hudi", "primaryKey" -> "id", "preCombineField" -> "ts").asJava
        )

        // Verify delegate was called when Polaris is enabled
        verify(mockPolarisDelegate, times(1)).createTable(any(), any(), any(), any())

      } finally {
        customSession.stop()
      }
    }
  }
}
