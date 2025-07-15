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

  private def buildCustomSparkSession(tempDir: File, enablePolaris: Boolean = false): (SparkSession, TestableHoodieCatalog, MockPolarisSparkCatalog) = {
    val mockPolarisDelegate = spy(new MockPolarisSparkCatalog())

    val sparkBuilder = SparkSession.builder()
      .appName("TestPolarisHoodieCatalogDelegation")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", tempDir.getCanonicalPath)
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.hoodie_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")

    if (enablePolaris) {
      sparkBuilder.config("spark.sql.catalog.polaris_catalog", "org.apache.polaris.spark.SparkCatalog")
    }

    // Create SparkSession first so it becomes the active session
    val customSession = sparkBuilder.getOrCreate()

    // Create TestableHoodieCatalog after SparkSession is active
    // This ensures HoodieCatalog can access SparkSession.active during initialization
    val testableHoodieCatalog = new TestableHoodieCatalog(mockPolarisDelegate)
    (customSession, testableHoodieCatalog, mockPolarisDelegate)
  }

  test("Test Normal Hudi Catalog Route") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create custom session with HoodieCatalog (no Polaris)
      val (customSession, testableHoodieCatalog, mockPolarisDelegate) = buildCustomSparkSession(tmp)

      try {
        // Verify Polaris is not detected
        assertFalse(HoodieSqlCommonUtils.isUsingPolarisCatalog(customSession))

        // Issue SQL DDL using custom session
        customSession.sql(s"""
          CREATE TABLE $tableName (
            id int,
            name string,
            ts long
          ) USING hudi
          TBLPROPERTIES (
            primaryKey = 'id',
            preCombineField = 'ts'
          )
          LOCATION '$tablePath'
        """.stripMargin)

        // Verification via filesystem
        assertTrue(new File(s"$tablePath/.hoodie").exists(), "Hudi metadata directory should exist")
        assertTrue(new File(s"$tablePath/.hoodie/hoodie.properties").exists(), "Hudi properties file should exist")

        // Mock Polaris delegate should not be called in normal route
        // Since we can't easily inject TestableHoodieCatalog into SparkSession,
        // we'll simulate the delegation logic verification
        assertFalse(HoodieSqlCommonUtils.isUsingPolarisCatalog(customSession))

        // Manual test of delegation logic for normal route
        // Inject the spark session into the testable catalog
        testableHoodieCatalog.setTestSparkSession(customSession)

        val testTableName = tableName + "_test"
        testableHoodieCatalog.createTable(
          Identifier.of(Array("default"), testTableName),
          StructType(Seq(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("ts", LongType)
          )),
          Array.empty[Transform],
          Map("provider" -> "hudi", "primaryKey" -> "id", "preCombineField" -> "ts").asJava
        )

        // Verify delegate was not called (normal route)
        verify(mockPolarisDelegate, never()).createTable(any(), any(), any(), any())

      } finally {
        customSession.stop()
      }
    }
  }

  test("Test Polaris Delegation Route") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      // Create custom session with HoodieCatalog + Polaris config
      val (customSession, testableHoodieCatalog, mockPolarisDelegate) = buildCustomSparkSession(tmp, enablePolaris = true)

      try {
        // Verify Polaris is detected
        assertTrue(HoodieSqlCommonUtils.isUsingPolarisCatalog(customSession))

        // Issue SQL DDL using custom session
        customSession.sql(s"""
          CREATE TABLE $tableName (
            id int,
            name string,
            ts long
          ) USING hudi
          TBLPROPERTIES (
            primaryKey = 'id',
            preCombineField = 'ts'
          )
          LOCATION '$tablePath'
        """.stripMargin)

        // Verification via filesystem - In real Polaris setup, HoodieCatalog would delegate
        // to Polaris after creating the Hudi table
        assertTrue(new File(s"$tablePath/.hoodie").exists(), "Hudi metadata directory should exist")
        assertTrue(new File(s"$tablePath/.hoodie/hoodie.properties").exists(), "Hudi properties file should exist")

        assertTrue(HoodieSqlCommonUtils.isUsingPolarisCatalog(customSession), "Polaris should be detected")

        // Manual test of delegation logic with Polaris enabled
        // Inject the spark session into the testable catalog
        testableHoodieCatalog.setTestSparkSession(customSession)
        testableHoodieCatalog.createTable(
          Identifier.of(Array("default"), tableName + "_polaris"),
          StructType(Seq(
            StructField("id", IntegerType),
            StructField("name", StringType),
            StructField("ts", LongType)
          )),
          Array.empty[Transform],
          Map("provider" -> "hudi", "primaryKey" -> "id", "preCombineField" -> "ts").asJava
        )

        // Verify delegate was called (Polaris route)
        verify(mockPolarisDelegate, times(1)).createTable(any(), any(), any(), any())

      } finally {
        customSession.stop()
      }
    }
  }
}
