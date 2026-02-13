/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.blob

import org.apache.hudi.common.schema.HoodieSchema

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import java.io.File
import java.nio.file.{Files, Path}

object BlobTestHelpers {
  def blobMetadata: Metadata = {
    new MetadataBuilder()
      .putBoolean(HoodieSchema.Blob.HUDI_BLOB, true)
      .build()
  }

  def blobStructCol(name: String, filePathCol: Column, offsetCol: Column, lengthCol: Column): Column = {
    struct(
      lit("out_of_line").as("type"),
      lit(null).cast("binary").as("data"),
      struct(
        filePathCol.as("external_path"),
        offsetCol.as("offset"),
        lengthCol.as("length"),
        lit(false).as("managed")
      ).as("reference")
    ).as(name, blobMetadata)
  }

  def createTestFile(tempDir: Path, name: String, size: Int): String = {
    val file = new File(tempDir.toString, name)
    val bytes = (0 until size).map(i => (i % 256).toByte).toArray
    Files.write(file.toPath, bytes)
    file.getAbsolutePath
  }

  /**
   * Assert that byte array contains expected pattern (i % 256) at given offset.
   *
   * @param data Array of bytes to verify
   * @param expectedOffset Starting offset for pattern (default 0)
   */
  def assertBytesContent(data: Array[Byte], expectedOffset: Int = 0): Unit = {
    for (i <- 0 until data.length) {
      assertEquals((expectedOffset + i) % 256, data(i) & 0xFF,
        s"Mismatch at byte $i (global offset ${expectedOffset + i})")
    }
  }

  /**
   * Execute code block with temporary Spark configuration.
   * Automatically restores previous values after execution.
   *
   * @param spark SparkSession instance
   * @param configs Configuration key-value pairs to set
   * @param fn Code block to execute with configs
   */
  def withSparkConfig[T](spark: SparkSession, configs: Map[String, String])(fn: => T): T = {
    val oldValues = configs.keys.map(k => (k, spark.conf.getOption(k))).toMap
    try {
      configs.foreach { case (k, v) => spark.conf.set(k, v) }
      fn
    } finally {
      oldValues.foreach { case (k, oldValue) =>
        oldValue match {
          case Some(v) => spark.conf.set(k, v)
          case None => spark.conf.unset(k)
        }
      }
    }
  }

  /**
   * Assert that DataFrame contains all specified columns.
   *
   * @param df DataFrame to check
   * @param columnNames Variable number of column names
   */
  def assertColumnsExist(df: org.apache.spark.sql.DataFrame, columnNames: String*): Unit = {
    columnNames.foreach { colName =>
      assertTrue(df.columns.contains(colName),
        s"DataFrame missing expected column: $colName. Available: ${df.columns.mkString(", ")}")
    }
  }
}
