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

package org.apache.hudi

import org.apache.spark.sql.hudi.DataSkippingUtils
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._
import scala.io.Source

/**
 * Pins the index filter that each input of [[DataSkippingGoldenInputs]] compiles to, so a change to
 * data skipping shows exactly which outputs it changes.
 *
 * To change an output on purpose: run this test, copy the `golden-index-filters.actual.txt` it
 * writes under the module's `target` directory over the golden file, and review the diff.
 */
class TestDataSkippingGoldenFilters {

  private val goldenResource = "/data-skipping/golden-index-filters.txt"

  @Test
  def testIndexFiltersMatchGolden(): Unit = {
    val source = Source.fromInputStream(getClass.getResourceAsStream(goldenResource), UTF_8.name())
    val (header, expected) = try {
      source.getLines().toList.partition(line => line.startsWith("#") || line.isEmpty)
    } finally {
      source.close()
    }
    val actual = DataSkippingGoldenInputs.goldenLines((filter, isExpressionIndex) =>
      DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr(filter, isExpressionIndex, DataSkippingGoldenInputs.indexedCols))

    if (actual != expected) {
      val actualFile = Paths.get("target", "golden-index-filters.actual.txt").toAbsolutePath
      Files.write(actualFile, (header ++ actual).asJava, UTF_8)
      val missing = expected.diff(actual)
      val unexpected = actual.diff(expected)
      fail(s"Index filters differ from $goldenResource: ${missing.size} golden lines missing, " +
        s"${unexpected.size} lines not in the golden file (${expected.size} golden, ${actual.size} actual).\n" +
        s"First missing:\n  ${missing.take(3).mkString("\n  ")}\nFirst unexpected:\n  ${unexpected.take(3).mkString("\n  ")}\n" +
        s"All actual lines are in $actualFile")
    }
  }
}
