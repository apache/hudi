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

package org.apache.hudi

import org.apache.hudi.HoodieBaseHadoopFsRelationFactory.resolveVariantAllowReadingShredded

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestHoodieHadoopFsRelationFactory {

  // Precedence for variant allow-reading-shredded:
  //   table option > hoodie session conf > explicit Spark conf > Hudi default.
  private val hudiDefault = "true"

  @Test
  def testTableOptionTakesHighestPrecedence(): Unit = {
    assertEquals("false",
      resolveVariantAllowReadingShredded(Some("false"), Some("true"), Some("true"), hudiDefault))
  }

  @Test
  def testHoodieSessionConfTakesPrecedenceOverSparkConf(): Unit = {
    assertEquals("false",
      resolveVariantAllowReadingShredded(None, Some("false"), Some("true"), hudiDefault))
  }

  @Test
  def testExplicitSparkConfIsRespected(): Unit = {
    // The fix: an explicitly-set spark.sql.variant.allowReadingShredded must not be clobbered
    // by the Hudi default when neither the table option nor the hoodie session conf is set.
    assertEquals("false",
      resolveVariantAllowReadingShredded(None, None, Some("false"), hudiDefault))
  }

  @Test
  def testFallsBackToHudiDefaultWhenNothingSet(): Unit = {
    assertEquals(hudiDefault,
      resolveVariantAllowReadingShredded(None, None, None, hudiDefault))
  }
}
