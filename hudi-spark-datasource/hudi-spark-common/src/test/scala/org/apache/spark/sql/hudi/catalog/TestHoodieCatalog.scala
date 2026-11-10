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

import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.connector.expressions.{Expressions, LogicalExpressions, Transform}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Tests [[HoodieCatalog.convertTransforms]], which maps the V2 partition transforms of a CREATE TABLE into
 * identity partition columns plus an optional bucket spec.
 * [[org.apache.spark.sql.HoodieSparkCatalogUtils.MatchBucketTransform]] is exercised through it, including
 * the sorted-bucket arm.
 */
class TestHoodieCatalog {

  @Test
  def testIdentityTransformsBecomePartitionColumns(): Unit = {
    val (partitionCols, bucketSpec) = HoodieCatalog.convertTransforms(
      Seq(Expressions.identity("dt"), Expressions.identity("region")))
    assertEquals(Seq("dt", "region"), partitionCols)
    assertTrue(bucketSpec.isEmpty)
  }

  @Test
  def testBucketTransformBecomesBucketSpec(): Unit = {
    val (partitionCols, bucketSpec) = HoodieCatalog.convertTransforms(
      Seq(Expressions.identity("dt"), Expressions.bucket(8, "id")))
    assertEquals(Seq("dt"), partitionCols)
    assertEquals(Some(BucketSpec(8, Seq("id"), Nil)), bucketSpec)
  }

  @Test
  def testSortedBucketTransformKeepsSortColumns(): Unit = {
    val sortedBucket: Transform = LogicalExpressions.bucket(
      4, Array(Expressions.column("id")), Array(Expressions.column("ts")))
    val (partitionCols, bucketSpec) = HoodieCatalog.convertTransforms(Seq(sortedBucket))
    assertTrue(partitionCols.isEmpty)
    assertEquals(Some(BucketSpec(4, Seq("id"), Seq("ts"))), bucketSpec)
  }

  @Test
  def testMultipleBucketTransformsAreRejected(): Unit = {
    val ex = assertThrows(classOf[HoodieException], () => HoodieCatalog.convertTransforms(
      Seq(Expressions.bucket(8, "id"), Expressions.bucket(4, "name"))))
    assertTrue(ex.getMessage.contains("Multiple bucket transformations are not supported"))
  }

  @Test
  def testUnsupportedTransformIsRejected(): Unit = {
    val ex = assertThrows(classOf[HoodieException], () => HoodieCatalog.convertTransforms(
      Seq(Expressions.years("ts"))))
    assertTrue(ex.getMessage.startsWith("Partitioning by transformation"), ex.getMessage)
    assertTrue(ex.getMessage.contains("years"), ex.getMessage)
  }
}
