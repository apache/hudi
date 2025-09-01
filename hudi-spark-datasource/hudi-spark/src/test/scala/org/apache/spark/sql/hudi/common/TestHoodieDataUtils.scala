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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.data.{HoodiePairData}
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.data.HoodieJavaPairRDD

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import scala.collection.JavaConverters._

/**
 * Test suite for HoodieDataUtils methods that handle null keys using RDD
 */
class TestHoodieDataUtils extends HoodieSparkSqlTestBase {

  private var jsc: JavaSparkContext = _
  private var context: HoodieSparkEngineContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    jsc = new JavaSparkContext(spark.sparkContext)
    context = new HoodieSparkEngineContext(jsc, new SQLContext(spark))
  }

  override def afterAll(): Unit = {
    if (jsc != null) {
      jsc.close()
    }
    super.afterAll()
  }

  // Helper method to create a Pair from key and value
  private def pair(key: String, value: String): Pair[String, String] = {
    Pair.of(key, value)
  }

  test("Test dedupeAndCollectAsMap with null keys") {
    // Given: A dataset with a mix of null and non-null keys, including duplicates
    // When: dedupeAndCollectAsMap is called on the data
    // Then: All non-null keys should be deduplicated normally, and null keys should be handled without causing reduceByKey failures
    val testData = Seq(
      pair("key1", "value1"),
      pair("key2", "value2"),
      pair(null, "nullValue1"),
      pair("key1", "value1_dup"),  // Duplicate key
      pair(null, "nullValue2"),    // Another null key
      pair("key3", "value3")
    )

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)
    val result = HoodieDataUtils.dedupeAndCollectAsMap(pairData)

    // Hard code expected map and assert equals
    val expectedMap = new java.util.HashMap[String, String]()
    expectedMap.put("key1", "value1_dup")  // Last value wins
    expectedMap.put("key2", "value2")
    expectedMap.put("key3", "value3")
    expectedMap.put(null, "nullValue2")     // Last null value wins

    assert(result.equals(expectedMap), s"Expected $expectedMap but got $result")
  }

  test("Test dedupeAndCollectAsList with null keys") {
    // Given: A dataset with a mix of null and non-null keys, including duplicates
    val testData = Seq(
      pair("key1", "value1"),
      pair("key2", "value2"),
      pair(null, "nullValue1"),
      pair("key1", "value1_dup"),  // Duplicate key
      pair(null, "nullValue2"),    // Another null key
      pair("key3", "value3")
    )

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)

    // When: dedupeAndCollectAsList is called
    val result = HoodieDataUtils.dedupeAndCollectAsList(pairData)

    // Then: Last value for each key should win
    val expected = Seq(
      ("key1", "value1_dup"),
      ("key2", "value2"),
      ("key3", "value3"),
      (null, "nullValue2")
    )

    val resultAsScala = result.asScala.map(p => (p.getKey.orElse(null), p.getValue)).toSet
    val expectedAsSet = expected.toSet

    assert(resultAsScala == expectedAsSet,
      s"Expected $expectedAsSet but got $resultAsScala")
  }

  test("Test CollectPairDataAsMap with null keys") {
    // Given: A dataset with both null and non-null keys, where some keys have multiple values
    // When: CollectPairDataAsMap is called on the data
    // Then: Each key (including null) should map to a set containing all its unique values
    val testData = Seq(
      pair("key1", "value1a"),
      pair("key1", "value1b"),
      pair(null, "nullValue1"),
      pair("key2", "value2"),
      pair(null, "nullValue2"),
      pair(null, "nullValue1"),  // Duplicate null value
      pair("key1", "value1c")
    )

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)
    val result = HoodieDataUtils.collectPairDataAsMap(pairData)

    // Hard code expected map and assert equals
    val expectedMap = new java.util.HashMap[String, java.util.Set[String]]()

    val key1Values = new java.util.HashSet[String]()
    key1Values.add("value1a")
    key1Values.add("value1b")
    key1Values.add("value1c")
    expectedMap.put("key1", key1Values)

    val key2Values = new java.util.HashSet[String]()
    key2Values.add("value2")
    expectedMap.put("key2", key2Values)

    val nullValues = new java.util.HashSet[String]()
    nullValues.add("nullValue1")
    nullValues.add("nullValue2")
    expectedMap.put(null, nullValues)

    assert(result.equals(expectedMap), s"Expected $expectedMap but got $result")
  }
}
