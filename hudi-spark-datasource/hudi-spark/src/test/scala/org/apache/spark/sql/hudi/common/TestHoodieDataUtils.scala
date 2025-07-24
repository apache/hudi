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

    // Verify results
    assert(result.size() == 4, "Should have 4 entries (3 non-null keys + 1 null key)")
    assert(result.get("key1") == "value1_dup", "key1 should have the last value (dedupe keeps incoming)")
    assert(result.get("key2") == "value2", "key2 should have its value")
    assert(result.get("key3") == "value3", "key3 should have its value")
    assert(result.containsKey(null), "Should contain null key")

    // The null key should have one of the null values (the implementation picks the first distinct one)
    val nullValue = result.get(null)
    assert(nullValue == "nullValue1" || nullValue == "nullValue2",
      s"Null key should have one of the null values, but got: $nullValue")
  }

  test("Test dedupeAndCollectAsMap with only null keys") {
    // Given: A dataset containing only null keys with different values
    // When: dedupeAndCollectAsMap is called on the data
    // Then: The result should contain exactly one entry for the null key with one of the values
    val testData = Seq(
      pair(null, "nullValue1"),
      pair(null, "nullValue2"),
      pair(null, "nullValue3")
    )

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)
    val result = HoodieDataUtils.dedupeAndCollectAsMap(pairData)

    // Verify results
    assert(result.size() == 1, "Should have 1 entry for null key")
    assert(result.containsKey(null), "Should contain null key")

    // The null key should have one of the values
    val nullValue = result.get(null)
    assert(Seq("nullValue1", "nullValue2", "nullValue3").contains(nullValue),
      s"Null key should have one of the null values, but got: $nullValue")
  }

  test("Test dedupeAndCollectAsMap with no null keys") {
    // Given: A dataset with only non-null keys
    // When: dedupeAndCollectAsMap is called on the data
    // Then: The result should contain all keys without any null key entries
    val testData = Seq(
      pair("key1", "value1"),
      pair("key2", "value2"),
      pair("key3", "value3")
    )

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)
    val result = HoodieDataUtils.dedupeAndCollectAsMap(pairData)

    // Verify results
    assert(result.size() == 3, "Should have 3 entries")
    assert(!result.containsKey(null), "Should not contain null key")
    assert(result.get("key1") == "value1")
    assert(result.get("key2") == "value2")
    assert(result.get("key3") == "value3")
  }

  test("Test dedupeAndCollectAsMapOfSet with null keys") {
    // Given: A dataset with both null and non-null keys, where some keys have multiple values
    // When: dedupeAndCollectAsMapOfSet is called on the data
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
    val result = HoodieDataUtils.dedupeAndCollectAsMapOfSet(pairData)

    // Verify results
    assert(result.size() == 3, "Should have 3 entries (2 non-null keys + 1 null key)")

    // Check key1 values
    val key1Values = result.get("key1")
    assert(key1Values.size() == 3, "key1 should have 3 values")
    assert(key1Values.asScala.toSet == Set("value1a", "value1b", "value1c"))

    // Check key2 values
    val key2Values = result.get("key2")
    assert(key2Values.size() == 1, "key2 should have 1 value")
    assert(key2Values.contains("value2"))

    // Check null key values
    assert(result.containsKey(null), "Should contain null key")
    val nullValues = result.get(null)
    assert(nullValues.size() == 2, "null key should have 2 distinct values")
    assert(nullValues.asScala.toSet == Set("nullValue1", "nullValue2"))
  }

  test("Test dedupeAndCollectAsMapOfSet with empty input") {
    // Given: An empty dataset
    // When: dedupeAndCollectAsMapOfSet is called on the empty data
    // Then: The result should be an empty map
    val testData = Seq.empty[Pair[String, String]]

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)
    val result = HoodieDataUtils.dedupeAndCollectAsMapOfSet(pairData)

    assert(result.isEmpty, "Result should be empty for empty input")
  }

  test("Test dedupeAndCollectAsMapOfSet with duplicate null values") {
    // Given: A dataset with only null keys having duplicate values
    // When: dedupeAndCollectAsMapOfSet is called on the data
    // Then: The null key should map to a set with deduplicated values (no duplicates in the set)
    val testData = Seq(
      pair(null, "value1"),
      pair(null, "value1"),  // Duplicate
      pair(null, "value2"),
      pair(null, "value2"),  // Duplicate
      pair(null, "value3")
    )

    val rdd = spark.sparkContext.parallelize(testData).toJavaRDD()
    val pairRDD = rdd.mapToPair(p => (p.getKey, p.getValue))
    val pairData: HoodiePairData[String, String] = HoodieJavaPairRDD.of(pairRDD)
    val result = HoodieDataUtils.dedupeAndCollectAsMapOfSet(pairData)

    assert(result.size() == 1, "Should have 1 entry for null key")
    assert(result.containsKey(null), "Should contain null key")

    val nullValues = result.get(null)
    assert(nullValues.size() == 3, "null key should have 3 distinct values")
    assert(nullValues.asScala.toSet == Set("value1", "value2", "value3"))
  }
}
