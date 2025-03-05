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

package org.apache.spark.sql.hive

import org.apache.hudi.common.testutils.HoodieTestUtils.getJavaVersion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.junit.Assume
import org.junit.jupiter.api.{BeforeAll, Disabled, TestInstance}
import org.junit.jupiter.api.TestInstance.Lifecycle

@TestInstance(Lifecycle.PER_CLASS)
class TestHiveClientUtils {
  protected var spark: SparkSession = null
  protected var hiveContext: TestHiveContext = null
  protected var hiveClient: HiveClient = null

  @BeforeAll
  def setUp(): Unit = {
    // This test is not supported yet for Java 17 due to MiniDFSCluster can't initialize under Java 17
    // for Java 17 test coverage this test has been converted to scala script here:
    // packaging/bundle-validation/spark_hadoop_mr/TestHiveClientUtils.scala
    Assume.assumeFalse(getJavaVersion == 11 || getJavaVersion == 17)

    spark = TestHive.sparkSession
    hiveContext = TestHive
    hiveClient = spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client
  }

  @Disabled("HUDI-9118")
  def reuseHiveClientFromSparkSession(): Unit = {
    assert(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive")
    assert(HiveClientUtils.getSingletonClientForMetadata(spark) == hiveClient)
  }
}
