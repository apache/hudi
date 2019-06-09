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

package com.uber.hoodie

import org.scalatest.{FunSuite, Matchers}
import DataSourceWriteOptions._
import com.uber.hoodie.config.HoodieWriteConfig
import com.uber.hoodie.exception.HoodieException
import org.apache.spark.sql.{SaveMode, SparkSession}

class HoodieSparkSqlWriterSuite extends FunSuite with Matchers {

  test("Parameters With Write Defaults") {
    val originals = HoodieSparkSqlWriter.parametersWithWriteDefaults(Map.empty)
    val rhsKey = "hoodie.right.hand.side.key"
    val rhsVal = "hoodie.right.hand.side.val"
    val modifier = Map(OPERATION_OPT_KEY -> INSERT_OPERATION_OPT_VAL, STORAGE_TYPE_OPT_KEY -> MOR_STORAGE_TYPE_OPT_VAL, rhsKey -> rhsVal)
    val modified = HoodieSparkSqlWriter.parametersWithWriteDefaults(modifier)
    val matcher = (k: String, v: String) => modified(k) should be(v)

    originals foreach {
      case (OPERATION_OPT_KEY, _) => matcher(OPERATION_OPT_KEY, INSERT_OPERATION_OPT_VAL)
      case (STORAGE_TYPE_OPT_KEY, _) => matcher(STORAGE_TYPE_OPT_KEY, MOR_STORAGE_TYPE_OPT_VAL)
      case (`rhsKey`, _) => matcher(rhsKey, rhsVal)
      case (k, v) => matcher(k, v)
    }
  }

  test("throw hoodie exception when invalid serializer") {
    val session = SparkSession.builder().appName("hoodie_test").master("local").getOrCreate()
    val sqlContext = session.sqlContext
    val options = Map("path" -> "hoodie/test/path", HoodieWriteConfig.TABLE_NAME -> "hoodie_test_tbl")
    val e = intercept[HoodieException](HoodieSparkSqlWriter.write(sqlContext, SaveMode.ErrorIfExists, options, session.emptyDataFrame))
    assert(e.getMessage.contains("spark.serializer"))
  }

}
