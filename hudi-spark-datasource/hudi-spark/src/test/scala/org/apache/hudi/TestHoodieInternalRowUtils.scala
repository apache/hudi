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

package org.apache.hudi

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.testutils.HoodieClientTestUtils
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{HoodieInternalRowUtils, Row, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class TestHoodieInternalRowUtils extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private val schema1 = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("address",
        StructType(Seq(
          StructField("city", StringType),
          StructField("street", StringType)
        ))
      )
    ))

  private val schema2 = StructType(
    Array(
      StructField("name1", StringType),
      StructField("age1", IntegerType)
    )
  )
  private val schemaMerge = StructType(schema1.fields ++ schema2.fields)

  override protected def beforeAll(): Unit = {
    // Initialize a local spark env
    val jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(classOf[TestHoodieInternalRowUtils].getName))
    jsc.setLogLevel("ERROR")
    sparkSession = SparkSession.builder.config(jsc.getConf).getOrCreate
  }

  override protected def afterAll(): Unit = {
    sparkSession.close()
  }

  test("test rewrite") {
    val rows = Seq(
      Row("Andrew", 18, Row("Mission st", "SF"), "John", 19)
    )
    val data = sparkSession.sparkContext.parallelize(rows)
    val oldRow = sparkSession.createDataFrame(data, schemaMerge).queryExecution.toRdd.first()

    val rowWriter1 = HoodieInternalRowUtils.genUnsafeRowWriter(schemaMerge, schema1)
    val newRow1 = rowWriter1(oldRow)

    val serDe1 = sparkAdapter.createSparkRowSerDe(schema1)
    assertEquals(serDe1.deserializeRow(newRow1), Row("Andrew", 18, Row("Mission st", "SF")));

    val rowWriter2 = HoodieInternalRowUtils.genUnsafeRowWriter(schemaMerge, schema2)
    val newRow2 = rowWriter2(oldRow)

    val serDe2 = sparkAdapter.createSparkRowSerDe(schema2)
    assertEquals(serDe2.deserializeRow(newRow2), Row("John", 19));
  }

  test("test rewrite with nullable value") {
    val data = sparkSession.sparkContext.parallelize(Seq(Row("Rob", 18, null.asInstanceOf[StructType])))
    val oldRow = sparkSession.createDataFrame(data, schema1).queryExecution.toRdd.first()
    val rowWriter = HoodieInternalRowUtils.genUnsafeRowWriter(schema1, schemaMerge)
    val newRow = rowWriter(oldRow)

    val serDe = sparkAdapter.createSparkRowSerDe(schemaMerge)
    assertEquals(serDe.deserializeRow(newRow), Row("Rob", 18, null.asInstanceOf[StructType], null.asInstanceOf[StringType], null.asInstanceOf[IntegerType]))
  }
}
