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

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.testutils.HoodieClientTestUtils

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class TestHoodieInternalRowUtils extends FunSuite with Matchers with BeforeAndAfterAll {

  private var sparkSession: SparkSession = _

  private val schema1 = StructType(
    Array(
      StructField("name", StringType),
      StructField("age", IntegerType)
    )
  )
  private val schema2 = StructType(
    Array(
      StructField("name1", StringType),
      StructField("age1", IntegerType)
    )
  )
  private val schemaMerge = StructType(schema1.fields ++ schema2.fields)
  private val schema1WithMetaData = StructType(Array(
    StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, StringType),
    StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, StringType),
    StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, StringType),
    StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, StringType),
    StructField(HoodieRecord.FILENAME_METADATA_FIELD, StringType),
    StructField(HoodieRecord.OPERATION_METADATA_FIELD, StringType),
    StructField(HoodieRecord.HOODIE_IS_DELETED, BooleanType)
  ) ++ schema1.fields)

  override protected def beforeAll(): Unit = {
    // Initialize a local spark env
    val jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest(classOf[TestHoodieInternalRowUtils].getName))
    jsc.setLogLevel("ERROR")
    sparkSession = SparkSession.builder.config(jsc.getConf).getOrCreate
  }

  override protected def afterAll(): Unit = {
    sparkSession.close()
  }

  test("test merge") {
    val data1 = sparkSession.sparkContext.parallelize(Seq(Row("like", 18)))
    val data2 = sparkSession.sparkContext.parallelize(Seq(Row("like1", 181)))
    val row1 = sparkSession.createDataFrame(data1, schema1).queryExecution.toRdd.first()
    val row2 = sparkSession.createDataFrame(data2, schema2).queryExecution.toRdd.first()
    val rowMerge = HoodieInternalRowUtils.stitchRecords(row1, schema1, row2, schema2, schemaMerge)
    assert(rowMerge.get(0, StringType).toString.equals("like"))
    assert(rowMerge.get(1, IntegerType) == 18)
    assert(rowMerge.get(2, StringType).toString.equals("like1"))
    assert(rowMerge.get(3, IntegerType) == 181)
  }

  test("test rewrite") {
    val data = sparkSession.sparkContext.parallelize(Seq(Row("like", 18, "like1", 181)))
    val oldRow = sparkSession.createDataFrame(data, schemaMerge).queryExecution.toRdd.first()
    val newRow1 = HoodieInternalRowUtils.rewriteRecord(oldRow, schemaMerge, schema1)
    val newRow2 = HoodieInternalRowUtils.rewriteRecord(oldRow, schemaMerge, schema2)
    assert(newRow1.get(0, StringType).toString.equals("like"))
    assert(newRow1.get(1, IntegerType) == 18)
    assert(newRow2.get(0, StringType).toString.equals("like1"))
    assert(newRow2.get(1, IntegerType) == 181)
  }

  test("test rewrite with nullable value") {
    val data = sparkSession.sparkContext.parallelize(Seq(Row("like", 18)))
    val oldRow = sparkSession.createDataFrame(data, schema1).queryExecution.toRdd.first()
    val newRow = HoodieInternalRowUtils.rewriteRecord(oldRow, schema1, schemaMerge)
    assert(newRow.get(0, StringType).toString.equals("like"))
    assert(newRow.get(1, IntegerType) == 18)
    assert(newRow.get(2, StringType) == null)
    assert(newRow.get(3, IntegerType) == null)
  }

  test("test rewrite with metaDataFiled value") {
    val data = sparkSession.sparkContext.parallelize(Seq(Row("like", 18)))
    val oldRow = sparkSession.createDataFrame(data, schema1).queryExecution.toRdd.first()
    val newRow = HoodieInternalRowUtils.rewriteRecordWithMetadata(oldRow, schema1, schema1WithMetaData, "file1")
    assert(newRow.get(0, StringType) == null)
    assert(newRow.get(1, StringType) == null)
    assert(newRow.get(2, StringType) == null)
    assert(newRow.get(3, StringType) == null)
    assert(newRow.get(4, StringType).toString.equals("file1"))
    assert(newRow.get(5, StringType) == null)
    assert(newRow.get(6, BooleanType) == null)
    assert(newRow.get(7, StringType).toString.equals("like"))
    assert(newRow.get(8, IntegerType) == 18)
  }
}
