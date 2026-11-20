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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.blob.BlobTestHelpers._
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.col
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull}
import org.junit.jupiter.api.Test

import scala.reflect.ClassTag

/**
 * Out-of-line blob reads on executors whose broadcast storage configuration went through Kryo.
 *
 * Local mode hands broadcasts to tasks without serializing them, so the test performs the Kryo
 * round trip itself, with a serializer that has no Hudi registrator (the case for engines that
 * enable Kryo without registering Hudi's classes).
 */
class TestBatchedBlobReaderKryo extends HoodieClientTestBase {

  @Test
  def testOutOfLineReadAfterKryoRoundTripOfTheBroadcastStorageConf(): Unit = {
    val extFile = createTestFile(tempDir, "kryo-out-of-line.bin", 1000)
    val df = sparkSession.createDataFrame(Seq((1, extFile, 0L, 100L), (2, extFile, 100L, 100L)))
      .toDF("id", "external_path", "offset", "length")
      .withColumn("file_info", blobStructCol("file_info", col("external_path"), col("offset"), col("length")))
      .select("id", "file_info")
    val storageConf = new HadoopStorageConfiguration(sparkSession.sparkContext.hadoopConfiguration)

    val shipped = kryoRoundTrip(BatchedBlobReader.broadcastStorageConf(sparkSession.sparkContext, storageConf).value)
    val dataIdx = df.schema.length
    val result = BatchedBlobReader.processRDD(
        df.queryExecution.toRdd, df.schema, sparkSession.sparkContext.broadcast(shipped), columnName = "file_info")
      .map(row => (row.getInt(0), row.getBinary(dataIdx)))
      .collect()
      .sortBy(_._1)

    assertEquals(2, result.length)
    result.foreach { case (id, bytes) =>
      assertNotNull(bytes)
      assertEquals(100, bytes.length)
      assertBytesContent(bytes, expectedOffset = (id - 1) * 100)
    }
  }

  private def kryoRoundTrip[T: ClassTag](value: T): T = {
    val serializer = new KryoSerializer(new SparkConf(false)).newInstance()
    serializer.deserialize[T](serializer.serialize(value))
  }
}
