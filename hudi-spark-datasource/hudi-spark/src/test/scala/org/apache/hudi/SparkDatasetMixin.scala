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

import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload, SerializableIndexedRecord}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator

import org.apache.avro.generic.GenericRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

trait SparkDatasetMixin {

  def toDataset(spark: SparkSession, records: java.util.List[HoodieRecord[_]]) = {
    val record1 = records.get(0)
    val isSerializableIndexedRecord = record1.getData.isInstanceOf[SerializableIndexedRecord]
    val avroRecords = records.asScala.map(record =>
    {
      if (isSerializableIndexedRecord) {
        record.getData.asInstanceOf[SerializableIndexedRecord].getData.asInstanceOf[GenericRecord]
        } else {
        record.getData
          .asInstanceOf[GenericRecord]
      }
    }
    )
      .toSeq
    val rdd: RDD[GenericRecord] = spark.sparkContext.parallelize(avroRecords)
    AvroConversionUtils.createDataFrame(rdd, HoodieTestDataGenerator.AVRO_SCHEMA.toString, spark)
  }

}
