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

package org.apache.spark.sql.avro

import org.apache.avro.Schema

import org.apache.hudi.HoodieSparkUtils

import org.apache.spark.sql.types.DataType

/**
 * This is to be compatible with the type returned by Spark 3.1
 * and other spark versions for AvroDeserializer
 */
case class HoodieAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType) {

  private val avroDeserializer = if (HoodieSparkUtils.isSpark3_2) {
    // SPARK-34404: As of Spark3.2, there is no AvroDeserializer's constructor with Schema and DataType arguments.
    // So use the reflection to get AvroDeserializer instance.
    val constructor = classOf[AvroDeserializer].getConstructor(classOf[Schema], classOf[DataType], classOf[String])
    constructor.newInstance(rootAvroType, rootCatalystType, "EXCEPTION")
  } else {
    val constructor = classOf[AvroDeserializer].getConstructor(classOf[Schema], classOf[DataType])
    constructor.newInstance(rootAvroType, rootCatalystType)
  }

  def deserializeData(data: Any): Any = {
    avroDeserializer.deserialize(data) match {
      case Some(r) => r // As of spark 3.1, this will return data wrapped with Option, so we fetch the data.
      case o => o // for other spark version, return the data directly.
    }
  }
}
