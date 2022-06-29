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

package org.apache.spark.sql.hudi.command.payload

import com.google.common.cache.CacheBuilder
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.AvroConversionUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hudi.command.payload.SqlTypedRecord.{getAvroDeserializer, getSqlType}
import org.apache.spark.sql.types.StructType

import java.util.concurrent.Callable

/**
 * A sql typed record which will convert the avro field to sql typed value.
 */
class SqlTypedRecord(val record: IndexedRecord) extends IndexedRecord {

  private lazy val sqlRow = getAvroDeserializer(getSchema).deserialize(record).get.asInstanceOf[InternalRow]

  override def put(i: Int, v: Any): Unit = {
    record.put(i, v)
  }

  override def get(i: Int): AnyRef = {
    sqlRow.get(i, getSqlType(getSchema)(i).dataType)
  }

  override def getSchema: Schema = record.getSchema
}

object SqlTypedRecord {

  private val sqlTypeCache = CacheBuilder.newBuilder().build[Schema, StructType]()

  private val avroDeserializerCache = CacheBuilder.newBuilder().build[Schema, HoodieAvroDeserializer]()

  def getSqlType(schema: Schema): StructType = {
    sqlTypeCache.get(schema, new Callable[StructType] {
      override def call(): StructType = {
        val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
        sqlTypeCache.put(schema, structType)
        structType
      }
    })
  }

  def getAvroDeserializer(schema: Schema): HoodieAvroDeserializer= {
    avroDeserializerCache.get(schema, new Callable[HoodieAvroDeserializer] {
      override def call(): HoodieAvroDeserializer = {
        val deserializer = sparkAdapter.createAvroDeserializer(schema, getSqlType(schema))
        avroDeserializerCache.put(schema, deserializer)
        deserializer
      }
    })
  }
}
