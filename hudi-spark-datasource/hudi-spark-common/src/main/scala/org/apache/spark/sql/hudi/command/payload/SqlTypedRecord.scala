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

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.hudi.AvroConversionUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hudi.command.payload.SqlTypedRecord.{getAvroDeserializer, getSqlType}
import org.apache.spark.sql.types.StructType

import java.util.function.Function

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

  private val sqlTypeCache = Caffeine.newBuilder()
    .maximumSize(16).build[Schema, StructType]

  private val avroDeserializerCacheLocal = new ThreadLocal[Cache[Schema, HoodieAvroDeserializer]] {
    override def initialValue(): Cache[Schema, HoodieAvroDeserializer] = {
      Caffeine.newBuilder().maximumSize(16).build[Schema, HoodieAvroDeserializer]
    }
  }

  def getSqlType(schema: Schema): StructType = {
    sqlTypeCache.get(schema, new Function[Schema, StructType] {
      override def apply(t: Schema): StructType = AvroConversionUtils.convertAvroSchemaToStructType(t)
    })
  }

  def getAvroDeserializer(schema: Schema): HoodieAvroDeserializer= {
    avroDeserializerCacheLocal.get().get(schema, new Function[Schema, HoodieAvroDeserializer] {
      override def apply(t: Schema): HoodieAvroDeserializer = sparkAdapter.createAvroDeserializer(t, getSqlType(t))
    })
  }
}
