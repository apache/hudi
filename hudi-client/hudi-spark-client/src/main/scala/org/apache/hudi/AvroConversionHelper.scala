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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hudi.HoodieSparkUtils.sparkAdapter
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

object AvroConversionHelper {

  /**
   * @deprecated please use [[AvroConversionUtils.createRowToAvroConverter]]
   */
  def createConverterToRow(sourceAvroSchema: Schema,
                           targetSqlType: StructType): GenericRecord => Row = {

    val encoder = RowEncoder.apply(targetSqlType).resolveAndBind()
    val serde = sparkAdapter.createSparkRowSerDe(encoder)
    val converter = AvroConversionUtils.createAvroToRowConverter(sourceAvroSchema, targetSqlType)

    avro => converter.apply(avro).map(serde.deserializeRow).get
  }

  /**
   * @deprecated please use [[AvroConversionUtils.createRowToAvroConverter]]
   */
  def createConverterToAvro(sourceSqlType: StructType,
                            structName: String,
                            recordNamespace: String): Row => GenericRecord = {
    val encoder = RowEncoder.apply(sourceSqlType).resolveAndBind()
    val serde = sparkAdapter.createSparkRowSerDe(encoder)
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(sourceSqlType, structName, recordNamespace)
    // NOTE: We're conservatively assuming that the record have to be non-nullable
    val converter = AvroConversionUtils.createRowToAvroConverter(sourceSqlType, avroSchema, nullable = false)

    row => converter.apply(serde.serializeRow(row))
  }
}
