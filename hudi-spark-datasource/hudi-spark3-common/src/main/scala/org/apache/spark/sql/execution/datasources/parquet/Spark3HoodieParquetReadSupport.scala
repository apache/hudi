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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.common.util.{Option => HOption}

import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.types.StructType

import java.time.ZoneId

/**
 * The Spark 3.x [[HoodieParquetReadSupport]], rejecting a shredded variant the request cannot
 * reconstruct. Mirrors [[org.apache.spark.sql.adapter.BaseSpark3Adapter#createParquetReadSupport]]'s
 * Spark 4.0 sibling, which rejects at the same point for the same reason.
 *
 * The per-version parquet readers guard base-file reads, but they are not the only route: log
 * blocks - native parquet log files and the inline blocks of an avro log file - are read by
 * {@code HoodieSparkParquetReader.getUnsafeRowIterator}, which builds a {@code ParquetReader} on
 * this read support instead. A shredded variant in a log block therefore only meets a guard here.
 */
class Spark3HoodieParquetReadSupport(convertTz: Option[ZoneId],
                                     enableVectorizedReader: Boolean,
                                     enableTimestampFieldRepair: Boolean,
                                     datetimeRebaseSpec: RebaseSpec,
                                     int96RebaseSpec: RebaseSpec,
                                     tableSchemaOpt: HOption[MessageType] = HOption.empty())
  extends HoodieParquetReadSupport(
    convertTz, enableVectorizedReader, enableTimestampFieldRepair,
    datetimeRebaseSpec, int96RebaseSpec, tableSchemaOpt) {

  override def init(context: InitContext): ReadContext = {
    val readContext = super.init(context)
    // Anchored on the catalyst request and the file schema, not on the requested parquet schema:
    // a Spark 3.x read asks for the variant's binary members alone, so the requested schema has
    // already had typed_value clipped away by the time it gets here and only the file can show
    // that the column is shredded.
    Option(context.getConfiguration.get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA))
      .map(StructType.fromString)
      .foreach(ParquetSchemaEvolutionUtils.validateNoShreddedVariantStructs(_, context.getFileSchema))
    readContext
  }
}
