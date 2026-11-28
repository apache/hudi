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

package org.apache.comet.serde.operator

import org.apache.comet.DataTypeSupport
import org.apache.comet.rules.CometScanTypeChecker
import org.apache.comet.serde.OperatorOuterClass

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PlanExpression}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ListBuffer

/**
 * Serializes the read of a Hudi table's base files for Comet's native Parquet scan. It lives in
 * a Comet package because the scan's serde is visible only there.
 */
object HudiBaseFileScanSerde {

  /**
   * The common block of a native Parquet scan that reads `output` from the table's base files and
   * prunes with `dataFilters`, or None when Comet's own Parquet scan would decline the read.
   *
   * Every output column is a data column: a Hudi base file stores the partition columns, so the
   * scan has no partition schema and takes no partition values. Filters holding a subquery are
   * left to the filter above the scan, as Comet leaves them at planning.
   */
  def nativeScanCommon(
      source: String,
      output: Seq[Attribute],
      dataFilters: Seq[Expression],
      objectStoreOptions: Map[String, String],
      hadoopConf: Configuration,
      conf: SQLConf): Option[OperatorOuterClass.NativeScanCommon] = {
    val schema = StructType(output.map(attr =>
      StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)))
    if (!CometScanTypeChecker().isSchemaSupported(schema, new ListBuffer[String]())) {
      return None
    }
    if (!conf.caseSensitiveAnalysis &&
      DataTypeSupport.hasCaseInsensitiveDuplicateFieldNames(schema)) {
      return None
    }
    val pushedFilters = dataFilters.filterNot(_.exists(_.isInstanceOf[PlanExpression[_]]))
    CometNativeScan.buildNativeScanCommon(
      source = source,
      output = output,
      requiredSchema = schema,
      dataSchema = schema,
      partitionSchema = new StructType(),
      fileConstantMetadataColumns = Nil,
      dataFilters = pushedFilters,
      objectStoreOptions = objectStoreOptions,
      hadoopConf = hadoopConf,
      conf = conf).map { builder =>
      // Spark may skip rows a filter rejects before it converts their values, so the native scan
      // must know a filter exists even when none of them is pushed.
      if (dataFilters.nonEmpty && conf.parquetFilterPushDown) {
        builder.setHasDataFilters(true)
      }
      builder.build()
    }
  }
}
