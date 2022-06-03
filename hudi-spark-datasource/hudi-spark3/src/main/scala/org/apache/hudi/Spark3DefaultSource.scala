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

import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Table, TableProvider, V1Table}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.hudi.catalog.HoodieInternalV2Table
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class Spark3DefaultSource extends DefaultSource with DataSourceRegister with TableProvider {

  override def shortName(): String = "hudi"

  def inferSchema: StructType = new StructType()

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw new HoodieException("'path' cannot be null, missing 'path' from table properties")

    val v2Table = HoodieInternalV2Table(SparkSession.active, path)
    // TODO elaborate
    V1Table(v2Table.v1Table)
  }
}
