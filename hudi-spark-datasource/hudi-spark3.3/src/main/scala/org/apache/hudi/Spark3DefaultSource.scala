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

import org.apache.spark.sql.sources.DataSourceRegister

/**
 * NOTE: PLEASE READ CAREFULLY
 *       All of Spark DataSourceV2 APIs are deliberately disabled to make sure
 *       there are no regressions in performance
 *       Please check out HUDI-4178 for more details
 */
class Spark3DefaultSource extends DefaultSource with DataSourceRegister /* with TableProvider */ {

  override def shortName(): String = "hudi"

  /*
  def inferSchema: StructType = new StructType()

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw new HoodieException("'path' cannot be null, missing 'path' from table properties")

    HoodieInternalV2Table(SparkSession.active, path)
  }
  */
}
