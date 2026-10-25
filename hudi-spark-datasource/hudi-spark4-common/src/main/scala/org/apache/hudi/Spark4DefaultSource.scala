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

package org.apache.hudi

import org.apache.spark.sql.types.{DataType, VariantType}

/**
 * Spark-4-specific `DataSourceRegister` for Hudi.
 *
 * Spark 4.0 added `CreatableRelationProvider.supportsDataType` and gates the V1 write path
 * (`DataSource.planForWriting`) on it. The default whitelist does not include `VariantType`,
 * so without this override `df.write.format("hudi").save(path)` fails with
 * `UNSUPPORTED_DATA_TYPE_FOR_DATASOURCE` whenever the DataFrame carries a native
 * VariantType column. The default implementation recurses through Array/Map/Struct
 * element types via virtual dispatch, so nested VariantType is covered automatically.
 */
class Spark4DefaultSource extends BaseDefaultSource {
  override def supportsDataType(dt: DataType): Boolean = dt match {
    case _: VariantType => true
    case _              => super.supportsDataType(dt)
  }
}
