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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.unsafe.types.VariantVal

/**
 * Base class for Spark 4.x HoodiePartitionValues implementations.
 * Adds the delegation logic available in all Spark 4.x versions on top of
 * [[BaseHoodiePartitionValues]]. Version-specific subclasses only implement
 * `copy()` and the getters introduced by a newer Spark version.
 */
abstract class Spark4HoodiePartitionValues(values: InternalRow)
  extends BaseHoodiePartitionValues(values) {

  override def getVariant(ordinal: Int): VariantVal = {
    values.getVariant(ordinal)
  }
}
