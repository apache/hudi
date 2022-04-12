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

package org.apache.spark

import org.apache.spark.sql.types.{DataType, NumericType, StringType}

// TODO unify w/ DataTypeUtils
object HoodieSparkTypeUtils {

  /**
   * Checks whether casting expression of [[from]] [[DataType]] to [[to]] [[DataType]] will
   * preserve ordering of the elements
   */
  def isCastPreservingOrdering(from: DataType, to: DataType): Boolean =
    (from, to) match {
      // NOTE: In the casting rules defined by Spark, only casting from String to Numeric
      // (and vice versa) are the only casts that might break the ordering of the elements after casting
      case (StringType, _: NumericType) => false
      case (_: NumericType, StringType) => false

      case _ => true
    }
}
