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

package org.apache.spark.sql.execution.hudi.utils

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.DataSourceStrategy.translateFilter
import org.apache.spark.sql.sources.{BaseRelation, Filter}

/**
 * This util object is use DataSourceStrategy protected translateFilter method
 * [https://github.com/apache/spark/blob/v2.4.4/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSourceStrategy.scala#L439]
 */
object PushDownUtils {

  /**
   * Tries to translate a Catalyst Seq[Expression] into data source Array[Filter].
   */
  def transformFilter(filterPredicates: Seq[Expression]): Array[Filter] = {
    val translatedMap: Map[Expression, Filter] = filterPredicates.flatMap { p =>
      translateFilter(p).map(f => p -> f)
    }.toMap
   translatedMap.values.toArray
  }
}
