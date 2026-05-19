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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}

abstract class HoodieSpark4CatalystExpressionUtils extends HoodieCatalystExpressionUtils {

  /**
   * The attribute name may differ from the one in the schema if the query analyzer
   * is case insensitive. We should change attribute names to match the ones in the schema,
   * so we do not need to worry about case sensitivity anymore
   */
  def normalizeExprs(exprs: Seq[Expression], attributes: Seq[Attribute]): Seq[Expression]

  /**
   * Returns a filter that its reference is a subset of `outputSet` and it contains the maximum
   * constraints from `condition`. This is used for predicate push-down
   * When there is no such filter, `None` is returned.
   */
  def extractPredicatesWithinOutputSet(condition: Expression,
                                                outputSet: AttributeSet): Option[Expression]
}
