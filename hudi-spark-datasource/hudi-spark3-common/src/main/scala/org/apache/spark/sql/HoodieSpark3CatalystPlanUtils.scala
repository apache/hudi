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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, UpdateAction}
import org.apache.spark.sql.execution.streaming.SerializedOffset

/**
 * Implementation of [[HoodieCatalystPlansUtils]] carrying the method bodies shared by all
 * supported Spark 3.x versions
 */
abstract class HoodieSpark3CatalystPlanUtils extends BaseHoodieCatalystPlanUtils {

  override def unapplyUpdateAction(mergeAction: Any): Option[(Option[Expression], Seq[Assignment])] = {
    mergeAction match {
      case UpdateAction(condition, assignments) => Some((condition, assignments))
      case _ => None
    }
  }

  override def extractJsonFromSerializedOffset(offset: Any): Option[String] = {
    offset match {
      case SerializedOffset(json) => Some(json)
      case _ => None
    }
  }
}
