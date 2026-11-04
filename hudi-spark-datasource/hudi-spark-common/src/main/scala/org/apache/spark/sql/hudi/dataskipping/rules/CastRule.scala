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

package org.apache.spark.sql.hudi.dataskipping.rules

import org.apache.spark.sql.HoodieSparkTypeUtils.isCastPreservingOrdering
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression}
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}

/**
 * `Cast`.
 *
 * Shared guard: `isCastPreservingOrdering` of the child type to the target type (rejects, for ex,
 * string to numeric, timestamp to string, narrowing numeric casts, and collation changes).
 */
object CastRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    // NOTE: Some casts (for ex, string to date in non-ANSI mode) return NULL on malformed input;
    //       `partial` is left false for now, as it has no behavioral effect yet
    case cast @ Cast(child, _, _, _) if isCastPreservingOrdering(cast.child.dataType, cast.dataType) =>
      Seq(Step(term, child, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }
}
