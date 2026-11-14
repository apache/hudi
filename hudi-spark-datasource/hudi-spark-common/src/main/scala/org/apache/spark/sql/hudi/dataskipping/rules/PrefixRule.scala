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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hudi.dataskipping.{Increasing, RangeRule, Step}

/**
 * String prefixes: `Left`.
 *
 * Shared guard: none; any length operand is accepted today.
 *
 * NOTE: Lower/Upper are not matched: case folding does not preserve the binary ordering of strings
 *       (for ex, "Zebra" < "apple", but lower("Zebra") = "zebra" > "apple")
 * NOTE: The optimizer rewrites `Left` into `Substring`, so this never matches post-optimizer (#19479)
 */
object PrefixRule extends RangeRule {

  override def steps(term: Expression): Seq[Step] = term match {
    // Left API change: Improve RuntimeReplaceable
    // https://issues.apache.org/jira/browse/SPARK-38240
    case org.apache.spark.sql.catalyst.expressions.Left(str, _) =>
      Seq(Step(term, str, Increasing, fileGuard = None, partial = false))

    case _ => Nil
  }
}
