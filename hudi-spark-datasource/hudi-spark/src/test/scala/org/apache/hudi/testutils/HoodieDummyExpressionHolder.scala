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

package org.apache.hudi.testutils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

/**
 * Test-only leaf plan for running a bare [[Expression]] through the full session optimizer.
 *
 * Forked from Spark's [[org.apache.spark.sql.catalyst.encoders.DummyExpressionHolder]], which
 * hard-codes output = Nil: SPARK-44219 validates optimized plans against dangling expression
 * references, so a holder without an output fails that check once the validation runs (enforced
 * on Spark 4). Passing the expressions' references as the output keeps the plan valid.
 */
case class HoodieDummyExpressionHolder(exprs: Seq[Expression], output: Seq[Attribute]) extends LeafNode {
  override lazy val resolved = true
}
