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

package org.apache.spark.sql.hudi.analysis

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{HoodieFileSystemViewTableValuedFunction, HoodieMetadataTableValuedFunction, HoodieQuery, HoodieTableChanges, HoodieTimelineTableValuedFunction}

object TableValuedFunctions {

  val funcs = Seq(
    (
      FunctionIdentifier(HoodieQuery.FUNC_NAME),
      new ExpressionInfo(HoodieQuery.getClass.getCanonicalName, HoodieQuery.FUNC_NAME),
      (args: Seq[Expression]) => new HoodieQuery(args)
    ),
    (
      FunctionIdentifier(HoodieTableChanges.FUNC_NAME),
      new ExpressionInfo(HoodieTableChanges.getClass.getCanonicalName, HoodieTableChanges.FUNC_NAME),
      (args: Seq[Expression]) => new HoodieTableChanges(args)
    ),
    (
      FunctionIdentifier(HoodieTimelineTableValuedFunction.FUNC_NAME),
      new ExpressionInfo(HoodieTimelineTableValuedFunction.getClass.getCanonicalName, HoodieTimelineTableValuedFunction.FUNC_NAME),
      (args: Seq[Expression]) => new HoodieTimelineTableValuedFunction(args)
    ),
    (
      FunctionIdentifier(HoodieFileSystemViewTableValuedFunction.FUNC_NAME),
      new ExpressionInfo(HoodieFileSystemViewTableValuedFunction.getClass.getCanonicalName, HoodieFileSystemViewTableValuedFunction.FUNC_NAME),
      (args: Seq[Expression]) => new HoodieFileSystemViewTableValuedFunction(args)
    ),
    (
      FunctionIdentifier(HoodieMetadataTableValuedFunction.FUNC_NAME),
      new ExpressionInfo(HoodieMetadataTableValuedFunction.getClass.getCanonicalName, HoodieMetadataTableValuedFunction.FUNC_NAME),
      (args: Seq[Expression]) => new HoodieMetadataTableValuedFunction(args)
    )
  )
}
