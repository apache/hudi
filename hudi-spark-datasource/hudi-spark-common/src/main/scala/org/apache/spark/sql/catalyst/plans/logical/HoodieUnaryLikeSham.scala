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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.trees.TreeNode

/**
 * NOTE: This is a sham trait providing a stub method definition which is not used anywhere.
 * This is required just to be able to compile the code that relies on [[UnaryLike]]
 * (introduced in Spark 3.2) against Spark < 3.2
 */
trait HoodieUnaryLikeSham[T <: TreeNode[T]] { self: TreeNode[T] =>

  protected def withNewChildInternal(newChild: T): T

}
