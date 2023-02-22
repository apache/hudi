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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * TODO elaborate
 */
case class AutoRecordKeyGenExpression(commitId: String) extends LeafExpression with Stateful {

  /**
   * Record ID within each partition. By being transient, count's value is reset to 0 every time
   * we serialize and deserialize and initialize it.
   */
  @transient private[this] var count: Long = _
  @transient private[this] var partitionPrefix: UTF8String = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    count = 0L
    partitionPrefix = UTF8String.fromString(s"${commitId}_${partitionIndex}_")
  }

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override protected def evalInternal(input: InternalRow): UTF8String = {
    val currentCount = count
    count += 1

    UTF8String.concat(partitionPrefix, UTF8String.fromString(currentCount.toString))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val countTerm = ctx.addMutableState(CodeGenerator.JAVA_LONG, "count")
    val partitionPrefixTerm = "partitionPrefix"
    ctx.addImmutableStateIfNotExists(CodeGenerator.javaType(StringType), partitionPrefixTerm)
    ctx.addPartitionInitializationStatement(s"$countTerm = 0L;")
    // NOTE: String here has to be broken in 2 since Scala's interpolated strings do not
    //       support escaping
    ctx.addPartitionInitializationStatement(s"$partitionPrefixTerm" + "= UTF8String.fromString(String.valueOf(partitionIndex) + \"_\");")

    ev.copy(code = code"""
      final ${CodeGenerator.javaType(dataType)} ${ev.value} = UTF8String.concat($partitionPrefixTerm, UTF8String.fromString(String.valueOf($countTerm)));
      $countTerm++;""", isNull = FalseLiteral)
  }

  override def nodeName: String = "record_key_gen_expression"

  override def sql: String = s"record_key_gen_expression()"

  override def freshCopy(): AutoRecordKeyGenExpression = AutoRecordKeyGenExpression(commitId)
}
