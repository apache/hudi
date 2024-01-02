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

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A projection that could turn UnsafeRow into GenericInternalRow
 *
 * NOTE: PLEASE REFRAIN MAKING ANY CHANGES TO THIS CODE UNLESS ABSOLUTELY NECESSARY
 *       This code is borrowed from Spark 3.1.x
 *       This code is borrowed, to fill in the gaps of Spark 2.x
 */
object SafeProjection extends CodeGeneratorWithInterpretedFallback[Seq[Expression], Projection] {

  override protected def createCodeGeneratedObject(in: Seq[Expression]): Projection = {
    GenerateSafeProjection.generate(in)
  }

  override protected def createInterpretedObject(in: Seq[Expression]): Projection = {
    throw new UnsupportedOperationException("Interpreted safe projection is not supported for Spark 2.x!")
  }

  /**
   * Returns a SafeProjection for given StructType.
   */
  def create(schema: StructType): Projection = create(schema.fields.map(_.dataType))

  /**
   * Returns a SafeProjection for given Array of DataTypes.
   */
  def create(fields: Array[DataType]): Projection = {
    createObject(fields.zipWithIndex.map(x => new BoundReference(x._2, x._1, true)))
  }

  /**
   * Returns a SafeProjection for given sequence of Expressions (bounded).
   */
  def create(exprs: Seq[Expression]): Projection = {
    createObject(exprs)
  }

  /**
   * Returns a SafeProjection for given sequence of Expressions, which will be bound to
   * `inputSchema`.
   */
  def create(exprs: Seq[Expression], inputSchema: Seq[Attribute]): Projection = {
    create(bindReferences(exprs, inputSchema))
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
  private def bindReferences[A <: Expression](
                                       expressions: Seq[A],
                                       input: AttributeSeq): Seq[A] = {
    expressions.map(BindReferences.bindReference(_, input))
  }
}
