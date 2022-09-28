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

package org.apache.spark.sql.hudi.command.payload

import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hudi.sql.IExpressionEvaluator
import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast, Expression, GenericInternalRow, LeafExpression, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.hudi.command.payload.ExpressionCodeGen.RECORD_NAME
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ParentClassLoader
import org.apache.spark.{TaskContext, TaskKilledException}
import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.{ClassBodyEvaluator, InternalCompilerException}

import java.util.UUID

/**
 * Do CodeGen for expression based on IndexedRecord.
 * The mainly difference with the spark's CodeGen for expression is that
 * the expression's input is a IndexedRecord but not a Row.
 *
 */
object ExpressionCodeGen extends Logging {

  val RECORD_NAME = "record"

  /**
   * CodeGen for expressions.
   * @param exprs The expression list to CodeGen.
   * @return An IExpressionEvaluator generate by CodeGen which take a IndexedRecord as input
   *         param and return a Array of results for each expression.
   */
  def doCodeGen(exprs: Seq[Expression], serializer: AvroSerializer): IExpressionEvaluator = {
    val ctx = new CodegenContext()
    // Set the input_row to null as we do not use row as the input object but Record.
    ctx.INPUT_ROW = null

    val replacedExprs = exprs.map(replaceBoundReference)
    val resultVars = replacedExprs.map(_.genCode(ctx))
    val className = s"ExpressionPayloadEvaluator_${UUID.randomUUID().toString.replace("-", "_")}"
    val codeBody =
      s"""
         |private Object[] references;
         |private String code;
         |private AvroSerializer serializer;
         |
         |public $className(Object references, String code, AvroSerializer serializer) {
         |  this.references = (Object[])references;
         |  this.code = code;
         |  this.serializer = serializer;
         |}
         |
         |public GenericRecord eval(IndexedRecord $RECORD_NAME) {
         |    ${resultVars.map(_.code).mkString("\n")}
         |    Object[] results = new Object[${resultVars.length}];
         |    ${
                (for (i <- resultVars.indices) yield {
                          s"""
                             |if (${resultVars(i).isNull}) {
                             |  results[$i] = null;
                             |} else {
                             |  results[$i] = ${resultVars(i).value.code};
                             |}
                       """.stripMargin
                 }).mkString("\n")
              }
              InternalRow row = new GenericInternalRow(results);
              return (GenericRecord) serializer.serialize(row);
         |  }
         |
         |public String getCode() {
         |  return code;
         |}
     """.stripMargin

    val evaluator = new ClassBodyEvaluator()
    val parentClassLoader = new ParentClassLoader(
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader))

    evaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    evaluator.setClassName(s"org.apache.hudi.sql.payload.$className")
    evaluator.setDefaultImports(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName,
      classOf[IndexedRecord].getName,
      classOf[AvroSerializer].getName,
      classOf[GenericRecord].getName,
      classOf[GenericInternalRow].getName,
      classOf[Cast].getName
    )
    evaluator.setImplementedInterfaces(Array(classOf[IExpressionEvaluator]))
    try {
      evaluator.cook(codeBody)
    } catch {
      case e: InternalCompilerException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        throw new InternalCompilerException(msg, e)
      case e: CompileException =>
        val msg = s"failed to compile: $e"
        logError(msg, e)
        throw new CompileException(msg, e.getLocation)
    }
    val referenceArray = ctx.references.toArray.map(_.asInstanceOf[Object])
    val expressionSql = exprs.map(_.sql).mkString("  ")

    evaluator.getClazz.getConstructor(classOf[Object], classOf[String], classOf[AvroSerializer])
      .newInstance(referenceArray, s"Expressions is: [$expressionSql]\nCodeBody is: {\n$codeBody\n}", serializer)
      .asInstanceOf[IExpressionEvaluator]
  }

  /**
   * Replace the BoundReference to the Record implement which will override the
   * doGenCode method.
   */
  private def replaceBoundReference(expression: Expression): Expression = {
    expression transformDown  {
      case BoundReference(ordinal, dataType, nullable) =>
         RecordBoundReference(ordinal, dataType, nullable)
      case other =>
        other
    }
  }
}

case class RecordBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends LeafExpression {

  /**
   * Do the CodeGen for RecordBoundReference.
   * Use "IndexedRecord" as the input object but not a "Row"
   */
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = JavaCode.javaType(dataType)
    val boxType = JavaCode.boxedType(dataType)

    val value = s"($boxType)$RECORD_NAME.get($ordinal)"
    if (nullable) {
      ev.copy(code =
        code"""
              | boolean ${ev.isNull} = $RECORD_NAME.get($ordinal) == null;
              | $javaType ${ev.value} = ${ev.isNull} ?
              | ${CodeGenerator.defaultValue(dataType)} : ($value);
          """
      )
    } else {
      ev.copy(code = code"$javaType ${ev.value} = $value;", isNull = FalseLiteral)
    }
  }

  override def eval(input: InternalRow): Any = {
    throw new IllegalArgumentException(s"Should not call eval method for " +
      s"${getClass.getCanonicalName}")
  }
}

