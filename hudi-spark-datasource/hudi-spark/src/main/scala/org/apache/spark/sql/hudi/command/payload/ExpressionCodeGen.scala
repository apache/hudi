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

import org.apache.spark.executor.InputMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.Decimal
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
 * TODO update
 */
object ExpressionCodeGen extends Logging {

  /**
   * TODO scala-doc
   */
  def doCodeGen(exprs: Seq[Expression]): UnsafeCatalystExpressionEvaluator = {
    val ctx = new CodegenContext()

    val className = s"ExpressionPayloadEvaluator_${UUID.randomUUID().toString.replace("-", "_")}"

    val exprEvalCodes = exprs.map(_.genCode(ctx))
    val codeBody =
      s"""
         |${ctx.declareMutableStates()}
         |
         |private Object[] references;
         |private String code;
         |
         |public $className(Object references, String code) {
         |  this.references = (Object[])references;
         |  this.code = code;
         |}
         |
         |public InternalRow doEval(InternalRow ${ctx.INPUT_ROW}) {
         |    ${exprEvalCodes.map(_.code).mkString("\n")}
         |    Object[] results = new Object[${exprEvalCodes.length}];
         |    ${(for (i <- exprEvalCodes.indices) yield {
                  s"""if (${exprEvalCodes(i).isNull}) {
                     |  results[$i] = null;
                     |} else {
                     |  results[$i] = ${exprEvalCodes(i).value.code};
                     |}""".stripMargin
                 }).mkString("\n")
              }
              return new GenericInternalRow(results);
         |  }
         |
         |public String code() {
         |  return code;
         |}
         |
         |${ctx.declareAddedFunctions()}
      """.stripMargin

    val classBodyEvaluator = new ClassBodyEvaluator()
    val parentClassLoader = new ParentClassLoader(
      Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader))

    classBodyEvaluator.setParentClassLoader(parentClassLoader)
    // Cannot be under package codegen, or fail with java.lang.InstantiationException
    classBodyEvaluator.setClassName(s"org.apache.hudi.sql.payload.$className")
    classBodyEvaluator.setDefaultImports(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[GenericInternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[Expression].getName,
      classOf[Cast].getName,
      classOf[TaskContext].getName,
      classOf[TaskKilledException].getName,
      classOf[InputMetrics].getName
    )
    classBodyEvaluator.setImplementedInterfaces(Array(classOf[UnsafeCatalystExpressionEvaluator]))
    try {
      classBodyEvaluator.cook(codeBody)
    } catch {
      case e: InternalCompilerException =>
        logError("Encountered internal compiler failure during code generation", e)
        throw e
      case e: CompileException =>
        logError(s"Encountered compilation failure during code generation", e)
        throw e
    }

    val references = ctx.references.toArray.map(_.asInstanceOf[Object])

    classBodyEvaluator.getClazz.getConstructor(classOf[Object], classOf[String])
      .newInstance(references, codeBody)
      .asInstanceOf[UnsafeCatalystExpressionEvaluator]
  }
}

// TODO scala-doc
trait UnsafeCatalystExpressionEvaluator {
  def eval(ir: InternalRow): InternalRow = {
    try doEval(ir) catch {
      case e: Throwable =>
        throw new RuntimeException(s"Encountered exception execute generated code: ${e.getMessage}.\n" +
          s"Code:\n$code", e)
    }
  }

  protected def doEval(ir: InternalRow): InternalRow
  protected def code: String
}



