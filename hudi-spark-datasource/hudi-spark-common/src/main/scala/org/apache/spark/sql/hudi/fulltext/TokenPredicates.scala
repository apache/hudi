/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.fulltext

import org.apache.hudi.metadata.FullTextIndexUtils

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, ExpressionInfo, ImplicitCastInputTypes, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{AbstractDataType, StringType}

import scala.collection.JavaConverters._

/**
 * Token predicates over a text column. Both the row-level evaluation and the full-text index use
 * [[FullTextIndexUtils.tokenize]], so pruning by the index can never disagree with the filter.
 */
sealed abstract class TokenPredicate extends BinaryExpression with Predicate with ImplicitCastInputTypes with CodegenFallback {

  def text: Expression = left

  def query: Expression = right

  /** True if every query token must be present; false if any one suffices. */
  def matchAll: Boolean

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType, StringType)

  override protected def nullSafeEval(textValue: Any, queryValue: Any): Any = {
    val queryTokens = FullTextIndexUtils.tokenize(queryValue.toString)
    if (queryTokens.isEmpty) {
      false
    } else {
      val textTokens = FullTextIndexUtils.tokenize(textValue.toString)
      if (matchAll) queryTokens.asScala.forall(textTokens.contains) else queryTokens.asScala.exists(textTokens.contains)
    }
  }
}

case class HudiHasToken(left: Expression, right: Expression) extends TokenPredicate {
  override def matchAll: Boolean = true
  override def prettyName: String = TokenPredicates.HAS_TOKEN
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): HudiHasToken =
    copy(left = newLeft, right = newRight)
}

case class HudiHasAllTokens(left: Expression, right: Expression) extends TokenPredicate {
  override def matchAll: Boolean = true
  override def prettyName: String = TokenPredicates.HAS_ALL_TOKENS
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): HudiHasAllTokens =
    copy(left = newLeft, right = newRight)
}

case class HudiHasAnyTokens(left: Expression, right: Expression) extends TokenPredicate {
  override def matchAll: Boolean = false
  override def prettyName: String = TokenPredicates.HAS_ANY_TOKENS
  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): HudiHasAnyTokens =
    copy(left = newLeft, right = newRight)
}

object TokenPredicates {
  val HAS_TOKEN = "hudi_has_token"
  val HAS_ALL_TOKENS = "hudi_has_all_tokens"
  val HAS_ANY_TOKENS = "hudi_has_any_tokens"

  private def info(clazz: Class[_], name: String, usage: String): ExpressionInfo =
    new ExpressionInfo(clazz.getCanonicalName, name, usage)

  private def builder(name: String, f: (Expression, Expression) => Expression): Seq[Expression] => Expression =
    (args: Seq[Expression]) => {
      require(args.length == 2, s"$name expects exactly 2 arguments, got ${args.length}")
      f(args.head, args(1))
    }

  val funcs: Seq[(FunctionIdentifier, ExpressionInfo, Seq[Expression] => Expression)] = Seq(
    (FunctionIdentifier(HAS_TOKEN),
      info(classOf[HudiHasToken], HAS_TOKEN,
        "Usage: hudi_has_token(text, token) - true if text contains the token, after full-text tokenization"),
      builder(HAS_TOKEN, HudiHasToken)),
    (FunctionIdentifier(HAS_ALL_TOKENS),
      info(classOf[HudiHasAllTokens], HAS_ALL_TOKENS,
        "Usage: hudi_has_all_tokens(text, query) - true if text contains every token of query"),
      builder(HAS_ALL_TOKENS, HudiHasAllTokens)),
    (FunctionIdentifier(HAS_ANY_TOKENS),
      info(classOf[HudiHasAnyTokens], HAS_ANY_TOKENS,
        "Usage: hudi_has_any_tokens(text, query) - true if text contains at least one token of query"),
      builder(HAS_ANY_TOKENS, HudiHasAnyTokens))
  )
}
