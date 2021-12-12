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

package org.apache.hudi

import org.apache.hudi.HoodieSparkUtils.convertToCatalystExpressions
import org.apache.hudi.HoodieSparkUtils.convertToCatalystExpression

import org.apache.spark.sql.sources.{And, EqualNullSafe, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Not, Or, StringContains, StringEndsWith, StringStartsWith}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

class TestConvertFilterToCatalystExpression {

  private lazy val tableSchema = {
    val fields = new ArrayBuffer[StructField]()
    fields.append(StructField("id", LongType, nullable = false))
    fields.append(StructField("name", StringType, nullable = true))
    fields.append(StructField("price", DoubleType, nullable = true))
    fields.append(StructField("ts", IntegerType, nullable = false))
    StructType(fields)
  }

  @Test
  def testBaseConvert(): Unit = {
    checkConvertFilter(eq("id", 1), "(`id` = 1)")
    checkConvertFilter(eqs("name", "a1"), "(`name` <=> 'a1')")
    checkConvertFilter(lt("price", 10), "(`price` < 10)")
    checkConvertFilter(lte("ts", 1), "(`ts` <= 1)")
    checkConvertFilter(gt("price", 10), "(`price` > 10)")
    checkConvertFilter(gte("price", 10), "(`price` >= 10)")
    checkConvertFilter(in("id", 1, 2 , 3), "(`id` IN (1, 2, 3))")
    checkConvertFilter(isNull("id"), "(`id` IS NULL)")
    checkConvertFilter(isNotNull("name"), "(`name` IS NOT NULL)")
    checkConvertFilter(and(lt("ts", 10), gt("ts", 1)),
      "((`ts` < 10) AND (`ts` > 1))")
    checkConvertFilter(or(lte("ts", 10), gte("ts", 1)),
      "((`ts` <= 10) OR (`ts` >= 1))")
    checkConvertFilter(not(and(lt("ts", 10), gt("ts", 1))),
      "(NOT ((`ts` < 10) AND (`ts` > 1)))")
    checkConvertFilter(startWith("name", "ab"), "`name` LIKE 'ab%'")
    checkConvertFilter(endWith("name", "cd"), "`name` LIKE '%cd'")
    checkConvertFilter(contains("name", "e"), "`name` LIKE '%e%'")
  }

  @Test
  def testConvertFilters(): Unit = {
    checkConvertFilters(Array.empty[Filter], null)
    checkConvertFilters(Array(eq("id", 1)), "(`id` = 1)")
    checkConvertFilters(Array(lt("ts", 10), gt("ts", 1)),
      "((`ts` < 10) AND (`ts` > 1))")
  }

  private def checkConvertFilter(filter: Filter, expectExpression: String): Unit = {
    // [SPARK-25769][SPARK-34636][SPARK-34626][SQL] sql method in UnresolvedAttribute,
    // AttributeReference and Alias don't quote qualified names properly
    val removeQuotesIfNeed = if (expectExpression != null && HoodieSparkUtils.isSpark3_2) {
      expectExpression.replace("`", "")
    } else {
      expectExpression
    }
    val exp = convertToCatalystExpression(filter, tableSchema)
    if (removeQuotesIfNeed == null) {
      assertEquals(exp.isEmpty, true)
    } else {
      assertEquals(exp.isDefined, true)
      assertEquals(removeQuotesIfNeed, exp.get.sql)
    }
  }

  private def checkConvertFilters(filters: Array[Filter], expectExpression: String): Unit = {
    // [SPARK-25769][SPARK-34636][SPARK-34626][SQL] sql method in UnresolvedAttribute,
    // AttributeReference and Alias don't quote qualified names properly
    val removeQuotesIfNeed = if (expectExpression != null && HoodieSparkUtils.isSpark3_2) {
      expectExpression.replace("`", "")
    } else {
      expectExpression
    }
    val exp = convertToCatalystExpressions(filters, tableSchema)
    if (removeQuotesIfNeed == null) {
      assertEquals(exp.isEmpty, true)
    } else {
      assertEquals(exp.isDefined, true)
      assertEquals(removeQuotesIfNeed, exp.get.sql)
    }
  }

  private def eq(attribute: String, value: Any): Filter = {
    EqualTo(attribute, value)
  }

  private def eqs(attribute: String, value: Any): Filter = {
    EqualNullSafe(attribute, value)
  }

  private def gt(attribute: String, value: Any): Filter = {
    GreaterThan(attribute, value)
  }

  private def gte(attribute: String, value: Any): Filter = {
    GreaterThanOrEqual(attribute, value)
  }

  private def lt(attribute: String, value: Any): Filter = {
    LessThan(attribute, value)
  }

  private def lte(attribute: String, value: Any): Filter = {
    LessThanOrEqual(attribute, value)
  }

  private def in(attribute: String, values: Any*): Filter = {
    In(attribute, values.toArray)
  }

  private def isNull(attribute: String): Filter = {
    IsNull(attribute)
  }

  private def isNotNull(attribute: String): Filter = {
    IsNotNull(attribute)
  }

  private def and(left: Filter, right: Filter): Filter = {
    And(left, right)
  }

  private def or(left: Filter, right: Filter): Filter = {
    Or(left, right)
  }

  private def not(child: Filter): Filter = {
    Not(child)
  }

  private def startWith(attribute: String, value: String): Filter = {
    StringStartsWith(attribute, value)
  }

  private def endWith(attribute: String, value: String): Filter = {
    StringEndsWith(attribute, value)
  }

  private def contains(attribute: String, value: String): Filter = {
    StringContains(attribute, value)
  }
}
