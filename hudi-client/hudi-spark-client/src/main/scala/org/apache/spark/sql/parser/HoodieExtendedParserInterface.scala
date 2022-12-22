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

package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * This trait helps us to bridge compatibility gap of [[ParserInterface]] b/w different
 * Spark versions
 */
trait HoodieExtendedParserInterface extends ParserInterface {

  def parseQuery(sqlText: String): LogicalPlan = {
    throw new UnsupportedOperationException(s"Unsupported, parseQuery is implemented in Spark >= 3.3.0")
  }

  def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    throw new UnsupportedOperationException(s"Unsupported, parseMultipartIdentifier is implemented in Spark >= 3.0.0")
  }

}
