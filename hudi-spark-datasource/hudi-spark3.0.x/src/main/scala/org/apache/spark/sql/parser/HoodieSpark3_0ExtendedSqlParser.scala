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

package org.apache.spark.sql.parser

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.{SparkSqlAstBuilder, SparkSqlParser}

// TODO: we should remove this file when we support datasourceV2 for hoodie on spark3.0.x
class HoodieSpark3_0ExtendedSqlParser(session: SparkSession, delegate: ParserInterface) extends SparkSqlParser(session.sqlContext.conf)
  with HoodieExtendedParserInterface
  with Logging {

  override val astBuilder: SparkSqlAstBuilder = new HoodieSpark30SqlAstBuilder(session.sqlContext.conf)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] = super[SparkSqlParser].parseMultipartIdentifier(sqlText)
}

