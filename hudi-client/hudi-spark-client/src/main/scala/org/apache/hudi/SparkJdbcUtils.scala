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

package org.apache.hudi

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.StructType

import java.sql.ResultSet

/**
 * Util functions for JDBC source and tables in Spark.
 */
object SparkJdbcUtils {
  /**
   * Takes a [[ResultSet]] and returns its Catalyst schema.
   *
   * @param resultSet      [[ResultSet]] instance.
   * @param dialect        [[JdbcDialect]] instance.
   * @param alwaysNullable If true, all the columns are nullable.
   * @return A [[StructType]] giving the Catalyst schema.
   */
  def getSchema(resultSet: ResultSet,
                dialect: JdbcDialect,
                alwaysNullable: Boolean): StructType = {
    // NOTE: Since Spark 3.4.0, the function signature of [[JdbcUtils.getSchema]] is changed
    // to have four arguments. Although calling this function in Scala with the first three
    // arguments works, with the fourth argument using the default value, this breaks the
    // Java code as Java cannot use the default argument value and has to use four arguments.
    // Instead of calling this function from Java code directly, we create this util function
    // in Scala for compatibility in Hudi code.
    //
    // Before Spark 3.4.0 (three arguments):
    // def getSchema(
    //      resultSet: ResultSet,
    //      dialect: JdbcDialect,
    //      alwaysNullable: Boolean = false): StructType
    //
    // Since Spark 3.4.0 (four arguments):
    // def getSchema(
    //      resultSet: ResultSet,
    //      dialect: JdbcDialect,
    //      alwaysNullable: Boolean = false,
    //      isTimestampNTZ: Boolean = false): StructType
    JdbcUtils.getSchema(resultSet, dialect, alwaysNullable)
  }
}
