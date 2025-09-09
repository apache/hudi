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

package org.apache.spark.sql.hudi.procedure

class TestAuditLockSetProcedure extends HoodieSparkProcedureTestBase {

  test("Test Call audit_lock_set Procedure - Enable Audit") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Test enabling audit
      val result1 = spark.sql(s"""call audit_lock_set(table => '$tableName', state => 'enabled')""").collect()
      assertResult(1) {
        result1.length
      }
      assertResult(tableName) {
        result1.head.get(0)
      }
      assertResult("enabled") {
        result1.head.get(1)
      }
      assert(result1.head.get(2).toString.contains("successfully enabled"))
    }
  }

  test("Test Call audit_lock_set Procedure - Disable Audit") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Test disabling audit
      val result1 = spark.sql(s"""call audit_lock_set(table => '$tableName', state => 'disabled')""").collect()
      assertResult(1) {
        result1.length
      }
      assertResult(tableName) {
        result1.head.get(0)
      }
      assertResult("disabled") {
        result1.head.get(1)
      }
      assert(result1.head.get(2).toString.contains("successfully disabled"))
    }
  }

  test("Test Call audit_lock_set Procedure - Invalid State Parameter") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Test invalid state parameter
      checkExceptionContain(s"""call audit_lock_set(table => '$tableName', state => 'invalid')""")(
        "State parameter must be 'enabled' or 'disabled'")
    }
  }

  test("Test Call audit_lock_set Procedure - Missing Required Arguments") {
    withTempDir { tmp =>
      val tableName = generateTableName
      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | location '${tmp.getCanonicalPath}/$tableName'
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
       """.stripMargin)

      // Test missing table parameter
      checkExceptionContain(s"""call audit_lock_set(state => 'enabled')""")(
        "Argument: table is required")

      // Test missing state parameter
      checkExceptionContain(s"""call audit_lock_set(table => '$tableName')""")(
        "Argument: state is required")
    }
  }
}
