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

package org.apache.spark.sql.hudi.ddl

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

class TestAlterTableAddPartition extends HoodieSparkSqlTestBase {

  test("Add partition for non-partitioned table") {
    withTable(generateTableName) { tableName =>
      // create table
      spark.sql(
        s"""
           | create table $tableName (
           |  id bigint,
           |  name string,
           |  ts string,
           |  dt string
           | )
           | using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
           |""".stripMargin)

      checkExceptionContain(s"alter table $tableName add partition (dt='2023-08-01')")(
        s"`$tableName` is a non-partitioned table that is not allowed to add partition")

      // show partitions
      checkAnswer(s"show partitions $tableName")(Seq.empty: _*)
    }
  }

  test("Add partition with location") {
    withTable(generateTableName){ tableName =>
      // create table
      spark.sql(
        s"""
           | create table $tableName (
           |  id bigint,
           |  name string,
           |  ts string,
           |  dt string
           | )
           | using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
           | partitioned by (dt)
           |""".stripMargin)

      checkExceptionContain(s"alter table $tableName add partition (dt='2023-08-01') location '/tmp/path'")(
        "Hoodie table does not support specify partition location explicitly")

      // show partitions
      checkAnswer(s"show partitions $tableName")(Seq.empty: _*)
    }
  }

  test("Add partition if not exists") {
    withTable(generateTableName){ tableName =>
      // create table
      spark.sql(
        s"""
           | create table $tableName (
           |  id bigint,
           |  name string,
           |  ts string,
           |  dt string
           | )
           | using hudi
           | tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           | )
           | partitioned by (dt)
           |""".stripMargin)

      spark.sql(s"alter table $tableName add partition (dt='2023-08-01')")
      // show partitions
      checkAnswer(s"show partitions $tableName")(Seq("dt=2023-08-01"))

      // no exception
      spark.sql(s"alter table $tableName add if not exists partition (dt='2023-08-01')")

      checkExceptionContain(s"alter table $tableName add partition (dt='2023-08-01')")(
        "Partition metadata already exists for path")
    }
  }

  Seq(false, true).foreach { hiveStyle =>
    test(s"Add partition for single-partition table, isHiveStylePartitioning: $hiveStyle") {
      withTable(generateTableName){ tableName =>
        // create table
        spark.sql(
          s"""
             | create table $tableName (
             |  id bigint,
             |  name string,
             |  ts string,
             |  dt string
             | )
             | using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.hive_style_partitioning = '$hiveStyle'
             | )
             | partitioned by (dt)
             |""".stripMargin)

        spark.sql(s"alter table $tableName add partition (dt='2023-08-01')")

        // show partitions
        checkAnswer(s"show partitions $tableName")(
          if (hiveStyle) Seq("dt=2023-08-01") else Seq("2023-08-01")
        )
      }
    }

    test(s"Add partition for multi-level partitioned table, isHiveStylePartitioning: $hiveStyle") {
      withTable(generateTableName){ tableName =>
        // create table
        spark.sql(
          s"""
             | create table $tableName (
             |  id bigint,
             |  name string,
             |  ts string,
             |  year string,
             |  month string,
             |  day string
             | )
             | using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.hive_style_partitioning = '$hiveStyle'
             | )
             | partitioned by (year, month, day)
             |""".stripMargin)

        spark.sql(s"alter table $tableName add partition (year='2023', month='08', day='01')")

        // show partitions
        checkAnswer(s"show partitions $tableName")(
          if (hiveStyle) Seq("year=2023/month=08/day=01") else Seq("2023/08/01")
        )
      }
    }
  }

  Seq(false, true).foreach { urlEncode =>
    test(s"Add partition for single-partition table, urlEncode: $urlEncode") {
      withTable(generateTableName){ tableName =>
        // create table
        spark.sql(
          s"""
             | create table $tableName (
             |  id bigint,
             |  name string,
             |  ts string,
             |  p_a string
             | )
             | using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.partitionpath.urlencode = '$urlEncode'
             | )
             | partitioned by (p_a)
             |""".stripMargin)

        spark.sql(s"alter table $tableName add partition (p_a='url%a')")

        // show partitions
        checkAnswer(s"show partitions $tableName")(
          if (urlEncode) Seq("p_a=url%25a") else Seq("p_a=url%a")
        )
      }
    }

    test(s"Add partition for multi-level partitioned table, urlEncode: $urlEncode") {
      withTable(generateTableName){ tableName =>
        // create table
        spark.sql(
          s"""
             | create table $tableName (
             |  id bigint,
             |  name string,
             |  ts string,
             |  p_a string,
             |  p_b string
             | )
             | using hudi
             | tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts',
             |  hoodie.datasource.write.partitionpath.urlencode = '$urlEncode'
             | )
             | partitioned by (p_a, p_b)
             |""".stripMargin)

        spark.sql(s"alter table $tableName add partition (p_a='url%a', p_b='key=val')")

        // show partitions
        checkAnswer(s"show partitions $tableName")(
          if (urlEncode) Seq("p_a=url%25a/p_b=key%3Dval") else Seq("p_a=url%a/p_b=key=val")
        )
      }
    }
  }
}
