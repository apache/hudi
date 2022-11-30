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

package org.apache.spark.sql.hudi

class TestQueryTable extends HoodieSparkSqlTestBase {

  test("Test incremental query with set parameters") {
    val tableName = generateTableName

    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  ts long,
         |  dt string
         |) using hudi
         | partitioned by (dt)
         | options (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = 'cow'
         | )
         |""".stripMargin)
    spark.sql(s"insert into $tableName values (1,'a1', 10, 1000, '2022-11-25')")
    spark.sql(s"insert into $tableName values (2,'a2', 20, 2000, '2022-11-25')")
    spark.sql(s"insert into $tableName values (3,'a3', 30, 3000, '2022-11-26')")
    spark.sql(s"insert into $tableName values (4,'a4', 40, 4000, '2022-12-26')")
    spark.sql(s"insert into $tableName values (5,'a5', 50, 5000, '2022-12-27')")

    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(1, "a1", 10.0, 1000, "2022-11-25"),
      Seq(2, "a2", 20.0, 2000, "2022-11-25"),
      Seq(3, "a3", 30.0, 3000, "2022-11-26"),
      Seq(4, "a4", 40.0, 4000, "2022-12-26"),
      Seq(5, "a5", 50.0, 5000, "2022-12-27")
    )


    import spark.implicits._
    val commits = spark.sql(s"call show_commits(table => '$tableName')").select("commit_time").map(k => k.getString(0)).take(10)
    val beginTime = commits(commits.length - 2)

    spark.sql(s"set hoodie.$tableName.datasource.query.type=incremental")
    spark.sql(s"set hoodie.$tableName.datasource.read.begin.instanttime=$beginTime")
    spark.sql(s"refresh table $tableName")
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(3, "a3", 30.0, 3000, "2022-11-26"),
      Seq(4, "a4", 40.0, 4000, "2022-12-26"),
      Seq(5, "a5", 50.0, 5000, "2022-12-27")
    )

    spark.sql(s"set hoodie.query.use.database=true")
    spark.sql(s"refresh table $tableName")
    val cnt = spark.sql(s"select * from $tableName").count()
    assertResult(5)(cnt)

    spark.sql(s"set hoodie.default.$tableName.datasource.query.type=incremental")
    spark.sql(s"set hoodie.default.$tableName.datasource.read.begin.instanttime=$beginTime")
    val endTime = commits(1)
    spark.sql(s"set hoodie.default.$tableName.datasource.read.end.instanttime=$endTime")
    spark.sql(s"refresh table $tableName")
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(3, "a3", 30.0, 3000, "2022-11-26"),
      Seq(4, "a4", 40.0, 4000, "2022-12-26")
    )

    spark.sql(s"set hoodie.default.$tableName.datasource.read.incr.path.glob=/dt=2022-11*/*")
    spark.sql(s"refresh table $tableName")
    checkAnswer(s"select id, name, price, ts, dt from $tableName")(
      Seq(3, "a3", 30.0, 3000, "2022-11-26")
    )
  }
}
