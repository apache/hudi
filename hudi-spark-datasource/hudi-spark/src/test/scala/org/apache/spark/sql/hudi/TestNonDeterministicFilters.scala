package org.apache.spark.sql.hudi

class TestNonDeterministicFilters extends HoodieSparkSqlTestBase {
  test("Test Non deterministic filters") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath + "/" + tableName

      // create table
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price int,
           |  ts int
           |) using hudi
           | location '$tablePath'
           | partitioned by (year string)
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts'
           | )
       """.stripMargin)

      spark.sql(s"""insert into $tableName values (1, 'n1', 10, 111, '2019')""")
      spark.sql(s"""insert into $tableName values (2, 'n2', 20, 222, '2020')""")
      spark.sql(s"""insert into $tableName values (3, 'n3', 30, 333, '2020')""")

      val df = spark.sql(s"select * from $tableName where year = '2020' and rand() > 0.01")
      df.show()
      df.collect()
    }
  }
}
