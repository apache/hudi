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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{DataSourceWriteOptions, ScalaAssertionSupport}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.hudi.common.ErrorMessageChecker.isIncompatibleDataException

class TestTableColumnTypeMismatch extends HoodieSparkSqlTestBase with ScalaAssertionSupport {

  test("Test Spark successful implicit type casting behaviors") {
    withTempDir { tmp =>
      // Define test cases for successful implicit casting
      case class TypeCastTestCase(
          sourceType: String,
          targetType: String,
          testValue: String,
          expectedValue: Any,
          description: String
      )

      val successfulTestCases = Seq(
        // Numeric widening conversions (always safe)
        TypeCastTestCase("tinyint", "smallint", "127", 127, "tinyint to smallint widening"),
        TypeCastTestCase("tinyint", "int", "127", 127, "tinyint to int widening"),
        TypeCastTestCase("tinyint", "bigint", "127", 127L, "tinyint to bigint widening"),
        TypeCastTestCase("tinyint", "float", "127", 127.0f, "tinyint to float widening"),
        TypeCastTestCase("tinyint", "double", "127", 127.0d, "tinyint to double widening"),
        TypeCastTestCase("tinyint", "decimal(10,1)", "127", java.math.BigDecimal.valueOf(127.0), "tinyint to decimal widening"),

        TypeCastTestCase("smallint", "int", "32767", 32767, "smallint to int widening"),
        TypeCastTestCase("smallint", "bigint", "32767", 32767L, "smallint to bigint widening"),
        TypeCastTestCase("smallint", "float", "32767", 32767.0f, "smallint to float widening"),
        TypeCastTestCase("smallint", "double", "32767", 32767.0d, "smallint to double widening"),
        TypeCastTestCase("smallint", "decimal(10,1)", "32767", java.math.BigDecimal.valueOf(32767.0), "smallint to decimal widening"),

        TypeCastTestCase("int", "bigint", "2147483647", 2147483647L, "int to bigint widening"),
        TypeCastTestCase("int", "float", "2147483647", 2147483647.0f, "int to float widening"),
        TypeCastTestCase("int", "double", "2147483647", 2147483647.0d, "int to double widening"),
        TypeCastTestCase("int", "decimal(10,1)", "22", java.math.BigDecimal.valueOf(22.0), "int to decimal widening"),

        // double value would have some epsilon error which is expected.
        TypeCastTestCase("float", "double", "3.14", 3.140000104904175d, "float to double widening"),
        TypeCastTestCase("float", "decimal(10,2)", "3.14", java.math.BigDecimal.valueOf(3.14).setScale(2, java.math.RoundingMode.HALF_UP), "float to decimal"),

        // Numeric narrowing conversions (potential data loss)
        TypeCastTestCase("double", "int", "123.45", 123, "double to int - truncates decimal"),
        TypeCastTestCase("decimal(10,2)", "int", "123.45", 123, "decimal to int - truncates decimal"),

        // Boolean conversions
        TypeCastTestCase("boolean", "string", "true", "true", "boolean to string"),

        // Timestamp/Date conversions
        TypeCastTestCase("timestamp", "string", "timestamp'2023-01-01 12:00:00'", "2023-01-01 12:00:00", "timestamp to string"),
        TypeCastTestCase("timestamp", "date", "timestamp'2023-01-01 12:00:00'", java.sql.Date.valueOf("2023-01-01"), "timestamp to date"),
        TypeCastTestCase("date", "string", "date'2023-01-01'", "2023-01-01", "date to string"),
        TypeCastTestCase("date", "timestamp", "date'2023-01-01'", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "date to timestamp")
      )

      val tableName = generateTableName

      // Create columns definition dynamically
      val columnsDefinition = successfulTestCases.zipWithIndex.map { case (test, idx) =>
        s"col_${idx} ${test.targetType}"
      }.mkString(",\n  ")

      // Create single table with all target type columns
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  $columnsDefinition,
           |  ts long
           |) using hudi
           |location '${tmp.getCanonicalPath}/$tableName'
           |tblproperties (
           |  primaryKey = 'id',
           |  orderingFields = 'ts'
           |)
         """.stripMargin)

      // Generate insert values
      val insertValues = successfulTestCases.zipWithIndex.map { case (test, idx) =>
        s"cast(${test.testValue} as ${test.sourceType}) as col_${idx}"
      }.mkString(",\n  ")

      // Insert all test values in one go
      spark.sql(
        s"""
           |insert into $tableName
           |select
           |  1 as id,
           |  $insertValues,
           |  1000 as ts
         """.stripMargin)

      // Verify each column value
      val result = spark.sql("select "
        + successfulTestCases.zipWithIndex.map { case (_, idx) => s"col_$idx" }.mkString(",\n  ")
        + s" from $tableName where id = 1").collect()(0)
      successfulTestCases.zipWithIndex.foreach { case (test, idx) =>
        assert(result(idx) == test.expectedValue,
          s"${test.description}: Expected ${test.expectedValue} but got ${result(idx)}")
      }
    }
  }

  test("Test Spark disallowed implicit type casting behaviors") {
    // Capturing the current behavior of Spark's implicit type casting.
    withTempDir { tmp =>
      // Define test cases for implicit casting
      case class TypeCastTestCase(sourceType: String,
                                  targetType: String,
                                  testValue: String, // SQL literal expression
                                  expectedValue: Any,
                                  description: String = "")

      val testCases = Seq(
        TypeCastTestCase("int", "decimal(10,1)", "2147483647", java.math.BigDecimal.valueOf(2147483647.0), "int to decimal widening overflow"),

        // String conversions
        TypeCastTestCase("string", "int", "'123'", 123, "string to int - invalid numeric string"),
        TypeCastTestCase("string", "double", "'12.34'", 12.34d, "string to double - invalid numeric string"),
        TypeCastTestCase("string", "double", "'abc'", null, "string to double - invalid numeric string"),
        TypeCastTestCase("string", "boolean", "'abc'", null, "string to boolean - invalid boolean string"),
        TypeCastTestCase("string", "timestamp", "'2023-01-01'", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "string to timestamp - invalid date string"),
        TypeCastTestCase("string", "date", "'2023-01-01'", java.sql.Date.valueOf("2023-01-01"), "string to date - invalid date string"),

        // Numeric narrowing conversions (potential data loss)
        TypeCastTestCase("double", "int", s"${Int.MaxValue.toDouble + 1}", null, "double to int - overflow"),
        TypeCastTestCase("bigint", "int", "2147483648", null, "bigint to int - overflow"),

        // Boolean conversions
        TypeCastTestCase("boolean", "int", "true", 1, "boolean to int")
      )

      testCases.foreach { testCase =>
        val tableName = generateTableName

        // Create table with target type
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  value ${testCase.targetType},
             |  ts long
             |) using hudi
             |location '${tmp.getCanonicalPath}/$tableName'
             |tblproperties (
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             |)
         """.stripMargin)

        // Test failed conversion
        val exception = intercept[Exception] {
          spark.sql(
            s"""
               |insert into $tableName
               |select 1 as id, cast(${testCase.testValue} as ${testCase.sourceType}) as value, 1000 as ts
           """.stripMargin)
        }

        val exceptionMsg = exception.getMessage
        val exceptionCauseMsg = Option(exception.getCause).map(_.getMessage).getOrElse("")
        assert(isIncompatibleDataException(exception),
          s"${testCase.description}: Expected casting related error but got different exception: " +
            s"Message from the exception ${exceptionMsg}, message from the exception cause ${exceptionCauseMsg}")
      }
    }
  }

  test("Test All Valid Type Casting For Merge Into and Insert") {
    // For all valid type casting pairs, test merge into and insert operations.
    // Define the column types for testing, based on successful casting cases
    case class ColumnTypePair(sourceType: String,
                              targetType: String,
                              testValue: String,
                              expectedValue: Any,
                              columnName: String)

    // Define valid type casting pairs based on the previous test cases
    val validTypePairs = Seq(
      // Numeric widening pairs
      ColumnTypePair("tinyint", "smallint", "127", 127, "tiny_to_small"),
      ColumnTypePair("tinyint", "int", "127", 127, "tiny_to_int"),
      ColumnTypePair("tinyint", "bigint", "127", 127L, "tiny_to_big"),
      ColumnTypePair("tinyint", "float", "127", 127.0f, "tiny_to_float"),
      ColumnTypePair("tinyint", "double", "127", 127.0d, "tiny_to_double"),
      ColumnTypePair("tinyint", "decimal(10,1)", "127", java.math.BigDecimal.valueOf(127.0), "tiny_to_decimal"),

      ColumnTypePair("smallint", "int", "32767", 32767, "small_to_int"),
      ColumnTypePair("smallint", "bigint", "32767", 32767L, "small_to_big"),
      ColumnTypePair("smallint", "float", "32767", 32767.0f, "small_to_float"),
      ColumnTypePair("smallint", "double", "32767", 32767.0d, "small_to_double"),
      ColumnTypePair("smallint", "decimal(10,1)", "32767", java.math.BigDecimal.valueOf(32767.0), "small_to_decimal"),

      ColumnTypePair("int", "bigint", "2147483647", 2147483647L, "int_to_big"),
      ColumnTypePair("int", "float", "2147483647", 2147483647.0f, "int_to_float"),
      ColumnTypePair("int", "double", "2147483647", 2147483647.0d, "int_to_double"),
      ColumnTypePair("int", "decimal(10,1)", "22", java.math.BigDecimal.valueOf(22.0), "int_to_decimal"),

      ColumnTypePair("float", "double", "3.14", 3.140000104904175d, "float_to_double"),
      ColumnTypePair("float", "decimal(10,2)", "3.14", java.math.BigDecimal.valueOf(3.14).setScale(2, java.math.RoundingMode.HALF_UP), "float_to_decimal"),

      // Timestamp/Date conversions
      ColumnTypePair("timestamp", "string", "timestamp'2023-01-01 12:00:00'", "2023-01-01 12:00:00", "ts_to_string"),
      ColumnTypePair("timestamp", "date", "timestamp'2023-01-01 12:00:00'", java.sql.Date.valueOf("2023-01-01"), "ts_to_date"),
      ColumnTypePair("date", "string", "date'2023-01-01'", "2023-01-01", "date_to_string"),
      ColumnTypePair("date", "timestamp", "date'2023-01-01'", java.sql.Timestamp.valueOf("2023-01-01 00:00:00"), "date_to_ts"),

      // Boolean conversions
      ColumnTypePair("boolean", "string", "true", "true", "bool_to_string")
    )

    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val targetTable = generateTableName
        val sourceTable = generateTableName

        // Create column definitions for both tables
        val targetColumns = validTypePairs.map(p => s"${p.columnName} ${p.targetType}").mkString(",\n  ")
        val sourceColumns = validTypePairs.map(p => s"${p.columnName} ${p.sourceType}").mkString(",\n  ")

        // Create target table.
        spark.sql(
          s"""
             |create table $targetTable (
             |  id int,
             |  $targetColumns,
             |  ts long
             |) using hudi
             |location '${tmp.getCanonicalPath}/$targetTable'
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             |)
         """.stripMargin)

        // Create source table
        spark.sql(
          s"""
             |create table $sourceTable (
             |  id int,
             |  $sourceColumns,
             |  ts long
             |) using hudi
             |location '${tmp.getCanonicalPath}/$sourceTable'
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  orderingFields = 'ts'
             |)
         """.stripMargin)

        // Insert initial data into target table
        val targetInsertValues = validTypePairs.map(_ => "null").mkString(", ")
        spark.sql(
          s"""
             |insert into $targetTable
             |select 1 as id, $targetInsertValues, 1000 as ts
         """.stripMargin)

        // Insert data into source table with test values
        val sourceValues = validTypePairs.map(p => s"cast(${p.testValue} as ${p.sourceType})").mkString(", ")
        spark.sql(
          s"""
             |insert into $sourceTable
             |select 1 as id, $sourceValues, 1001 as ts
         """.stripMargin)

        // Perform merge operation
        spark.sql(
          s"""
             |merge into $targetTable t
             |using $sourceTable s
             |on t.id = s.id
             |when matched then update set *
             |when not matched then insert *
         """.stripMargin)

        // Verify results
        val c = validTypePairs.map(p => s"${p.columnName}").mkString(",\n  ")
        val result = spark.sql(s"select $c from $targetTable where id = 1").collect()(0)
        validTypePairs.zipWithIndex.foreach { case (pair, idx) =>
          val actualValue = result.get(idx) // +1 because id is first column
          assert(actualValue == pair.expectedValue,
            s"${tableType.toUpperCase}: Column ${pair.columnName} - Expected ${pair.expectedValue} (${pair.expectedValue.getClass}) but got $actualValue (${if (actualValue != null) actualValue.getClass else "null"})")
        }

        // Test insert case
        val sourceValues2 = validTypePairs.map(p => s"cast(${p.testValue} as ${p.sourceType})").mkString(", ")
        spark.sql(
          s"""
             |insert into $sourceTable
             |select 2 as id, $sourceValues2, 1002 as ts
         """.stripMargin)

        spark.sql(
          s"""
             |merge into $targetTable t
             |using $sourceTable s
             |on t.id = s.id
             |when matched then update set *
             |when not matched then insert *
         """.stripMargin)
        // Verify inserted row
        val result2 = spark.sql(s"select * from $targetTable where id = 2").collect()(0)
        validTypePairs.zipWithIndex.foreach { case (pair, idx) =>
          val actualValue = result2.get(idx + 1)
          assert(actualValue != pair.expectedValue,
            s"${tableType.toUpperCase}: Insert - Column ${pair.columnName} - Expected ${pair.expectedValue} (${pair.expectedValue.getClass}) but got $actualValue (${if (actualValue != null) actualValue.getClass else "null"})")
        }
      }
    }
  }

  test("Test Column Type Mismatches for MergeInto Delete Actions") {
    Seq("mor").foreach { tableType =>
      withTempDir { tmp =>
        def createTargetTable(partitionCol: String, partitionType: String): String = {
          val targetTable = generateTableName
          spark.sql(
            s"""
               |create table $targetTable (
               |  id long,
               |  name string,
               |  value_double double,
               |  ts long,
               |  $partitionCol $partitionType
               |) using hudi
               |partitioned by ($partitionCol)
               |location '${tmp.getCanonicalPath}/$targetTable'
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  orderingFields = 'ts'
               |)
         """.stripMargin)
          targetTable
        }

        // Scenario 1: Successful merge with partition column (both partition and pk can be cast)
        {
          val targetTable = createTargetTable("part_col", "long")

          // Insert initial data into target table
          spark.sql(
            s"""
               |insert into $targetTable
               |select
               |  cast(id as long) as id,
               |  name,
               |  value_double,
               |  ts,
               |  cast(part_col as long) as part_col
               |from (
               |  select 1 as id, 'record1' as name, 1.1 as value_double, 1000 as ts, 100 as part_col
               |  union all
               |  select 2 as id, 'record2' as name, 2.2 as value_double, 1000 as ts, 200 as part_col
               |)
         """.stripMargin)

          // Merge using inline subquery instead of source table
          spark.sql(
            s"""
               |merge into $targetTable t
               |using (
               |  select
               |    cast(1 as int) as id,
               |    cast('updated1' as string) as name,
               |    cast(1.11 as double) as value_double,
               |    cast(1001 as long) as ts,
               |    cast(100 as int) as part_col,
               |    cast('Y' as string) as delete_flag
               |) s
               |on t.id = s.id and t.part_col = s.part_col
               |when matched and s.delete_flag = 'Y' then delete
         """.stripMargin)

          checkAnswer(s"select id, name, value_double, ts, part_col from $targetTable order by id")(
            Seq(2L, "record2", 2.2, 1000L, 200L))
        }

        // Scenario 2: Partition column type not cast-able
        {
          val targetTable = createTargetTable("part_col", "boolean")

          // Insert initial data into target table with boolean partition
          spark.sql(
            s"""
               |insert into $targetTable
               |select
               |  cast(id as long) as id,
               |  name,
               |  value_double,
               |  ts,
               |  true as part_col
               |from (
               |  select 1 as id, 'record1' as name, 1.1 as value_double, 1000 as ts
               |)
         """.stripMargin)

          val sourceTableSubQuery =
            s"""
               |  select
               |    cast(1 as int) as id,
               |    cast('updated1' as string) as name,
               |    cast(1.11 as double) as value_double,
               |    cast(1001 as long) as ts,
               |    cast('2024-01-01' as date) as part_col,
               |    cast('Y' as string) as delete_flag
               |""".stripMargin
          // Should fail with cast related error due to incompatible partition types
          val e1 = intercept[Exception] {
            spark.sql(
              s"""
                 |merge into $targetTable t
                 |using ($sourceTableSubQuery) s
                 |on t.id = s.id and t.part_col = s.part_col
                 |when matched and s.delete_flag = 'Y' then delete
           """.stripMargin)
          }
          assert(
            e1.getMessage.contains(
              "the left and right operands of the binary operator have incompatible types " +
                "(\"BOOLEAN\" and \"DATE\")")
            || e1.getMessage.contains(
              "cannot resolve '(t.part_col = s.part_col)' due to data type mismatch: differing types" +
                " in '(t.part_col = s.part_col)' (boolean and date)."))

          spark.sql(
            s"""
               |merge into $targetTable t
               |using ($sourceTableSubQuery) s
               |on t.id = s.id
               |when matched and s.delete_flag = 'Y' then delete
           """.stripMargin)

          checkAnswer(s"select id, name, value_double, ts, part_col from $targetTable order by id")(
            Seq(1L, "record1", 1.1, 1000L, true))
        }

        // Scenario 4: Failed merge due to primary key type mismatch
        {
          val targetTable = createTargetTable("part_col", "long")

          // Insert initial data
          spark.sql(
            s"""
               |insert into $targetTable
               |select
               |  cast(id as long) as id,
               |  name,
               |  value_double,
               |  ts,
               |  part_col
               |from (
               |  select 1 as id, 'record1' as name, 1.1 as value_double, 1000 as ts, 100 as part_col
               |)
         """.stripMargin)

          val e2 = intercept[Exception] {
            spark.sql(
              s"""
                 |merge into $targetTable t
                 |using (
                 |  select
                 |    cast(1.0 as double) as id,
                 |    cast('updated1' as string) as name,
                 |    cast(1.11 as double) as value_double,
                 |    cast(1001 as long) as ts,
                 |    cast(100 as long) as part_col,
                 |    cast('Y' as string) as delete_flag
                 |) s
                 |on t.id = s.id
                 |when matched and s.delete_flag = 'Y' then delete
           """.stripMargin)
          }
          assert(e2.getMessage.contains("Invalid MERGE INTO matching condition: s.id: can't cast s.id (of DoubleType) to LongType"))
        }
      }
    }
  }

  test("Test Column Type Mismatches for MergeInto Insert and Update Actions") {
    // Define test cases
    case class TypeMismatchTestCase(
                                     description: String,
                                     targetSchema: Seq[(String, String)], // (colName, colType)
                                     sourceSchema: Seq[(String, String)],
                                     partitionCols: Seq[String],
                                     primaryKey: String,
                                     orderingFields: String,
                                     tableType: String, // COW or MOR
                                     expectedErrorPattern: String
                                   )

    val testCases = Seq(
      TypeMismatchTestCase(
        description = "Partition column type mismatch",
        targetSchema = Seq(
          "id" -> "int",
          "name" -> "string",
          "price" -> "int",
          "ts" -> "long"
        ),
        sourceSchema = Seq(
          "id" -> "int",
          "name" -> "int", // mismatched type
          "price" -> "int",
          "ts" -> "long"
        ),
        partitionCols = Seq("name", "price"),
        primaryKey = "id",
        orderingFields = "ts",
        tableType = "cow",
        expectedErrorPattern = "Partition key data type mismatch between source table and target table. Target table uses StringType for column 'name', source table uses IntegerType for 's0.name'"
      ),
      TypeMismatchTestCase(
        description = "Primary key type mismatch",
        targetSchema = Seq(
          "id" -> "int",
          "name" -> "string",
          "price" -> "int",
          "ts" -> "long"
        ),
        sourceSchema = Seq(
          "id" -> "long", // mismatched type
          "name" -> "string",
          "price" -> "int",
          "ts" -> "long"
        ),
        partitionCols = Seq("name", "price"),
        primaryKey = "id",
        orderingFields = "ts",
        tableType = "mor",
        expectedErrorPattern = "Primary key data type mismatch between source table and target table. Target table uses IntegerType for column 'id', source table uses LongType for 's0.id'"
      ),
      TypeMismatchTestCase(
        description = "Precombine field type mismatch",
        targetSchema = Seq(
          "id" -> "int",
          "name" -> "string",
          "price" -> "int",
          "ts" -> "long"
        ),
        sourceSchema = Seq(
          "id" -> "int",
          "name" -> "string",
          "price" -> "int",
          "ts" -> "int" // mismatched type
        ),
        partitionCols = Seq("name", "price"),
        primaryKey = "id",
        orderingFields = "ts",
        tableType = "cow",
        expectedErrorPattern = "Ordering field data type mismatch between source table and target table. Target table uses LongType for column 'ts', source table uses IntegerType for 's0.ts'"
      )
    )

    def createTable(tableName: String, schema: Seq[(String, String)], partitionCols: Seq[String],
                    primaryKey: String, orderingFields: String, tableType: String, location: String): Unit = {
      val schemaStr = schema.map { case (name, dataType) => s"$name $dataType" }.mkString(",\n  ")
      val partitionColsStr = if (partitionCols.nonEmpty) s"partitioned by (${partitionCols.mkString(", ")})" else ""

      spark.sql(
        s"""
           |create table $tableName (
           |  $schemaStr
           |) using hudi
           |$partitionColsStr
           |location '$location'
           |tblproperties (
           |  type = '$tableType',
           |  primaryKey = '$primaryKey',
           |  orderingFields = '$orderingFields'
           |)
       """.stripMargin)
    }

    // Run test cases
    testCases.foreach { testCase =>
      withSparkSqlSessionConfig(s"${DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key}" -> "false") {
        withTempDir { tmp =>
          val targetTable = generateTableName

          // Create only target table
          createTable(
            targetTable,
            testCase.targetSchema,
            testCase.partitionCols,
            testCase.primaryKey,
            testCase.orderingFields,
            testCase.tableType,
            s"${tmp.getCanonicalPath}/$targetTable"
          )

          // Insert sample data into target table
          spark.sql(
            s"""
               |insert into $targetTable
               |select 1 as id, 'John Doe' as name, 19 as price, 1598886000 as ts
               |union all
               |select 2, 'Jane Doe', 24, 1598972400
           """.stripMargin)

          // Test UPDATE action with inline subquery
          val updateQuery =
            s"""
               |merge into $targetTable t
               |using (
               |  select
               |    cast(1 as ${testCase.sourceSchema.find(_._1 == "id").get._2}) as id,
               |    cast('John Doe' as ${testCase.sourceSchema.find(_._1 == "name").get._2}) as name,
               |    cast(20 as ${testCase.sourceSchema.find(_._1 == "price").get._2}) as price,
               |    cast(1598886001 as ${testCase.sourceSchema.find(_._1 == "ts").get._2}) as ts
               |) s0
               |on t.${testCase.primaryKey} = s0.${testCase.primaryKey}
               |when matched then update set *
             """.stripMargin

          val updateError = intercept[AnalysisException] {
            spark.sql(updateQuery)
          }.getMessage

          assert(updateError.contains(testCase.expectedErrorPattern),
            s"UPDATE - Expected error pattern '${testCase.expectedErrorPattern}' not found in actual error: $updateError")

          // Test INSERT action with inline subquery
          val insertQuery =
            s"""
               |merge into $targetTable t
               |using (
               |  select
               |    cast(3 as ${testCase.sourceSchema.find(_._1 == "id").get._2}) as id,
               |    cast('Bob Smith' as ${testCase.sourceSchema.find(_._1 == "name").get._2}) as name,
               |    cast(30 as ${testCase.sourceSchema.find(_._1 == "price").get._2}) as price,
               |    cast(1598886002 as ${testCase.sourceSchema.find(_._1 == "ts").get._2}) as ts
               |) s0
               |on t.${testCase.primaryKey} = s0.${testCase.primaryKey}
               |when not matched then insert *
             """.stripMargin

          val insertError = intercept[AnalysisException] {
            spark.sql(insertQuery)
          }.getMessage

          assert(insertError.contains(testCase.expectedErrorPattern),
            s"INSERT - Expected error pattern '${testCase.expectedErrorPattern}' not found in actual error: $insertError")
        }
      }
    }
  }

  test("Test MergeInto with partition column type mismatch should throw") {
    withSparkSqlSessionConfig(s"${DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key}" -> "false") {
      withTempDir { tmp =>
        val targetTable = generateTableName

        // Create target table with string partition
        spark.sql(
          s"""
             |create table $targetTable (
             |  id int,
             |  name long,
             |  ts int
             |) using hudi
             |partitioned by (name)
             |location '${tmp.getCanonicalPath}/$targetTable'
             |tblproperties (
             |  type = 'cow',
             |  primaryKey = 'id'
             |)
         """.stripMargin)

        // Insert sample data
        spark.sql(
          s"""
             |insert into $targetTable
             |select 1 as id, 124L as name, 1000 as ts
       """.stripMargin)

        val e = intercept[AnalysisException] {
          spark.sql(
            s"""
               |merge into $targetTable t
               |using (
               |  select
               |    cast(1 as int) as id,
               |    cast(123 as int) as name,
               |    cast(1001 as long) as ts
               |) s
               |on t.id = s.id
               |when matched then update set name = s.name
         """.stripMargin)
        }
        assert(e.getMessage.contains("data type mismatch between source table and target table"))
      }
    }
  }

  test("Test MergeInto with precombine column type mismatch behavior based on record.merge.mode") {
    withSparkSqlSessionConfig(s"${DataSourceWriteOptions.ENABLE_MERGE_INTO_PARTIAL_UPDATES.key}" -> "false") {
      withTempDir { tmp =>
        Seq("EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING").foreach { mergeMode =>
          val targetTable = generateTableName

          // Create target table with int ts
          spark.sql(
            s"""
               |create table $targetTable (
               |  id int,
               |  name string,
               |  ts int
               |) using hudi
               |partitioned by (name)
               |location '${tmp.getCanonicalPath}/$targetTable'
               |tblproperties (
               |  type = 'cow',
               |  primaryKey = 'id',
               |  orderingFields = 'ts',
               |  'hoodie.record.merge.mode' = '$mergeMode'
               |)
           """.stripMargin)

          // Insert sample data
          spark.sql(
            s"""
               |insert into $targetTable
               |select 1 as id, 'John' as name, 1000 as ts
         """.stripMargin)

          if (mergeMode == "EVENT_TIME_ORDERING") {
            // Should throw exception for EVENT_TIME_ORDERING
            val e = intercept[AnalysisException] {
              spark.sql(
                s"""
                   |merge into $targetTable t
                   |using (
                   |  select
                   |    cast(1 as int) as id,
                   |    cast('John' as string) as name,
                   |    cast(1001 as long) as ts
                   |) s
                   |on t.id = s.id
                   |when matched then update set ts = s.ts
               """.stripMargin)
            }
            assert(e.getMessage.contains("data type mismatch between source table and target table"))
          } else {
            // Should succeed for COMMIT_TIME_ORDERING
            spark.sql(
              s"""
                 |merge into $targetTable t
                 |using (
                 |  select
                 |    cast(1 as int) as id,
                 |    cast('John' as string) as name,
                 |    cast(1001 as long) as ts
                 |) s
                 |on t.id = s.id
                 |when matched then update set ts = s.ts
             """.stripMargin)

            // Verify the update succeeded
            checkAnswer(s"select id, name, ts from $targetTable where id = 1")(
              Seq(1, "John", 1001)
            )
          }
        }
      }
    }
  }

  test("Test Type Casting with Global Index for Primary Key and Partition Key Updates") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        withSQLConf("hoodie.index.type" -> "GLOBAL_SIMPLE",
          "hoodie.simple.index.update.partition.path" -> "true") {
          val tableName = generateTableName

          // Create table with both primary key and partition key
          spark.sql(
            s"""
               |create table $tableName (
               |  c1 int,
               |  c2 int,
               |  c3 string,
               |  ts long
               |) using hudi
               |partitioned by (c2)
               |location '${tmp.getCanonicalPath}/$tableName'
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'c1',
               |  orderingFields = 'ts'
               |)
           """.stripMargin)

          // Test Case 1: Initial insert with double values
          spark.sql(
            s"""
               |insert into $tableName
               |select
               |  cast(1.0 as double) as c1,
               |  cast(1.0 as double) as c2,
               |  'a' as c3,
               |  1000 as ts
           """.stripMargin)

          // Verify initial insert
          checkAnswer(
            s"select c1, c2, c3 from $tableName")(
            Seq(1, 1, "a")
          )

          // Test Case 2: Insert overwrite with double values
          spark.sql(
            s"""
               |insert overwrite table $tableName
               |select
               |  cast(1.4 as double) as c1,
               |  cast(3.2 as double) as c2,
               |  'a' as c3,
               |  1003 as ts
           """.stripMargin)

          // Verify final state after insert overwrite
          checkAnswer(
            s"select c1, c2, c3 from $tableName")(
            Seq(1, 3, "a")
          )
        }
      }
    }
  }
}

object ErrorMessageChecker {
  private val incompatibleDataPatterns = Set(
    "Cannot write incompatible data to table",
    "overflow",
    "cannot be cast",
    "Cannot safely cast",
    "Conversion of",
    "Failed to parse",
    "cannot be represented as Decimal"
  )

  def containsIncompatibleDataError(message: String): Boolean = {
    incompatibleDataPatterns.exists(message.contains)
  }

  def isIncompatibleDataException(exception: Exception): Boolean = {
    containsIncompatibleDataError(exception.getMessage) ||
      Option(exception.getCause)
        .exists(cause => containsIncompatibleDataError(cause.getMessage))
  }
}
