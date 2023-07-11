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

package org.apache.hudi.functional

import org.apache.hudi.common.util.FileIOUtils
import org.apache.spark.sql.hudi.HoodieSparkSqlTestBase

class TestSqlStatement extends HoodieSparkSqlTestBase {
  val STATE_INIT = 0
  val STATE_SKIP_COMMENT = 1
  val STATE_FINISH_COMMENT = 2
  val STATE_READ_SQL = 3
  val STATE_FINISH_READ_SQL = 4
  val STATE_START_FIRST_RESULT_LINE = 5
  val STATE_END_FIRST_RESULT_LINE = 6
  val STATE_READ_RESULT_LINE = 7
  val STATE_FINISH_READ_RESULT_LINE = 8
  val STATE_AFTER_FINISH_READ_RESULT_LINE = 9
  val STATE_START_LAST_RESULT_LINE = 10
  val STATE_END_LAST_RESULT_LINE = 11
  val STATE_FINISH_ALL = 12

  test("Test Sql Statements") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val params = Map(
          "tableType" -> tableType,
          "tmpDir" -> tmp.getCanonicalPath
        )
        execSqlFile("/sql-statements.sql", params)
      }
    }
  }

  private def execSqlFile(sqlFile: String, params: Map[String, String]): Unit = {
    val inputStream = getClass.getResourceAsStream(sqlFile)
    var sqlText = FileIOUtils.readAsUTFString(inputStream)
    // replace parameters in the sql file
    params.foreach { case (k, v) =>
      sqlText = sqlText.replace("${" + k + "}", v)
    }

    var pos = 0
    var state = STATE_INIT
    val sqlBuffer = new StringBuilder
    var sqlResult: String = null
    val sqlExpectResult = new StringBuilder
    val sqlExpectLineResult = new StringBuilder

    while (pos < sqlText.length) {
      var c = sqlText.charAt(pos)
      val (changedState, needFetchNext) = changeState(c, state)
      state = changedState
      pos = pos + 1
      if (needFetchNext) {
        c = sqlText.charAt(pos)
      }
      state match {
        case STATE_READ_SQL =>
         sqlBuffer.append(c)
        case STATE_FINISH_READ_SQL =>
          val sql = sqlBuffer.toString().trim
          try {
            if (sql.startsWith("select")) {
              sqlResult = spark.sql(sql).collect()
                .map(row => row.toSeq.mkString(" ")).mkString("\n")
            } else {
              spark.sql(sql)
              sqlResult = "ok"
            }
          } catch {
            case e: Throwable =>
              throw new RuntimeException(s"Error in execute: $sql", e)
          }
        case STATE_READ_RESULT_LINE =>
          sqlExpectLineResult.append(c)
        case STATE_FINISH_READ_RESULT_LINE =>
          if (sqlExpectResult.nonEmpty) {
            sqlExpectResult.append("\n")
          }
          sqlExpectResult.append(sqlExpectLineResult.toString().trim)
          sqlExpectLineResult.clear()
        case STATE_END_LAST_RESULT_LINE =>
          val expectResult = sqlExpectResult.toString()
            .split("\n").map(line => line.split("\\s+").mkString(" "))
            .mkString("\n")
          if (expectResult != sqlResult) {
            throw new IllegalArgumentException(s"UnExpect result for: $sqlBuffer\n" +
              s"Expect:\n $expectResult, Actual:\n $sqlResult")
          }
          sqlBuffer.clear()
          sqlExpectResult.clear()
          sqlResult = null

        case _=>
      }
    }
    state = STATE_FINISH_ALL
  }

  /**
   * Change current state.
   * @param c Current char.
   * @param state Current state.
   * @return (changedState, needFetchNext)
   */
  private def changeState(c: Char, state: Int): (Int, Boolean) = {
    state match {
      case STATE_INIT | STATE_FINISH_COMMENT |
           STATE_FINISH_READ_SQL | STATE_END_LAST_RESULT_LINE =>
        if (c == '#') {
          (STATE_SKIP_COMMENT, false)
        } else if (c == '+') {
          (STATE_START_FIRST_RESULT_LINE, false)
        } else if (!Character.isWhitespace(c)) {
          (STATE_READ_SQL, false)
        } else {
          (STATE_INIT, false)
        }
      case STATE_SKIP_COMMENT =>
        if (c == '\n' || c == '\r') {
          (STATE_FINISH_COMMENT, false)
        } else {
          (state, false)
        }
      case STATE_READ_SQL =>
        if (c == ';') {
          (STATE_FINISH_READ_SQL, false)
        } else {
          (state, false)
        }
      case STATE_START_FIRST_RESULT_LINE =>
        if (c == '+') {
          (STATE_END_FIRST_RESULT_LINE, false)
        } else {
          (state, false)
        }
      case STATE_END_FIRST_RESULT_LINE =>
        if (c == '|') {
          (STATE_READ_RESULT_LINE, true)
        } else {
          (state, false)
        }
      case STATE_READ_RESULT_LINE =>
        if (c == '|') {
          (STATE_FINISH_READ_RESULT_LINE, false)
        } else {
          (state, false)
        }
      case STATE_FINISH_READ_RESULT_LINE | STATE_AFTER_FINISH_READ_RESULT_LINE =>
        if (c == '+') {
          (STATE_START_LAST_RESULT_LINE, false)
        } else if (c == '|') {
          (STATE_READ_RESULT_LINE, true)
        } else {
          (STATE_AFTER_FINISH_READ_RESULT_LINE, false)
        }
      case STATE_START_LAST_RESULT_LINE =>
        if (c == '+') {
          (STATE_END_LAST_RESULT_LINE, false)
        } else {
          (state, false)
        }
      case _ =>
        throw new IllegalArgumentException(s"Illegal State: $state meet '$c'")
    }
  }
}
