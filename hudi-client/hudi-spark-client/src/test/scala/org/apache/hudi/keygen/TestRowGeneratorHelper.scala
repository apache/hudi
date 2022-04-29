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

package org.apache.hudi.keygen

import java.sql.Timestamp

import org.apache.spark.sql.Row

import org.apache.hudi.keygen.RowKeyGeneratorHelper._

import org.junit.jupiter.api.{Assertions, Test}

import scala.collection.JavaConverters._

class TestRowGeneratorHelper {

  @Test
  def testGetPartitionPathFromRow(): Unit = {

    /** single plain partition */
    val row1 = Row.fromSeq(Seq(1, "z3", 10.0, "20220108"))
    val ptField1 = List("dt").asJava
    val ptPos1 = Map("dt" -> List(new Integer(3)).asJava).asJava
    Assertions.assertEquals("20220108",
      getPartitionPathFromRow(row1, ptField1, false, ptPos1))
    Assertions.assertEquals("dt=20220108",
      getPartitionPathFromRow(row1, ptField1, true, ptPos1))

    /** multiple plain partitions */
    val row2 = Row.fromSeq(Seq(1, "z3", 10.0, "2022", "01", "08"))
    val ptField2 = List("year", "month", "day").asJava
    val ptPos2 = Map("year" -> List(new Integer(3)).asJava,
      "month" -> List(new Integer(4)).asJava,
      "day" -> List(new Integer(5)).asJava
    ).asJava
    Assertions.assertEquals("2022/01/08",
      getPartitionPathFromRow(row2, ptField2, false, ptPos2))
    Assertions.assertEquals("year=2022/month=01/day=08",
      getPartitionPathFromRow(row2, ptField2, true, ptPos2))

    /** multiple partitions which contains TimeStamp type or Instant type */
    val timestamp = Timestamp.valueOf("2020-01-08 10:00:00")
    val instant = timestamp.toInstant
    val ptField3 = List("event", "event_time").asJava
    val ptPos3 = Map("event" -> List(new Integer(3)).asJava,
      "event_time" -> List(new Integer(4)).asJava
    ).asJava

    // with timeStamp type
    val row2_ts = Row.fromSeq(Seq(1, "z3", 10.0, "click", timestamp))
    Assertions.assertEquals("click/2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row2_ts, ptField3, false, ptPos3))
    Assertions.assertEquals("event=click/event_time=2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row2_ts, ptField3, true, ptPos3))

    // with instant type
    val row2_instant = Row.fromSeq(Seq(1, "z3", 10.0, "click", instant))
    Assertions.assertEquals("click/2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row2_instant, ptField3, false, ptPos3))
    Assertions.assertEquals("event=click/event_time=2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row2_instant, ptField3, true, ptPos3))

    /** mixed case with plain and nested partitions */
    val nestedRow4 = Row.fromSeq(Seq(instant, "ad"))
    val ptField4 = List("event_time").asJava
    val ptPos4 = Map("event_time" -> List(new Integer(3), new Integer(0)).asJava).asJava
    // with instant type
    val row4 = Row.fromSeq(Seq(1, "z3", 10.0, nestedRow4, "click"))
    Assertions.assertEquals("2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row4, ptField4, false, ptPos4))
    Assertions.assertEquals("event_time=2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row4, ptField4, true, ptPos4))

    val nestedRow5 = Row.fromSeq(Seq(timestamp, "ad"))
    val ptField5 = List("event", "event_time").asJava
    val ptPos5 = Map(
      "event_time" -> List(new Integer(3), new Integer(0)).asJava,
      "event" -> List(new Integer(4)).asJava
    ).asJava
    val row5 = Row.fromSeq(Seq(1, "z3", 10.0, nestedRow5, "click"))
    Assertions.assertEquals("click/2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row5, ptField5, false, ptPos5))
    Assertions.assertEquals("event=click/event_time=2020-01-08 10:00:00.0",
      getPartitionPathFromRow(row5, ptField5, true, ptPos5))
  }
}
