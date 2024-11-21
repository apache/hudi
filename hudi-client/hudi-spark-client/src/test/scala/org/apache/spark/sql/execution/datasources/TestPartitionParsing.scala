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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import java.lang.Integer

class TestPartitionParsing {

  @Test
  def testSingleFieldParsing(): Unit = {
    val partitionPath = "105"
    val partitionSchema = new StructType().add("partition", IntegerType)

    val output = BetterParsePartitionUtil.doParsePartition(partitionPath, partitionSchema, null, null, None)
    assertEquals(1, output.length)
    assertEquals(new Integer(105), output(0))
  }

  @Test
  def testSingleFieldParsingHiveStyle(): Unit = {
    val partitionPath = "partition=105"
    val partitionSchema = new StructType().add("partition", IntegerType)

    val output = BetterParsePartitionUtil.doParsePartition(partitionPath, partitionSchema, null, null, None)
    assertEquals(1, output.length)
    assertEquals(new Integer(105), output(0))
  }

  @Test
  def testMultiFieldParsing(): Unit = {
    val partitionPath = "105/abcd/123"
    val partitionSchema = new StructType()
      .add("partition", IntegerType)
      .add("strpartition", StringType)
      .add("longpartition", LongType)

    val output = BetterParsePartitionUtil.doParsePartition(partitionPath, partitionSchema, null, null, None)
    assertEquals(3, output.length)
    assertEquals(new Integer(105), output(0))
    assertEquals(UTF8String.fromString("abcd"), output(1))
    assertEquals(Long.box(123), output(2))
  }

  @Test
  def testMultiFieldParsingWithHiveStyle(): Unit = {
    val partitionPath = "partition=105/strpartition=abcd/longpartition=123"
    val partitionSchema = new StructType()
      .add("partition", IntegerType)
      .add("strpartition", StringType)
      .add("longpartition", LongType)

    val output = BetterParsePartitionUtil.doParsePartition(partitionPath, partitionSchema, null, null, None)
    assertEquals(3, output.length)
    assertEquals(new Integer(105), output(0))
    assertEquals(UTF8String.fromString("abcd"), output(1))
    assertEquals(Long.box(123), output(2))
  }
}
