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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.index.vector.VectorIndexMdtSearchUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordGlobalLocation}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.schema.MessageTypeParser
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.nio.file.{Path => NioPath}

class TestParquetRecordKeyLocator {

  @TempDir
  var tempDir: NioPath = _

  @Test
  def testLocateKnownKeyAtPhysicalPositionAndOmitMissingKey(): Unit = {
    val conf = new Configuration
    val schema = MessageTypeParser.parseMessageType(
      s"message test { required binary ${HoodieRecord.RECORD_KEY_METADATA_FIELD} (UTF8); }")
    GroupWriteSupport.setSchema(schema, conf)
    val file = new Path(tempDir.resolve("keys.parquet").toUri)
    val groups = new SimpleGroupFactory(schema)
    val writer = ExampleParquetWriter.builder(file).withConf(conf).build()
    try {
      writer.write(groups.newGroup().append(HoodieRecord.RECORD_KEY_METADATA_FIELD, "key-0"))
      writer.write(groups.newGroup().append(HoodieRecord.RECORD_KEY_METADATA_FIELD, "key-1"))
    } finally {
      writer.close()
    }

    val result = new ParquetRecordKeyLocator(conf).locate(
      file.toString,
      Seq(candidate("key-1"), candidate("missing")))

    assertEquals(1, result.candidates.size)
    assertEquals("key-1", result.candidates.head.getRecordKey)
    assertEquals(1L, result.candidates.head.getLocation.getPosition)
    assertFalse(result.candidates.exists(_.getRecordKey == "missing"))
  }

  private def candidate(recordKey: String): VectorIndexMdtSearchUtils.ScoredPostingMatch = {
    val posting = new VectorIndexMdtSearchUtils.PostingMatch(
      recordKey, 1, 0, "file", "partition", "001", -1L,
      Array[Byte](1), Array[Byte](2), null, null, null)
    new VectorIndexMdtSearchUtils.ScoredPostingMatch(
      posting,
      1.0f,
      new HoodieRecordGlobalLocation("partition", "001", "file", -1L))
  }
}
