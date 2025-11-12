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

package org.apache.spark.sql.hudi.streaming

import org.apache.hudi.common.util.FileIOUtils

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog

import java.io.{BufferedWriter, InputStream, OutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets

/**
 * Hoodie type metadata log that uses the specified path as the metadata storage.
 */
class HoodieMetadataLog(
    sparkSession: SparkSession,
    metadataPath: String) extends HDFSMetadataLog[HoodieSourceOffset](sparkSession, metadataPath) {

  private val VERSION = 1

  override def serialize(metadata: HoodieSourceOffset, out: OutputStream): Unit = {
    val writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))
    writer.write("v" + VERSION + "\n")
    writer.write(metadata.json)
    writer.flush()
  }

  /**
   * Deserialize the init offset from the metadata file.
   * The format in the metadata file is like this:
   * ----------------------------------------------
   * v1         -- The version info in the first line
   * offsetJson -- The json string of HoodieSourceOffset in the rest of the file
   * -----------------------------------------------
   * @param in
   * @return
   */
  override def deserialize(in: InputStream): HoodieSourceOffset = {
    val content = FileIOUtils.readAsUTFString(in)
    // Get version from the first line
    val firstLineEnd = content.indexOf("\n")
    if (firstLineEnd > 0) {
      val version = getVersion(content.substring(0, firstLineEnd))
      if (version > VERSION) {
        throw new IllegalStateException(s"UnSupportVersion: max support version is: $VERSION" +
          s" current version is: $version")
      }
      // Get offset from the rest line in the file
      HoodieSourceOffset.fromJson(content.substring(firstLineEnd + 1))
    } else {
      throw new IllegalStateException(s"Bad metadata format, failed to find the version line.")
    }
  }


  private def getVersion(versionLine: String): Int = {
    if (versionLine.startsWith("v")) {
      versionLine.substring(1).toInt
    } else {
      throw new IllegalStateException(s"Illegal version line: $versionLine " +
        s"in the streaming metadata path")
    }
  }
}
