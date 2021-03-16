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

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

case class HoodieSourceOffset(commitTime: String) extends Offset {

  override def json(): String = {
    HoodieSourceOffset.toJson(this)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case HoodieSourceOffset(otherCommitTime) =>
        otherCommitTime == commitTime
      case _=> false
    }
  }

  override def hashCode(): Int = {
    commitTime.hashCode
  }
}


object HoodieSourceOffset {
  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setSerializationInclusion(Include.NON_ABSENT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  def toJson(offset: HoodieSourceOffset): String = {
    mapper.writeValueAsString(offset)
  }

  def fromJson(json: String): HoodieSourceOffset = {
    mapper.readValue[HoodieSourceOffset](json)
  }

  def apply(offset: Offset): HoodieSourceOffset = {
    offset match {
      case SerializedOffset(json) => fromJson(json)
      case o: HoodieSourceOffset => o
    }
  }

  val INIT_OFFSET = HoodieSourceOffset(HoodieTimeline.INIT_INSTANT_TS)
}
