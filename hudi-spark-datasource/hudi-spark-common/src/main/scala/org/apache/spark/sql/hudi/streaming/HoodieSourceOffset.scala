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

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.table.timeline.HoodieTimeline

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.execution.streaming.Offset

case class HoodieSourceOffset(offsetCommitTime: String) extends Offset {

  override val json: String = {
    HoodieSourceOffset.toJson(this)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case HoodieSourceOffset(otherCompletionTime) =>
        otherCompletionTime == offsetCommitTime
      case _=> false
    }
  }

  override def hashCode(): Int = {
    offsetCommitTime.hashCode
  }
}


object HoodieSourceOffset extends SparkAdapterSupport {

  lazy val mapper: ObjectMapper = {
    val _mapper = new ObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def toJson(offset: HoodieSourceOffset): String = {
    mapper.writeValueAsString(offset)
  }

  def fromJson(json: String): HoodieSourceOffset = {
    mapper.readValue(json, classOf[HoodieSourceOffset])
  }

  def apply(offset: Offset): HoodieSourceOffset = {
    offset match {
      case o: HoodieSourceOffset => o
      case _ =>
        // Use adapter to extract JSON from SerializedOffset to handle Spark version differences
        // In Spark 4.1, SerializedOffset moved to runtime package
        sparkAdapter.getCatalystPlanUtils.extractJsonFromSerializedOffset(offset) match {
          case Some(json) => fromJson(json)
          case None => throw new IllegalArgumentException(s"Unsupported offset type: ${offset.getClass}")
        }
    }
  }

  val INIT_OFFSET = HoodieSourceOffset(HoodieTimeline.INIT_INSTANT_TS)
}
