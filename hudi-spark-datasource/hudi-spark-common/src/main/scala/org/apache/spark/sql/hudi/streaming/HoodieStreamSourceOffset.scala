
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
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

case class HoodieStreamSourceOffset(commitTime: String, partition: String, cursor: Int)  extends Offset {

  override def  json: String = {
    HoodieStreamSourceOffset.toJson(this)
  }
}


object HoodieStreamSourceOffset {
  lazy val mapper: ObjectMapper = {
    val _mapper = new ObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def toJson(offset: HoodieStreamSourceOffset): String = {
    mapper.writeValueAsString(offset)
  }

  def fromJson(json: String): HoodieStreamSourceOffset = {
    mapper.readValue(json, classOf[HoodieStreamSourceOffset])
  }

  def apply(offset: Offset): HoodieStreamSourceOffset = {
    offset match {
      case SerializedOffset(json) => fromJson(json)
      case o: HoodieStreamSourceOffset => o
    }
  }
}
