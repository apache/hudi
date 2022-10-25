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

package org.apache.spark.sql.hudi

import org.apache.hudi.common.util.BinaryUtil
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{KryoSerializer, SerializerInstance}

import java.nio.ByteBuffer


object SerDeUtils {

  private val SERIALIZER_THREAD_LOCAL = new ThreadLocal[SerializerInstance] {

    override protected def initialValue: SerializerInstance = {
      new KryoSerializer(new SparkConf(true)).newInstance()
    }
  }

  def toBytes(o: Any): Array[Byte] = {
    val buf = SERIALIZER_THREAD_LOCAL.get.serialize(o)
    BinaryUtil.toBytes(buf)
  }

  def toObject(bytes: Array[Byte]): Any = {
    SERIALIZER_THREAD_LOCAL.get.deserialize(ByteBuffer.wrap(bytes))
  }
}
