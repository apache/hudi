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

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer


object SerDeUtils {
  private val log = LogManager.getLogger(getClass)

  private val kryoLocal = new ThreadLocal[Kryo] {
    override protected def initialValue: Kryo = {
      val conf = new SparkConf(true)
      if (conf.get("spark.thriftserver.proxy.enabled", "").equals("true")) {
        // Check whether JdbcServer is in multi-tenant mode.
        log.warn("If ArrayIndexOutOfBoundsException occurs in JdbcServer multi-tenant mode, set spark.kryo.referenceTracking to true and " +
          "spark.kryo.classesToRegister to scala.collection.immutable.Nil.")
      }
      val serializer = new KryoSerializer(conf)
      serializer.newKryo()
    }
  }

  def toBytes(o: Any): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream(4096 * 5)
    val output = new Output(outputStream)
    try {
      kryoLocal.get.writeClassAndObject(output, o)
      output.flush()
    } finally {
      output.clear()
      output.close()
    }
    outputStream.toByteArray
  }

  def toObject(bytes: Array[Byte]): Any = {
    val input = new Input(bytes)
    kryoLocal.get.readClassAndObject(input)
  }
}
