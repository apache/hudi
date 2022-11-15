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

package org.apache.hudi

import org.apache.hudi.common.config.TypedProperties

import java.{util => ju}
import scala.collection.JavaConverters

object HoodieConversionUtils {

  /**
   * Converts Java's [[ju.Map]] into Scala's (immutable) [[Map]] (by default [[JavaConverters]] convert to
   * a mutable one)
   */
  def mapAsScalaImmutableMap[K, V](map: ju.Map[K, V]): Map[K, V] = {
    // NOTE: We have to use deprecated [[JavaConversions]] to stay compatible w/ Scala 2.11
    import scala.collection.JavaConversions.mapAsScalaMap
    map.toMap
  }

  def toJavaOption[T](opt: Option[T]): org.apache.hudi.common.util.Option[T] =
    if (opt.isDefined) org.apache.hudi.common.util.Option.of(opt.get) else org.apache.hudi.common.util.Option.empty()

  def toScalaOption[T](opt: org.apache.hudi.common.util.Option[T]): Option[T] =
    if (opt.isPresent) Some(opt.get) else None

  def toProperties(params: Map[String, String]): TypedProperties = {
    val props = new TypedProperties()
    params.foreach(kv => props.setProperty(kv._1, kv._2))
    props
  }

}
