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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.Objects

/**
 * This class is to make scala-2.11 compilable.
 * Using Identifier.of(namespace, name) to get a IdentifierImpl will throw
 * compile exception( Static methods in interface require -target:jvm-1.8)
 */
case class HoodieIdentifier(namespace: Array[String], name: String) extends Identifier {

  override def equals(o: Any): Boolean = {
    o match {
      case that: HoodieIdentifier => util.Arrays.equals(namespace.asInstanceOf[Array[Object]],
        that.namespace.asInstanceOf[Array[Object]]) && name == that.name
      case _ => false
    }
  }

  override def hashCode: Int = {
    val nh = namespace.toSeq.hashCode().asInstanceOf[Object]
    Objects.hash(nh, name)
  }
}
