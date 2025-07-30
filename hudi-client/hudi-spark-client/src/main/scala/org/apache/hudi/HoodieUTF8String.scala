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

package org.apache.hudi

import org.apache.spark.unsafe.types.UTF8String

import java.io.Serializable

abstract class HoodieUTF8String(utf8String: UTF8String) extends Comparable[HoodieUTF8String] with Serializable {

  def getUtf8String: UTF8String = utf8String

  override def toString: String = utf8String.toString

  override def hashCode(): Int = getUtf8String.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case hoodieUtf8String: HoodieUTF8String => getUtf8String.equals(hoodieUtf8String.getUtf8String)
    case _ => false
  }
}
