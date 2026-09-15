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

package org.apache.spark.sql

import org.apache.hudi.HoodieUTF8String

import org.apache.spark.unsafe.types.UTF8String

import java.nio.ByteBuffer

trait HoodieUTF8StringFactory extends Serializable {

  def wrapUTF8String(utf8String: UTF8String): HoodieUTF8String

  /**
   * Wrap a sort-column value that is not directly [[Comparable]] on the Spark record path so that
   * it can be used as a clustering/bulk-insert sort key (see [[org.apache.hudi.common.util.SortUtils]]
   * -> [[org.apache.hudi.common.util.collection.FlatLists.ofComparableArray]], which casts every
   * element to Comparable). Two engine types reach here as non-Comparable Java values:
   *  - Spark strings arrive as [[UTF8String]] (whose natural ordering differs from the Avro path),
   *    wrapped into a version-specific [[HoodieUTF8String]].
   *  - Spark binary columns arrive as a raw byte[] (NOT Comparable), which threw
   *    `ClassCastException: [B cannot be cast to java.lang.Comparable`. The Avro path yields a
   *    java.nio.ByteBuffer for the same column, so wrapping byte[] with ByteBuffer.wrap keeps the
   *    exact same (byte-lexicographic) ordering while being Comparable and shuffle-serializable.
   */
  private def wrapComparableIfNecessary(obj: AnyRef): AnyRef = {
    obj match {
      case string: UTF8String => wrapUTF8String(string)
      case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
      case _ => obj
    }
  }

  def wrapArrayOfObjects(objects: Array[AnyRef]): Array[AnyRef] = {
    objects.map(obj => wrapComparableIfNecessary(obj))
  }
}
