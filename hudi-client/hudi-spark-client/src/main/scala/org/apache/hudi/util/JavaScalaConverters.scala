/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.util

import scala.collection.JavaConverters._

/**
 * Utils that do conversion between Java and Scala collections, used by classes in Java code only.
 * For classes in Scala code, import `scala.collection.JavaConverters._` directly.
 */
object JavaScalaConverters {
  /**
   * @param scalaList list in Scala [[Seq]].
   * @tparam A type of item.
   * @return list in [[java.util.List]].
   */
  def convertScalaListToJavaList[A](scalaList: Seq[A]): java.util.List[A] = {
    scalaList.asJava
  }

  /**
   * @param javaList list in [[java.util.List]].
   * @tparam A type of item.
   * @return list in Scala immutable [[List]].
   */
  def convertJavaListToScalaList[A](javaList: java.util.List[A]): List[A] = {
    javaList.asScala.toList
  }

  /**
   * @param javaList list in [[java.util.List]].
   * @tparam A type of item.
   * @return list in Scala [[Seq]].
   */
  def convertJavaListToScalaSeq[A](javaList: java.util.List[A]): Seq[A] = {
    javaList.asScala.toSeq
  }

  /**
   * @param javaIterator iterator in [[java.util.Iterator]]
   * @tparam A type of item.
   * @return iterator in Scala [[Iterator]].
   */
  def convertJavaIteratorToScalaIterator[A](javaIterator: java.util.Iterator[A]): Iterator[A] = {
    javaIterator.asScala
  }
}
