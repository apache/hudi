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

import org.apache.hudi.common.util.ReflectionUtils

/**
 * Used to catch exceptions from an iterator
 * @param in iterator to catch exceptions from
 * @param exceptionClass name of exception class to throw when an exception is thrown during iteration
 * @param msg message the thrown exception should have
 */
class ExceptionWrappingIterator[T](val in: Iterator[T], val exceptionClass: String, val msg: String) extends Iterator[T] {
  override def hasNext: Boolean = try in.hasNext
  catch {
    case e: Throwable => throw createException(e)
  }

  override def next: T = try in.next
  catch {
    case e: Throwable => throw createException(e)
  }

  private def createException(e: Throwable): Throwable = {
    ReflectionUtils.loadClass(exceptionClass, Array(classOf[String], classOf[Throwable]).asInstanceOf[Array[Class[_]]], msg, e).asInstanceOf[Throwable]
  }
}
