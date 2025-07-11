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

package org.apache.hudi.util

import org.apache.hudi.common.function.{SerializableFunction, SerializableFunctionUnchecked, SerializablePairFunction}
import org.apache.hudi.common.util.collection

import scala.language.implicitConversions

/**
 * Utility allowing for seamless conversion b/w Java/Scala functional primitives
 */
object JFunction {

  def scalaFunction1Noop[T]: T => Unit = _ => {}

  ////////////////////////////////////////////////////////////
  // From Java to Scala
  ////////////////////////////////////////////////////////////

  implicit def toScala[T, R](f: java.util.function.Function[T, R]): T => R =
    (t: T) => f.apply(t)

  ////////////////////////////////////////////////////////////
  // From Scala to Java
  ////////////////////////////////////////////////////////////

  implicit def toJavaSupplier[R](f: () => R): java.util.function.Supplier[R] =
    new java.util.function.Supplier[R] {
      override def get(): R = f.apply()
    }

  implicit def toJavaFunction[T, R](f: Function[T, R]): java.util.function.Function[T, R] =
    new java.util.function.Function[T, R] {
      override def apply(t: T): R = f.apply(t)
    }

  implicit def toJavaSerializableFunction[T, R](f: Function[T, R]): SerializableFunction[T, R] =
    new SerializableFunction[T, R] {
      override def apply(t: T): R = f.apply(t)
    }

  implicit def toJavaSerializableFunctionUnchecked[T, R](f: Function[T, R]): SerializableFunctionUnchecked[T, R] =
    new SerializableFunctionUnchecked[T, R] {
      override def apply(t: T): R = f.apply(t)
    }

  implicit def toJavaSerializableFunctionPairOut[T, K, V](f: Function[T, collection.Pair[K, V]]): SerializablePairFunction[T, K, V] =
    new SerializablePairFunction[T, K, V] {
      override def call(t: T): collection.Pair[K, V] = f.apply(t)
    }

  implicit def toJavaConsumer[T](f: T => Unit): java.util.function.Consumer[T] =
    new java.util.function.Consumer[T] {
      override def accept(t: T): Unit = f.apply(t)
    }

}