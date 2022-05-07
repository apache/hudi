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

package org.apache.hudi.common.util.jvm;

/**
 * Describes constant memory overheads for various constructs in a JVM implementation.
 */
public interface MemoryLayoutSpecification {

  /**
   * Returns the fixed overhead of an array of any type or length in this JVM.
   *
   * @return the fixed overhead of an array.
   */
  int getArrayHeaderSize();

  /**
   * Returns the fixed overhead of for any {@link Object} subclass in this JVM.
   *
   * @return the fixed overhead of any object.
   */
  int getObjectHeaderSize();

  /**
   * Returns the quantum field size for a field owned by an object in this JVM.
   *
   * @return the quantum field size for an object.
   */
  int getObjectPadding();

  /**
   * Returns the fixed size of an object reference in this JVM.
   *
   * @return the size of all object references.
   */
  int getReferenceSize();

  /**
   * Returns the quantum field size for a field owned by one of an object's ancestor superclasses in this JVM.
   *
   * @return the quantum field size for a superclass field.
   */
  int getSuperclassFieldPadding();
}
