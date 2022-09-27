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

package org.apache.hudi.common.util;

import org.openjdk.jol.info.GraphLayout;

/**
 * Contains utility methods for calculating the memory usage of objects. It only works on the HotSpot and OpenJ9 JVMs, and infers
 * the actual memory layout (32 bit vs. 64 bit word size, compressed object pointers vs. uncompressed) from the best
 * available indicators. It can reliably detect a 32 bit vs. 64 bit JVM. It can only make an educated guess at whether
 * compressed OOPs are used, though; specifically, it knows what the JVM's default choice of OOP compression would be
 * based on HotSpot version and maximum heap sizes, but if the choice is explicitly overridden with the
 * <tt>-XX:{+|-}UseCompressedOops</tt> command line switch, it can not detect this fact and will report incorrect sizes,
 * as it will presume the default JVM behavior.
 *
 * @author Attila Szegedi
 */
public class ObjectSizeCalculator {
  /**
   * Given an object, returns the total allocated size, in bytes, of the object and all other objects reachable from it.
   * Attempts to detect the current JVM memory layout, but may fail with {@link UnsupportedOperationException};
   *
   * @param obj the object; can be null. Passing in a {@link java.lang.Class} object doesn't do anything special, it
   *        measures the size of all objects reachable through it (which will include its class loader, and by
   *        extension, all other Class objects loaded by the same loader, and all the parent class loaders). It doesn't
   *        provide the size of the static fields in the JVM class that the Class object represents.
   * @return the total allocated size of the object and all other objects it retains.
   * @throws UnsupportedOperationException if the current vm memory layout cannot be detected.
   */
  public static long getObjectSize(Object obj) throws UnsupportedOperationException {
    // JDK versions 16 or later enforce strong encapsulation and block illegal reflective access.
    // In effect, we cannot calculate object size by deep reflection and invoking `setAccessible` on a field,
    // especially when the `isAccessible` is false. More details in JEP 403. While integrating Hudi with other
    // software packages that compile against JDK 16 or later (e.g. Trino), the IllegalAccessException will be thrown.
    // In that case, we use Java Object Layout (JOL) to estimate the object size.
    //
    // NOTE: We cannot get the object size base on the amount of byte serialized because there is no guarantee
    //       that the incoming object is serializable. We could have used Java's Instrumentation API, but it
    //       needs an instrumentation agent that can be hooked to the JVM. In lieu of that, we are using JOL.
    //       GraphLayout gives the deep size of an object, including the size of objects that are referenced from the given object.
    return obj == null ? 0 : GraphLayout.parseInstance(obj).totalSize();
  }
}
