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

package org.apache.hudi.hbase.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class UnsafeAvailChecker {

  private static final String CLASS_NAME = "sun.misc.Unsafe";
  private static final Logger LOG = LoggerFactory.getLogger(UnsafeAvailChecker.class);
  private static boolean avail = false;
  private static boolean unaligned = false;

  static {
    avail = AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
      @Override
      public Boolean run() {
        try {
          Class<?> clazz = Class.forName(CLASS_NAME);
          Field f = clazz.getDeclaredField("theUnsafe");
          f.setAccessible(true);
          Object theUnsafe = f.get(null);
          if (theUnsafe == null) {
            LOG.warn("Could not get static instance from sun.misc.Unsafe");
            return false;
          }
          // Check for availability of all methods used by UnsafeAccess
          Method m;
          try {
            m = clazz.getDeclaredMethod("arrayBaseOffset", Class.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing arrayBaseOffset(Class)");
              return false;
            }
            m = clazz.getDeclaredMethod("copyMemory", Object.class, long.class, Object.class,
                long.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing copyMemory(Object,long,Object,long,long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getByte", Object.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getByte(Object,long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getShort", long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getShort(long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getShort", Object.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getShort(Object,long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getInt", long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getInt(long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getInt", Object.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getInt(Object,long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getLong", long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getLong(long)");
              return false;
            }
            m = clazz.getDeclaredMethod("getLong", Object.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing getLong(Object,long)");
              return false;
            }
            m = clazz.getDeclaredMethod("putByte", long.class, byte.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putByte(long,byte)");
              return false;
            }
            m = clazz.getDeclaredMethod("putByte", Object.class, long.class, byte.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putByte(Object,long,byte)");
              return false;
            }
            m = clazz.getDeclaredMethod("putShort", long.class, short.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putShort(long,short)");
              return false;
            }
            m = clazz.getDeclaredMethod("putShort", Object.class, long.class, short.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putShort(Object,long,short)");
              return false;
            }
            m = clazz.getDeclaredMethod("putInt", long.class, int.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putInt(long,int)");
              return false;
            }
            m = clazz.getDeclaredMethod("putInt", Object.class, long.class, int.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putInt(Object,long,int)");
              return false;
            }
            m = clazz.getDeclaredMethod("putLong", long.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putLong(long,long)");
              return false;
            }
            m = clazz.getDeclaredMethod("putLong", Object.class, long.class, long.class);
            if (m == null) {
              LOG.warn("sun.misc.Unsafe is missing putLong(Object,long,long)");
              return false;
            }
            // theUnsafe is accessible and all methods are available
            return true;
          } catch (Throwable e) {
            LOG.warn("sun.misc.Unsafe is missing one or more required methods", e);
          }
        } catch (Throwable e) {
          LOG.warn("sun.misc.Unsafe is not available/accessible", e);
        }
        return false;
      }
    });
    // When Unsafe itself is not available/accessible consider unaligned as false.
    if (avail) {
      String arch = System.getProperty("os.arch");
      if ("ppc64".equals(arch) || "ppc64le".equals(arch) || "aarch64".equals(arch)) {
        // java.nio.Bits.unaligned() wrongly returns false on ppc (JDK-8165231),
        unaligned = true;
      } else {
        try {
          // Using java.nio.Bits#unaligned() to check for unaligned-access capability
          Class<?> clazz = Class.forName("java.nio.Bits");
          Method m = clazz.getDeclaredMethod("unaligned");
          m.setAccessible(true);
          unaligned = (Boolean) m.invoke(null);
        } catch (Exception e) {
          LOG.warn("java.nio.Bits#unaligned() check failed."
              + "Unsafe based read/write of primitive types won't be used", e);
        }
      }
    }
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and it is
   *         accessible.
   */
  public static boolean isAvailable() {
    return avail;
  }

  /**
   * @return true when running JVM is having sun's Unsafe package available in it and underlying
   *         system having unaligned-access capability.
   */
  public static boolean unaligned() {
    return unaligned;
  }

  private UnsafeAvailChecker() {
    // private constructor to avoid instantiation
  }
}
