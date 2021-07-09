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

package org.apache.hudi.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import sun.misc.Unsafe;

public class HiveHasher {

  @Override
  public String toString() {
    return HiveHasher.class.getSimpleName();
  }

  public static int hashInt(int input) {
    return input;
  }

  public static int hashLong(long input) {
    return (int) ((input >>> 32) ^ input);
  }

  public static int hashUnsafeBytes(Object base, long offset, int lengthInBytes) {
    assert (lengthInBytes >= 0) : "lengthInBytes cannot be negative";
    int result = 0;
    for (int i = 0; i < lengthInBytes; i++) {
      result = (result * 31) + (int) Platform.getByte(base, offset + i);
    }
    return result;
  }

  static class Platform {

    private static final Unsafe UNSAFE;

    public static final int BOOLEAN_ARRAY_OFFSET;

    public static final int BYTE_ARRAY_OFFSET;

    public static final int SHORT_ARRAY_OFFSET;

    public static final int INT_ARRAY_OFFSET;

    public static final int LONG_ARRAY_OFFSET;

    public static final int FLOAT_ARRAY_OFFSET;

    public static final int DOUBLE_ARRAY_OFFSET;

    private static final boolean UNALIGNED;

    // Access fields and constructors once and store them, for performance:

    private static final Constructor<?> DBB_CONSTRUCTOR;
    private static final Field DBB_CLEANER_FIELD;

    static {
      try {
        Class<?> cls = Class.forName("java.nio.DirectByteBuffer");
        Constructor<?> constructor = cls.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
        constructor.setAccessible(true);
        Field cleanerField = cls.getDeclaredField("cleaner");
        cleanerField.setAccessible(true);
        DBB_CONSTRUCTOR = constructor;
        DBB_CLEANER_FIELD = cleanerField;
      } catch (ClassNotFoundException | NoSuchMethodException | NoSuchFieldException e) {
        throw new IllegalStateException(e);
      }
    }

    // Split java.version on non-digit chars:
    private static final int MAJOR_VERSION =
        Integer.parseInt(System.getProperty("java.version").split("\\D+")[0]);

    private static final Method CLEANER_CREATE_METHOD;

    static {
      // The implementation of Cleaner changed from JDK 8 to 9
      String cleanerClassName;
      if (MAJOR_VERSION < 9) {
        cleanerClassName = "sun.misc.Cleaner";
      } else {
        cleanerClassName = "jdk.internal.ref.Cleaner";
      }
      try {
        Class<?> cleanerClass = Class.forName(cleanerClassName);
        Method createMethod = cleanerClass.getMethod("create", Object.class, Runnable.class);
        // Accessing jdk.internal.ref.Cleaner should actually fail by default in JDK 9+,
        // unfortunately, unless the user has allowed access with something like
        // --add-opens java.base/java.lang=ALL-UNNAMED  If not, we can't really use the Cleaner
        // hack below. It doesn't break, just means the user might run into the default JVM limit
        // on off-heap memory and increase it or set the flag above. This tests whether it's
        // available:
        try {
          createMethod.invoke(null, null, null);
        } catch (IllegalAccessException e) {
          // Don't throw an exception, but can't log here?
          createMethod = null;
        } catch (InvocationTargetException ite) {
          // shouldn't happen; report it
          throw new IllegalStateException(ite);
        }
        CLEANER_CREATE_METHOD = createMethod;
      } catch (ClassNotFoundException | NoSuchMethodException e) {
        throw new IllegalStateException(e);
      }

    }

    public static byte getByte(Object object, long offset) {
      return UNSAFE.getByte(object, offset);
    }

    static {
      sun.misc.Unsafe unsafe;
      try {
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        unsafe = (sun.misc.Unsafe) unsafeField.get(null);
      } catch (Throwable cause) {
        unsafe = null;
      }
      UNSAFE = unsafe;

      if (UNSAFE != null) {
        BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
        BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
        INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
        LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
        FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
        DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
      } else {
        BOOLEAN_ARRAY_OFFSET = 0;
        BYTE_ARRAY_OFFSET = 0;
        SHORT_ARRAY_OFFSET = 0;
        INT_ARRAY_OFFSET = 0;
        LONG_ARRAY_OFFSET = 0;
        FLOAT_ARRAY_OFFSET = 0;
        DOUBLE_ARRAY_OFFSET = 0;
      }
    }

    // This requires `majorVersion` and `_UNSAFE`.
    static {
      boolean unaligned;
      String arch = System.getProperty("os.arch", "");
      if (arch.equals("ppc64le") || arch.equals("ppc64") || arch.equals("s390x")) {
        // Since java.nio.Bits.unaligned() doesn't return true on ppc (See JDK-8165231), but
        // ppc64 and ppc64le support it
        unaligned = true;
      } else {
        try {
          Class<?> bitsClass =
              Class.forName("java.nio.Bits", false, ClassLoader.getSystemClassLoader());
          if (UNSAFE != null && MAJOR_VERSION >= 9) {
            // Java 9/10 and 11/12 have different field names.
            Field unalignedField =
                bitsClass.getDeclaredField(MAJOR_VERSION >= 11 ? "UNALIGNED" : "unaligned");
            unaligned = UNSAFE.getBoolean(
                UNSAFE.staticFieldBase(unalignedField), UNSAFE.staticFieldOffset(unalignedField));
          } else {
            Method unalignedMethod = bitsClass.getDeclaredMethod("unaligned");
            unalignedMethod.setAccessible(true);
            unaligned = Boolean.TRUE.equals(unalignedMethod.invoke(null));
          }
        } catch (Throwable t) {
          // We at least know x86 and x64 support unaligned access.
          //noinspection DynamicRegexReplaceableByCompiledPattern
          unaligned = arch.matches("^(i[3-6]86|x86(_64)?|x64|amd64|aarch64)$");
        }
      }
      UNALIGNED = unaligned;
    }
  }
}
