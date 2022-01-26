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
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for determining the "size" of a class, an attempt to calculate the
 * actual bytes that an object of this class will occupy in memory
 *
 * The core of this class is taken from the Derby project
 */
@InterfaceAudience.Private
public class ClassSize {
  private static final Logger LOG = LoggerFactory.getLogger(ClassSize.class);

  /** Array overhead */
  public static final int ARRAY;

  /** Overhead for ArrayList(0) */
  public static final int ARRAYLIST;

  /** Overhead for LinkedList(0) */
  public static final int LINKEDLIST;

  /** Overhead for a single entry in LinkedList */
  public static final int LINKEDLIST_ENTRY;

  /** Overhead for ByteBuffer */
  public static final int BYTE_BUFFER;

  /** Overhead for an Integer */
  public static final int INTEGER;

  /** Overhead for entry in map */
  public static final int MAP_ENTRY;

  /** Object overhead is minimum 2 * reference size (8 bytes on 64-bit) */
  public static final int OBJECT;

  /** Reference size is 8 bytes on 64-bit, 4 bytes on 32-bit */
  public static final int REFERENCE;

  /** String overhead */
  public static final int STRING;

  /** Overhead for TreeMap */
  public static final int TREEMAP;

  /** Overhead for ConcurrentHashMap */
  public static final int CONCURRENT_HASHMAP;

  /** Overhead for ConcurrentHashMap.Entry */
  public static final int CONCURRENT_HASHMAP_ENTRY;

  /** Overhead for ConcurrentHashMap.Segment */
  public static final int CONCURRENT_HASHMAP_SEGMENT;

  /** Overhead for ConcurrentSkipListMap */
  public static final int CONCURRENT_SKIPLISTMAP;

  /** Overhead for ConcurrentSkipListMap Entry */
  public static final int CONCURRENT_SKIPLISTMAP_ENTRY;

  /** Overhead for CellFlatMap */
  public static final int CELL_FLAT_MAP;

  /** Overhead for CellChunkMap */
  public static final int CELL_CHUNK_MAP;

  /** Overhead for Cell Chunk Map Entry */
  public static final int CELL_CHUNK_MAP_ENTRY;

  /** Overhead for CellArrayMap */
  public static final int CELL_ARRAY_MAP;

  /** Overhead for Cell Array Entry */
  public static final int CELL_ARRAY_MAP_ENTRY;

  /** Overhead for ReentrantReadWriteLock */
  public static final int REENTRANT_LOCK;

  /** Overhead for AtomicLong */
  public static final int ATOMIC_LONG;

  /** Overhead for AtomicInteger */
  public static final int ATOMIC_INTEGER;

  /** Overhead for AtomicBoolean */
  public static final int ATOMIC_BOOLEAN;

  /** Overhead for AtomicReference */
  public static final int ATOMIC_REFERENCE;

  /** Overhead for CopyOnWriteArraySet */
  public static final int COPYONWRITE_ARRAYSET;

  /** Overhead for CopyOnWriteArrayList */
  public static final int COPYONWRITE_ARRAYLIST;

  /** Overhead for timerange */
  public static final int TIMERANGE;

  /** Overhead for SyncTimeRangeTracker */
  public static final int SYNC_TIMERANGE_TRACKER;

  /** Overhead for NonSyncTimeRangeTracker */
  public static final int NON_SYNC_TIMERANGE_TRACKER;

  /** Overhead for CellSkipListSet */
  public static final int CELL_SET;

  public static final int STORE_SERVICES;

  /**
   * MemoryLayout abstracts details about the JVM object layout. Default implementation is used in
   * case Unsafe is not available.
   */
  private static class MemoryLayout {
    int headerSize() {
      return 2 * oopSize();
    }

    int arrayHeaderSize() {
      return (int) align(3 * oopSize());
    }

    /**
     * Return the size of an "ordinary object pointer". Either 4 or 8, depending on 32/64 bit,
     * and CompressedOops
     */
    int oopSize() {
      return is32BitJVM() ? 4 : 8;
    }

    /**
     * Aligns a number to 8.
     * @param num number to align to 8
     * @return smallest number &gt;= input that is a multiple of 8
     */
    public long align(long num) {
      //The 7 comes from that the alignSize is 8 which is the number of bytes
      //stored and sent together
      return  ((num + 7) >> 3) << 3;
    }

    long sizeOfByteArray(int len) {
      return align(ARRAY + len);
    }
  }

  /**
   * UnsafeLayout uses Unsafe to guesstimate the object-layout related parameters like object header
   * sizes and oop sizes
   * See HBASE-15950.
   */
  private static class UnsafeLayout extends MemoryLayout {
    @SuppressWarnings("unused")
    private static final class HeaderSize {
      private byte a;
    }

    public UnsafeLayout() {
    }

    @Override
    int headerSize() {
      try {
        return (int) UnsafeAccess.theUnsafe.objectFieldOffset(
            HeaderSize.class.getDeclaredField("a"));
      } catch (NoSuchFieldException | SecurityException e) {
        LOG.error(e.toString(), e);
      }
      return super.headerSize();
    }

    @Override
    int arrayHeaderSize() {
      return UnsafeAccess.theUnsafe.arrayBaseOffset(byte[].class);
    }

    @Override
    @SuppressWarnings("static-access")
    int oopSize() {
      // Unsafe.addressSize() returns 8, even with CompressedOops. This is how many bytes each
      // element is allocated in an Object[].
      return UnsafeAccess.theUnsafe.ARRAY_OBJECT_INDEX_SCALE;
    }

    @Override
    @SuppressWarnings("static-access")
    long sizeOfByteArray(int len) {
      return align(ARRAY + len * UnsafeAccess.theUnsafe.ARRAY_BYTE_INDEX_SCALE);
    }
  }

  private static MemoryLayout getMemoryLayout() {
    // Have a safeguard in case Unsafe estimate is wrong. This is static context, there is
    // no configuration, so we look at System property.
    String enabled = System.getProperty("hbase.memorylayout.use.unsafe");
    if (UnsafeAvailChecker.isAvailable() && (enabled == null || Boolean.parseBoolean(enabled))) {
      LOG.debug("Using Unsafe to estimate memory layout");
      return new UnsafeLayout();
    }
    LOG.debug("Not using Unsafe to estimate memory layout");
    return new MemoryLayout();
  }

  private static final MemoryLayout memoryLayout = getMemoryLayout();
  private static final boolean USE_UNSAFE_LAYOUT = (memoryLayout instanceof UnsafeLayout);

  public static boolean useUnsafeLayout() {
    return USE_UNSAFE_LAYOUT;
  }

  /**
   * Method for reading the arc settings and setting overheads according
   * to 32-bit or 64-bit architecture.
   */
  static {
    REFERENCE = memoryLayout.oopSize();

    OBJECT = memoryLayout.headerSize();

    ARRAY = memoryLayout.arrayHeaderSize();

    ARRAYLIST = align(OBJECT + REFERENCE + (2 * Bytes.SIZEOF_INT)) + align(ARRAY);

    LINKEDLIST = align(OBJECT + (2 * Bytes.SIZEOF_INT) + (2 * REFERENCE));

    LINKEDLIST_ENTRY = align(OBJECT + (2 * REFERENCE));

    //noinspection PointlessArithmeticExpression
    BYTE_BUFFER = JVM.getJVMSpecVersion() < 17 ?
        align(OBJECT + REFERENCE +
            (5 * Bytes.SIZEOF_INT) +
            (3 * Bytes.SIZEOF_BOOLEAN) + Bytes.SIZEOF_LONG) + align(ARRAY) :
        align(OBJECT + 2 * REFERENCE +
            (5 * Bytes.SIZEOF_INT) +
            (3 * Bytes.SIZEOF_BOOLEAN) + Bytes.SIZEOF_LONG) + align(ARRAY);

    INTEGER = align(OBJECT + Bytes.SIZEOF_INT);

    MAP_ENTRY = align(OBJECT + 5 * REFERENCE + Bytes.SIZEOF_BOOLEAN);

    TREEMAP = align(OBJECT + (2 * Bytes.SIZEOF_INT) + 7 * REFERENCE);

    // STRING is different size in jdk6 and jdk7. Just use what we estimate as size rather than
    // have a conditional on whether jdk7.
    STRING = (int) estimateBase(String.class, false);

    // CONCURRENT_HASHMAP is different size in jdk6 and jdk7; it looks like its different between
    // 23.6-b03 and 23.0-b21. Just use what we estimate as size rather than have a conditional on
    // whether jdk7.
    CONCURRENT_HASHMAP = (int) estimateBase(ConcurrentHashMap.class, false);

    CONCURRENT_HASHMAP_ENTRY = align(REFERENCE + OBJECT + (3 * REFERENCE) +
        (2 * Bytes.SIZEOF_INT));

    CONCURRENT_HASHMAP_SEGMENT = align(REFERENCE + OBJECT +
        (3 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_FLOAT + ARRAY);

    // The size changes from jdk7 to jdk8, estimate the size rather than use a conditional
    CONCURRENT_SKIPLISTMAP = (int) estimateBase(ConcurrentSkipListMap.class, false);

    // CellFlatMap object contains two integers, one boolean and one reference to object, so
    // 2*INT + BOOLEAN + REFERENCE
    CELL_FLAT_MAP = OBJECT + 2*Bytes.SIZEOF_INT + Bytes.SIZEOF_BOOLEAN + REFERENCE;

    // CELL_ARRAY_MAP is the size of an instance of CellArrayMap class, which extends
    // CellFlatMap class. CellArrayMap object containing a ref to an Array of Cells
    CELL_ARRAY_MAP = align(CELL_FLAT_MAP + REFERENCE + ARRAY);

    // CELL_CHUNK_MAP is the size of an instance of CellChunkMap class, which extends
    // CellFlatMap class. CellChunkMap object containing a ref to an Array of Chunks
    CELL_CHUNK_MAP = align(CELL_FLAT_MAP + REFERENCE + ARRAY);

    CONCURRENT_SKIPLISTMAP_ENTRY = align(
        align(OBJECT + (3 * REFERENCE)) + /* one node per entry */
            align((OBJECT + (3 * REFERENCE))/2)); /* one index per two entries */

    // REFERENCE in the CellArrayMap all the rest is counted in KeyValue.heapSize()
    CELL_ARRAY_MAP_ENTRY = align(REFERENCE);

    // The Cell Representation in the CellChunkMap, the Cell object size shouldn't be counted
    // in KeyValue.heapSize()
    // each cell-representation requires three integers for chunkID (reference to the ByteBuffer),
    // offset and length, and one long for seqID
    CELL_CHUNK_MAP_ENTRY = 3*Bytes.SIZEOF_INT + Bytes.SIZEOF_LONG;

    REENTRANT_LOCK = align(OBJECT + (3 * REFERENCE));

    ATOMIC_LONG = align(OBJECT + Bytes.SIZEOF_LONG);

    ATOMIC_INTEGER = align(OBJECT + Bytes.SIZEOF_INT);

    ATOMIC_BOOLEAN = align(OBJECT + Bytes.SIZEOF_BOOLEAN);

    ATOMIC_REFERENCE = align(OBJECT + REFERENCE);

    COPYONWRITE_ARRAYSET = align(OBJECT + REFERENCE);

    COPYONWRITE_ARRAYLIST = align(OBJECT + (2 * REFERENCE) + ARRAY);

    TIMERANGE = align(ClassSize.OBJECT + Bytes.SIZEOF_LONG * 2 + Bytes.SIZEOF_BOOLEAN);

    SYNC_TIMERANGE_TRACKER = align(ClassSize.OBJECT + 2 * REFERENCE);

    NON_SYNC_TIMERANGE_TRACKER = align(ClassSize.OBJECT + 2 * Bytes.SIZEOF_LONG);

    CELL_SET = align(OBJECT + REFERENCE + Bytes.SIZEOF_INT);

    STORE_SERVICES = align(OBJECT + REFERENCE + ATOMIC_LONG);
  }

  /**
   * The estimate of the size of a class instance depends on whether the JVM
   * uses 32 or 64 bit addresses, that is it depends on the size of an object
   * reference. It is a linear function of the size of a reference, e.g.
   * 24 + 5*r where r is the size of a reference (usually 4 or 8 bytes).
   *
   * This method returns the coefficients of the linear function, e.g. {24, 5}
   * in the above example.
   *
   * @param cl A class whose instance size is to be estimated
   * @param debug debug flag
   * @return an array of 3 integers. The first integer is the size of the
   * primitives, the second the number of arrays and the third the number of
   * references.
   */
  @SuppressWarnings("unchecked")
  private static int [] getSizeCoefficients(Class cl, boolean debug) {
    int primitives = 0;
    int arrays = 0;
    int references = 0;
    int index = 0;

    for ( ; null != cl; cl = cl.getSuperclass()) {
      Field[] field = cl.getDeclaredFields();
      if (null != field) {
        for (Field aField : field) {
          if (Modifier.isStatic(aField.getModifiers())) continue;
          Class fieldClass = aField.getType();
          if (fieldClass.isArray()) {
            arrays++;
            references++;
          } else if (!fieldClass.isPrimitive()) {
            references++;
          } else {// Is simple primitive
            String name = fieldClass.getName();

            if (name.equals("int") || name.equals("I"))
              primitives += Bytes.SIZEOF_INT;
            else if (name.equals("long") || name.equals("J"))
              primitives += Bytes.SIZEOF_LONG;
            else if (name.equals("boolean") || name.equals("Z"))
              primitives += Bytes.SIZEOF_BOOLEAN;
            else if (name.equals("short") || name.equals("S"))
              primitives += Bytes.SIZEOF_SHORT;
            else if (name.equals("byte") || name.equals("B"))
              primitives += Bytes.SIZEOF_BYTE;
            else if (name.equals("char") || name.equals("C"))
              primitives += Bytes.SIZEOF_CHAR;
            else if (name.equals("float") || name.equals("F"))
              primitives += Bytes.SIZEOF_FLOAT;
            else if (name.equals("double") || name.equals("D"))
              primitives += Bytes.SIZEOF_DOUBLE;
          }
          if (debug) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("" + index + " " + aField.getName() + " " + aField.getType());
            }
          }
          index++;
        }
      }
    }
    return new int [] {primitives, arrays, references};
  }

  /**
   * Estimate the static space taken up by a class instance given the
   * coefficients returned by getSizeCoefficients.
   *
   * @param coeff the coefficients
   *
   * @param debug debug flag
   * @return the size estimate, in bytes
   */
  private static long estimateBaseFromCoefficients(int [] coeff, boolean debug) {
    long prealign_size = OBJECT + coeff[0] + coeff[2] * REFERENCE;

    // Round up to a multiple of 8
    long size = align(prealign_size) + align(coeff[1] * ARRAY);
    if (debug) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Primitives=" + coeff[0] + ", arrays=" + coeff[1] +
            ", references=" + coeff[2] + ", refSize " + REFERENCE +
            ", size=" + size + ", prealign_size=" + prealign_size);
      }
    }
    return size;
  }

  /**
   * Estimate the static space taken up by the fields of a class. This includes
   * the space taken up by by references (the pointer) but not by the referenced
   * object. So the estimated size of an array field does not depend on the size
   * of the array. Similarly the size of an object (reference) field does not
   * depend on the object.
   *
   * @param cl class
   * @param debug debug flag
   * @return the size estimate in bytes.
   */
  @SuppressWarnings("unchecked")
  public static long estimateBase(Class cl, boolean debug) {
    return estimateBaseFromCoefficients( getSizeCoefficients(cl, debug), debug);
  }

  /**
   * Aligns a number to 8.
   * @param num number to align to 8
   * @return smallest number &gt;= input that is a multiple of 8
   */
  public static int align(int num) {
    return (int)(align((long)num));
  }

  /**
   * Aligns a number to 8.
   * @param num number to align to 8
   * @return smallest number &gt;= input that is a multiple of 8
   */
  public static long align(long num) {
    return memoryLayout.align(num);
  }

  /**
   * Determines if we are running in a 32-bit JVM. Some unit tests need to
   * know this too.
   */
  public static boolean is32BitJVM() {
    final String model = System.getProperty("sun.arch.data.model");
    return model != null && model.equals("32");
  }

  /**
   * Calculate the memory consumption (in byte) of a byte array,
   * including the array header and the whole backing byte array.
   *
   * If the whole byte array is occupied (not shared with other objects), please use this function.
   * If not, please use {@link #sizeOfByteArray(int)} instead.
   *
   * @param b the byte array
   * @return the memory consumption (in byte) of the whole byte array
   */
  public static long sizeOf(byte[] b) {
    return memoryLayout.sizeOfByteArray(b.length);
  }

  /**
   * Calculate the memory consumption (in byte) of a part of a byte array,
   * including the array header and the part of the backing byte array.
   *
   * This function is used when the byte array backs multiple objects.
   * For example, in {@link org.apache.hudi.hbase.KeyValue},
   * multiple KeyValue objects share a same backing byte array ({@link org.apache.hudi.hbase.KeyValue#bytes}).
   * Also see {@link org.apache.hudi.hbase.KeyValue#heapSize()}.
   *
   * @param len the length (in byte) used partially in the backing byte array
   * @return the memory consumption (in byte) of the part of the byte array
   */
  public static long sizeOfByteArray(int len) {
    return memoryLayout.sizeOfByteArray(len);
  }

}
