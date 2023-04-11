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

package org.apache.hudi.console.base.util;

import java.lang.reflect.Array;
import java.util.Arrays;

public final class ObjectUtils {

  private ObjectUtils() {}

  private static final int INITIAL_HASH = 7;
  private static final int MULTIPLIER = 31;

  private static final String EMPTY_STRING = "";
  private static final String NULL_STRING = "null";
  private static final String ARRAY_START = "{";
  private static final String ARRAY_END = "}";
  private static final String EMPTY_ARRAY = ARRAY_START + ARRAY_END;
  private static final String ARRAY_ELEMENT_SEPARATOR = ", ";

  /**
   * Return whether the given throwable is a checked exception: that is, neither a RuntimeException
   * nor an Error.
   *
   * @param ex the throwable to check
   * @return whether the throwable is a checked exception
   * @see Exception
   * @see RuntimeException
   * @see Error
   */
  public static boolean isCheckedException(Throwable ex) {
    return !(ex instanceof RuntimeException || ex instanceof Error);
  }

  /**
   * Check whether the given exception is compatible with the exceptions declared in a throws
   * clause.
   *
   * @param ex the exception to checked
   * @param declaredExceptions the exceptions declared in the throws clause
   * @return whether the given exception is compatible
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static boolean isCompatibleWithThrowsClause(Throwable ex, Class[] declaredExceptions) {
    if (!isCheckedException(ex)) {
      return true;
    }
    if (declaredExceptions != null) {
      int i = 0;
      while (i < declaredExceptions.length) {
        if (declaredExceptions[i].isAssignableFrom(ex.getClass())) {
          return true;
        }
        i++;
      }
    }
    return false;
  }

  /**
   * Determine whether the given object is an array: either an Object array or a primitive array.
   *
   * @param obj the object to check
   */
  public static boolean isArray(Object obj) {
    return (obj != null && obj.getClass().isArray());
  }

  /**
   * Determine whether the given array is empty: i.e. <code>null</code> or of zero length.
   *
   * @param array the array to check
   */
  public static boolean isEmpty(Object[] array) {
    return (array == null || array.length == 0);
  }

  /**
   * Check whether the given array contains the given element.
   *
   * @param array the array to check (may be <code>null</code>, in which case the return value will
   *     always be <code>false</code>)
   * @param element the element to check for
   * @return whether the element has been found in the given array
   */
  public static boolean containsElement(Object[] array, Object element) {
    if (array == null) {
      return false;
    }
    for (Object arrayEle : array) {
      if (safeEquals(arrayEle, element)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check whether the given array of enum constants contains a constant with the given name,
   * ignoring case when determining a match.
   *
   * @param enumValues the enum values to check, typically the product of a call to MyEnum.values()
   * @param constant the constant name to find (must not be null or empty string)
   * @return whether the constant has been found in the given array
   */
  public static boolean containsConstant(Enum<?>[] enumValues, String constant) {
    return containsConstant(enumValues, constant, false);
  }

  /**
   * Check whether the given array of enum constants contains a constant with the given name.
   *
   * @param enumValues the enum values to check, typically the product of a call to MyEnum.values()
   * @param constant the constant name to find (must not be null or empty string)
   * @param caseSensitive whether case is significant in determining a match
   * @return whether the constant has been found in the given array
   */
  public static boolean containsConstant(
      Enum<?>[] enumValues, String constant, boolean caseSensitive) {
    for (Enum<?> candidate : enumValues) {
      if (caseSensitive
          ? candidate.toString().equals(constant)
          : candidate.toString().equalsIgnoreCase(constant)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Case insensitive alternative to {@link Enum#valueOf(Class, String)}.
   *
   * @param <E> the concrete Enum type
   * @param enumValues the array of all Enum constants in question, usually per Enum.values()
   * @param constant the constant to get the enum value of
   * @throws IllegalArgumentException if the given constant is not found in the given array of enum
   *     values. Use {@link #containsConstant(Enum[], String)} as a guard to avoid this exception.
   */
  public static <E extends Enum<?>> E caseInsensitiveValueOf(E[] enumValues, String constant) {
    for (E candidate : enumValues) {
      if (candidate.toString().equalsIgnoreCase(constant)) {
        return candidate;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "constant [%s] does not exist in enum type %s",
            constant, enumValues.getClass().getComponentType().getName()));
  }

  /**
   * Append the given object to the given array, returning a new array consisting of the input array
   * contents plus the given object.
   *
   * @param array the array to append to (can be <code>null</code>)
   * @param obj the object to append
   * @return the new array (of the same component type; never <code>null</code>)
   */
  public static <A, O extends A> A[] addObjectToArray(A[] array, O obj) {
    Class<?> compType = Object.class;
    if (array != null) {
      compType = array.getClass().getComponentType();
    } else if (obj != null) {
      compType = obj.getClass();
    }
    int newArrLength = (array != null ? array.length + 1 : 1);
    @SuppressWarnings("unchecked")
    A[] newArr = (A[]) Array.newInstance(compType, newArrLength);
    if (array != null) {
      System.arraycopy(array, 0, newArr, 0, array.length);
    }
    newArr[newArr.length - 1] = obj;
    return newArr;
  }

  /**
   * Convert the given array (which may be a primitive array) to an object array (if necessary of
   * primitive wrapper objects).
   *
   * <p>A <code>null</code> source value will be converted to an empty Object array.
   *
   * @param source the (potentially primitive) array
   * @return the corresponding object array (never <code>null</code>)
   * @throws IllegalArgumentException if the parameter is not an array
   */
  public static Object[] toObjectArray(Object source) {
    if (source instanceof Object[]) {
      return (Object[]) source;
    }
    if (source == null) {
      return new Object[0];
    }
    if (!source.getClass().isArray()) {
      throw new IllegalArgumentException("Source is not an array: " + source);
    }
    int length = Array.getLength(source);
    if (length == 0) {
      return new Object[0];
    }
    @SuppressWarnings("rawtypes")
    Class wrapperType = Array.get(source, 0).getClass();
    Object[] newArray = (Object[]) Array.newInstance(wrapperType, length);
    for (int i = 0; i < length; i++) {
      newArray[i] = Array.get(source, i);
    }
    return newArray;
  }

  // ---------------------------------------------------------------------
  // Convenience methods for content-based equality/hash-code handling
  // ---------------------------------------------------------------------

  /**
   * Determine if the given objects are equal, returning <code>true</code> if both are <code>null
   * </code> or <code>false</code> if only one is <code>null</code>.
   *
   * <p>Compares arrays with <code>Arrays.equals</code>, performing an equality check based on the
   * array elements rather than the array reference.
   *
   * @param o1 first Object to compare
   * @param o2 second Object to compare
   * @return whether the given objects are equal
   * @see Arrays#equals
   */
  public static boolean safeEquals(Object o1, Object o2) {
    if (o1 == null || o2 == null) {
      return false;
    }

    if (o1 == o2) {
      return true;
    }

    if (o1.equals(o2)) {
      return true;
    }
    if (o1.getClass().isArray() && o2.getClass().isArray()) {
      if (o1 instanceof Object[] && o2 instanceof Object[]) {
        return Arrays.equals((Object[]) o1, (Object[]) o2);
      }
      if (o1 instanceof boolean[] && o2 instanceof boolean[]) {
        return Arrays.equals((boolean[]) o1, (boolean[]) o2);
      }
      if (o1 instanceof byte[] && o2 instanceof byte[]) {
        return Arrays.equals((byte[]) o1, (byte[]) o2);
      }
      if (o1 instanceof char[] && o2 instanceof char[]) {
        return Arrays.equals((char[]) o1, (char[]) o2);
      }
      if (o1 instanceof double[] && o2 instanceof double[]) {
        return Arrays.equals((double[]) o1, (double[]) o2);
      }
      if (o1 instanceof float[] && o2 instanceof float[]) {
        return Arrays.equals((float[]) o1, (float[]) o2);
      }
      if (o1 instanceof int[] && o2 instanceof int[]) {
        return Arrays.equals((int[]) o1, (int[]) o2);
      }
      if (o1 instanceof long[] && o2 instanceof long[]) {
        return Arrays.equals((long[]) o1, (long[]) o2);
      }
      if (o1 instanceof short[] && o2 instanceof short[]) {
        return Arrays.equals((short[]) o1, (short[]) o2);
      }
    }
    return false;
  }

  public static boolean safeTrimEquals(Object o1, Object o2) {
    boolean equals = safeEquals(o1, o2);
    if (!equals) {
      if (o1 != null && o2 != null) {
        if (o1 instanceof String && o2 instanceof String) {
          return o1.toString().trim().equals(o2.toString().trim());
        }
      }
    }
    return equals;
  }

  /**
   * Return as hash code for the given object; typically the value of <code>
   * {@link Object#hashCode()}</code>. If the object is an array, this method will delegate to any
   * of the <code>safeHashCode</code> methods for arrays in this class. If the object is <code>null
   * </code>, this method returns 0.
   *
   * @see #safeHashCode(Object[])
   * @see #safeHashCode(boolean[])
   * @see #safeHashCode(byte[])
   * @see #safeHashCode(char[])
   * @see #safeHashCode(double[])
   * @see #safeHashCode(float[])
   * @see #safeHashCode(int[])
   * @see #safeHashCode(long[])
   * @see #safeHashCode(short[])
   */
  public static int safeHashCode(Object obj) {
    if (obj == null) {
      return 0;
    }
    if (obj.getClass().isArray()) {
      if (obj instanceof Object[]) {
        return safeHashCode((Object[]) obj);
      }
      if (obj instanceof boolean[]) {
        return safeHashCode((boolean[]) obj);
      }
      if (obj instanceof byte[]) {
        return safeHashCode((byte[]) obj);
      }
      if (obj instanceof char[]) {
        return safeHashCode((char[]) obj);
      }
      if (obj instanceof double[]) {
        return safeHashCode((double[]) obj);
      }
      if (obj instanceof float[]) {
        return safeHashCode((float[]) obj);
      }
      if (obj instanceof int[]) {
        return safeHashCode((int[]) obj);
      }
      if (obj instanceof long[]) {
        return safeHashCode((long[]) obj);
      }
      if (obj instanceof short[]) {
        return safeHashCode((short[]) obj);
      }
    }
    return obj.hashCode();
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(Object[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (Object anArray : array) {
      hash = MULTIPLIER * hash + safeHashCode(anArray);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(boolean[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (boolean anArray : array) {
      hash = MULTIPLIER * hash + hashCode(anArray);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(byte[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (byte anArray : array) {
      hash = MULTIPLIER * hash + anArray;
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(char[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (char anArray : array) {
      hash = MULTIPLIER * hash + anArray;
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(double[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (double anArray : array) {
      hash = MULTIPLIER * hash + hashCode(anArray);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(float[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (float anArray : array) {
      hash = MULTIPLIER * hash + hashCode(anArray);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(int[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (int anArray : array) {
      hash = MULTIPLIER * hash + anArray;
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(long[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (long anArray : array) {
      hash = MULTIPLIER * hash + hashCode(anArray);
    }
    return hash;
  }

  /**
   * Return a hash code based on the contents of the specified array. If <code>array</code> is
   * <code>null</code>, this method returns 0.
   */
  public static int safeHashCode(short[] array) {
    if (array == null) {
      return 0;
    }
    int hash = INITIAL_HASH;
    for (short anArray : array) {
      hash = MULTIPLIER * hash + anArray;
    }
    return hash;
  }

  /**
   * Return the same value as <code>{@link Boolean#hashCode()}</code>.
   *
   * @see Boolean#hashCode()
   */
  public static int hashCode(boolean bool) {
    return bool ? 1231 : 1237;
  }

  /**
   * Return the same value as <code>{@link Double#hashCode()}</code>.
   *
   * @see Double#hashCode()
   */
  public static int hashCode(double dbl) {
    long bits = Double.doubleToLongBits(dbl);
    return hashCode(bits);
  }

  /**
   * Return the same value as <code>{@link Float#hashCode()}</code>.
   *
   * @see Float#hashCode()
   */
  public static int hashCode(float flt) {
    return Float.floatToIntBits(flt);
  }

  /**
   * Return the same value as <code>{@link Long#hashCode()}</code>.
   *
   * @see Long#hashCode()
   */
  public static int hashCode(long lng) {
    return (int) (lng ^ (lng >>> 32));
  }

  // ---------------------------------------------------------------------
  // Convenience methods for toString output
  // ---------------------------------------------------------------------

  /**
   * Return a String representation of an object's overall identity.
   *
   * @param obj the object (may be <code>null</code>)
   * @return the object's identity as String representation, or an empty String if the object was
   *     <code>null</code>
   */
  public static String identityToString(Object obj) {
    if (obj == null) {
      return EMPTY_STRING;
    }
    return obj.getClass().getName() + "@" + getIdentityHexString(obj);
  }

  /**
   * Return a hex String form of an object's identity hash code.
   *
   * @param obj the object
   * @return the object's identity code in hex notation
   */
  public static String getIdentityHexString(Object obj) {
    return Integer.toHexString(System.identityHashCode(obj));
  }

  /**
   * Return a content-based String representation if <code>obj</code> is not <code>null</code>;
   * otherwise returns an empty String.
   *
   * <p>Differs from {@link #safeToString(Object)} in that it returns an empty String rather than
   * "null" for a <code>null</code> value.
   *
   * @param obj the object to build a display String for
   * @return a display String representation of <code>obj</code>
   * @see #safeToString(Object)
   */
  public static String getDisplayString(Object obj) {
    if (obj == null) {
      return EMPTY_STRING;
    }
    return safeToString(obj);
  }

  /**
   * Determine the class name for the given object.
   *
   * <p>Returns <code>"null"</code> if <code>obj</code> is <code>null</code>.
   *
   * @param obj the object to introspect (may be <code>null</code>)
   * @return the corresponding class name
   */
  public static String safeClassName(Object obj) {
    return (obj != null ? obj.getClass().getName() : NULL_STRING);
  }

  /**
   * Return a String representation of the specified Object.
   *
   * <p>Builds a String representation of the contents in case of an array. Returns <code>"null"
   * </code> if <code>obj</code> is <code>null</code>.
   *
   * @param obj the object to build a String representation for
   * @return a String representation of <code>obj</code>
   */
  public static String safeToString(Object obj) {
    if (obj == null) {
      return NULL_STRING;
    }
    if (obj instanceof String) {
      return (String) obj;
    }
    if (obj instanceof Object[]) {
      return safeToString((Object[]) obj);
    }
    if (obj instanceof boolean[]) {
      return safeToString((boolean[]) obj);
    }
    if (obj instanceof byte[]) {
      return safeToString((byte[]) obj);
    }
    if (obj instanceof char[]) {
      return safeToString((char[]) obj);
    }
    if (obj instanceof double[]) {
      return safeToString((double[]) obj);
    }
    if (obj instanceof float[]) {
      return safeToString((float[]) obj);
    }
    if (obj instanceof int[]) {
      return safeToString((int[]) obj);
    }
    if (obj instanceof long[]) {
      return safeToString((long[]) obj);
    }
    if (obj instanceof short[]) {
      return safeToString((short[]) obj);
    }
    String str = obj.toString();
    return (str != null ? str : EMPTY_STRING);
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(Object[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }
      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(boolean[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }

      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(byte[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }
      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(char[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }
      sb.append("'").append(array[i]).append("'");
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(double[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }

      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(float[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }

      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(int[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }
      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(long[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }
      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }

  /**
   * Return a String representation of the contents of the specified array.
   *
   * <p>The String representation consists of a list of the array's elements, enclosed in curly
   * braces (<code>"{}"</code>). Adjacent elements are separated by the characters <code>", "</code>
   * (a comma followed by a space). Returns <code>"null"</code> if <code>array</code> is <code>null
   * </code>.
   *
   * @param array the array to build a String representation for
   * @return a String representation of <code>array</code>
   */
  public static String safeToString(short[] array) {
    if (array == null) {
      return NULL_STRING;
    }
    int length = array.length;
    if (length == 0) {
      return EMPTY_ARRAY;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < length; i++) {
      if (i == 0) {
        sb.append(ARRAY_START);
      } else {
        sb.append(ARRAY_ELEMENT_SEPARATOR);
      }
      sb.append(array[i]);
    }
    sb.append(ARRAY_END);
    return sb.toString();
  }
}
