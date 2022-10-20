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

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBinaryUtil {

  @Test
  public void testIntConvert() {
    // test Int
    int[] testInt = new int[] {-1, 1, -2, 10000, -100000, 2, Integer.MAX_VALUE, Integer.MIN_VALUE};
    List<OrginValueWrapper<Integer>> valueWrappers = new ArrayList<>();
    List<ConvertResultWrapper<Integer>> convertResultWrappers = new ArrayList<>();
    for (int i = 0; i < testInt.length; i++) {
      valueWrappers.add(new OrginValueWrapper<>(i, testInt[i]));
      convertResultWrappers.add(new ConvertResultWrapper<>(i, BinaryUtil.intTo8Byte(testInt[i])));
    }

    Collections.sort(valueWrappers, ((o1, o2) -> o1.originValue.compareTo(o2.originValue)));

    Collections.sort(convertResultWrappers, ((o1, o2) -> BinaryUtil.compareTo(o1.result, 0, o1.result.length, o2.result, 0, o2.result.length)));

    for (int i = 0; i < testInt.length; i++) {
      assertEquals(valueWrappers.get(i).index, convertResultWrappers.get(i).index);
    }
  }

  @Test
  public void testLongConvert() {
    // test Long
    long[] testLong = new long[] {-1L, 1L, -2L, 10000L, -100000L, 2L, Long.MAX_VALUE, Long.MIN_VALUE};
    List<OrginValueWrapper<Long>> valueWrappers = new ArrayList<>();
    List<ConvertResultWrapper<Long>> convertResultWrappers = new ArrayList<>();
    for (int i = 0; i < testLong.length; i++) {
      valueWrappers.add(new OrginValueWrapper<>((long)i, testLong[i]));
      convertResultWrappers.add(new ConvertResultWrapper<>((long)i, BinaryUtil.longTo8Byte(testLong[i])));
    }

    Collections.sort(valueWrappers, ((o1, o2) -> o1.originValue.compareTo(o2.originValue)));

    Collections.sort(convertResultWrappers, ((o1, o2) -> BinaryUtil.compareTo(o1.result, 0, o1.result.length, o2.result, 0, o2.result.length)));

    for (int i = 0; i < testLong.length; i++) {
      assertEquals(valueWrappers.get(i).index, convertResultWrappers.get(i).index);
    }
  }

  @Test
  public void testDoubleConvert() {
    // test Long
    double[] testDouble = new double[] {-1.00d, 1.05d, -2.3d, 10000.002d, -100000.7d, 2.9d, Double.MAX_VALUE};
    List<OrginValueWrapper<Double>> valueWrappers = new ArrayList<>();
    List<ConvertResultWrapper<Double>> convertResultWrappers = new ArrayList<>();
    for (int i = 0; i < testDouble.length; i++) {
      valueWrappers.add(new OrginValueWrapper<>((Double)(i * 1.0), testDouble[i]));
      convertResultWrappers.add(new ConvertResultWrapper<>((Double)(i * 1.0), BinaryUtil.doubleTo8Byte(testDouble[i])));
    }

    Collections.sort(valueWrappers, ((o1, o2) -> o1.originValue.compareTo(o2.originValue)));

    Collections.sort(convertResultWrappers, ((o1, o2) -> BinaryUtil.compareTo(o1.result, 0, o1.result.length, o2.result, 0, o2.result.length)));

    for (int i = 0; i < testDouble.length; i++) {
      assertEquals(valueWrappers.get(i).index, convertResultWrappers.get(i).index);
    }
  }

  @Test
  public void testFloatConvert() {
    // test Long
    float[] testDouble = new float[] {-1.00f, 1.05f, -2.3f, 10000.002f, -100000.7f, 2.9f, Float.MAX_VALUE, Float.MIN_VALUE};
    List<OrginValueWrapper<Float>> valueWrappers = new ArrayList<>();
    List<ConvertResultWrapper<Float>> convertResultWrappers = new ArrayList<>();
    for (int i = 0; i < testDouble.length; i++) {
      valueWrappers.add(new OrginValueWrapper<>((float)(i * 1.0), testDouble[i]));
      convertResultWrappers.add(new ConvertResultWrapper<>((float)(i * 1.0), BinaryUtil.doubleTo8Byte((double) testDouble[i])));
    }

    Collections.sort(valueWrappers, ((o1, o2) -> o1.originValue.compareTo(o2.originValue)));

    Collections.sort(convertResultWrappers, ((o1, o2) -> BinaryUtil.compareTo(o1.result, 0, o1.result.length, o2.result, 0, o2.result.length)));

    for (int i = 0; i < testDouble.length; i++) {
      assertEquals(valueWrappers.get(i).index, convertResultWrappers.get(i).index);
    }
  }

  private class ConvertResultWrapper<T> {
    T index;
    byte[] result;
    public ConvertResultWrapper(T index, byte[] result) {
      this.index = index;
      this.result = result;
    }
  }

  private class OrginValueWrapper<T> {
    T index;
    T originValue;
    public OrginValueWrapper(T index, T originValue) {
      this.index = index;
      this.originValue = originValue;
    }
  }

  @Test
  public void testConvertBytesToLong() {
    long[] tests = new long[] {Long.MIN_VALUE, -1L, 0, 1L, Long.MAX_VALUE};
    for (int i = 0; i < tests.length; i++) {
      assertEquals(BinaryUtil.convertBytesToLong(convertLongToBytes(tests[i])), tests[i]);
    }
  }

  @Test
  public void testConvertBytesToLongWithPadding() {
    byte[] bytes = new byte[2];
    bytes[0] = 2;
    bytes[1] = 127;
    assertEquals(BinaryUtil.convertBytesToLong(bytes), 2 * 256 + 127);
  }

  private byte[] convertLongToBytes(long num) {
    byte[] byteNum = new byte[8];
    for (int i = 0; i < 8; i++) {
      int offset = 64 - (i + 1) * 8;
      byteNum[i] = (byte) ((num >> offset) & 0xff);
    }
    return byteNum;
  }
}
