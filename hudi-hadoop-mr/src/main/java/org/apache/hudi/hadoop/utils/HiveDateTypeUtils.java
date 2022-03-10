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

package org.apache.hudi.hadoop.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hudi.exception.HoodieException;

import java.lang.reflect.Constructor;

public class HiveDateTypeUtils {
  // this class is unique to Hive3 and cannot be found in Hive2
  private static final String HIVE_UNIQUE_CLASS = "org.apache.hadoop.hive.serde2.io.DateWritableV2";
  private static final String HIVE3_DATE_WRITE_TYPE = "org.apache.hadoop.hive.serde2.io.DateWritableV2";
  private static final String HIVE2_DATE_WRITE_TYPE = "org.apache.hadoop.hive.serde2.io.DateWritable";
  private static Constructor<?> dateWriteableTypeConstructor;

  static {
    setDateWriteableTypeConstructor();
  }

  public static Writable getDateType(int value) {
    try {
      return (Writable) dateWriteableTypeConstructor.newInstance(value);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  private static void setDateWriteableTypeConstructor() {
    try {
      Class<?> clazz = Class.forName(hive3Exist() ? HIVE3_DATE_WRITE_TYPE : HIVE2_DATE_WRITE_TYPE);
      dateWriteableTypeConstructor = clazz.getDeclaredConstructor(int.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean hive3Exist() {
    try {
      Class.forName(HIVE_UNIQUE_CLASS);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
