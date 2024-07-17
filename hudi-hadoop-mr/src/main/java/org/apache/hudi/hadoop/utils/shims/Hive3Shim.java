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

package org.apache.hudi.hadoop.utils.shims;

import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Timestamp;

/**
 * Shim clazz for Hive3.
 */
public class Hive3Shim implements HiveShim {

  public static final Logger LOG = LoggerFactory.getLogger(Hive3Shim.class);

  public static final String TIMESTAMP_CLASS_NAME = "org.apache.hadoop.hive.common.type.Timestamp";
  public static final String TIMESTAMP_WRITEABLE_V2_CLASS_NAME = "org.apache.hadoop.hive.serde2.io.TimestampWritableV2";
  public static final String DATE_WRITEABLE_V2_CLASS_NAME = "org.apache.hadoop.hive.serde2.io.DateWritableV2";

  private static Class<?> TIMESTAMP_CLASS = null;
  private static Class<?> TIMESTAMP_WRITABLE_V2_CLASS = null;
  private static Method SET_TIME_IN_MILLIS = null;
  private static Method TO_SQL_TIMESTAMP = null;
  private static Method GET_TIMESTAMP = null;
  private static Constructor<?> TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR = null;

  private static Class<?> DATE_WRITEABLE_CLASS = null;
  private static Method GET_DAYS = null;
  private static Constructor<?> DATE_WRITEABLE_V2_CONSTRUCTOR = null;

  static {
    // timestamp
    try {
      // Hive.Timestamp methods
      TIMESTAMP_CLASS = Class.forName(TIMESTAMP_CLASS_NAME);
      SET_TIME_IN_MILLIS = TIMESTAMP_CLASS.getDeclaredMethod("setTimeInMillis", long.class);
      TO_SQL_TIMESTAMP = TIMESTAMP_CLASS.getDeclaredMethod("toSqlTimestamp");

      // Hive.TimestampWritable methods
      TIMESTAMP_WRITABLE_V2_CLASS = Class.forName(TIMESTAMP_WRITEABLE_V2_CLASS_NAME);
      GET_TIMESTAMP = TIMESTAMP_WRITABLE_V2_CLASS.getDeclaredMethod("getTimestamp");
      TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR = TIMESTAMP_WRITABLE_V2_CLASS.getConstructor(TIMESTAMP_CLASS);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      // This will be printed out when Hive3Shim initialization as a singleton fails
      LOG.warn("cannot find hive3 timestampv2 class or method, use hive2 class!", e);
    }

    // date
    try {
      DATE_WRITEABLE_CLASS = Class.forName(DATE_WRITEABLE_V2_CLASS_NAME);
      GET_DAYS = DATE_WRITEABLE_CLASS.getDeclaredMethod("getDays");
      DATE_WRITEABLE_V2_CONSTRUCTOR = DATE_WRITEABLE_CLASS.getConstructor(int.class);
    } catch (ClassNotFoundException | NoSuchMethodException e) {
      // This will be printed out when Hive3Shim initialization as a singleton fails
      LOG.warn("cannot find hive3 datev2 class or method, use hive2 class!", e);
    }
  }

  private static final Hive3Shim INSTANCE = new Hive3Shim();

  private Hive3Shim() {
  }

  public static Hive3Shim getInstance() {
    return INSTANCE;
  }

  /**
   * Get timestamp writeable object from long value.
   * Hive3 use TimestampWritableV2 to build timestamp objects and Hive2 use TimestampWritable.
   * So that we need to initialize timestamp according to the version of Hive.
   */
  public Writable getTimestampWriteable(long value, boolean timestampMillis) {
    try {
      Object timestamp = TIMESTAMP_CLASS.newInstance();
      SET_TIME_IN_MILLIS.invoke(timestamp, timestampMillis ? value : value / 1000);
      return (Writable) TIMESTAMP_WRITEABLE_V2_CONSTRUCTOR.newInstance(timestamp);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new HoodieException("can not create writable v2 class!", e);
    }
  }

  /**
   * Get date writeable object from int value.
   * Hive3 use DateWritableV2 to build date objects and Hive2 use DateWritable.
   * So that we need to initialize date according to the version of Hive.
   */
  public Writable getDateWriteable(int value) {
    try {
      return (Writable) DATE_WRITEABLE_V2_CONSTRUCTOR.newInstance(value);
    } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
      throw new HoodieException("can not create writable v2 class!", e);
    }
  }

  public int getDays(Object dateWritable) {
    try {
      return (int) GET_DAYS.invoke(dateWritable);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException("can not create writable v2 class!", e);
    }
  }

  public long getMills(Object timestampWritable) {
    try {
      Object hiveTimestamp = GET_TIMESTAMP.invoke(timestampWritable);
      return ((Timestamp) TO_SQL_TIMESTAMP.invoke(hiveTimestamp)).getTime();
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException("can not create writable v2 class!", e);
    }
  }
}
