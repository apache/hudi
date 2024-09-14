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

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.Writable;

import java.sql.Timestamp;

/**
 * Shim clazz for Hive2.
 */
public class Hive2Shim implements HiveShim {
  private static final Hive2Shim INSTANCE = new Hive2Shim();

  private Hive2Shim() {
  }

  public static Hive2Shim getInstance() {
    return INSTANCE;
  }

  public Writable getTimestampWriteable(long value, boolean timestampMillis) {
    Timestamp timestamp = new Timestamp(timestampMillis ? value : value / 1000);
    return new TimestampWritable(timestamp);
  }

  public Writable getDateWriteable(int value) {
    return new DateWritable(value);
  }

  public int getDays(Object dateWritable) {
    return ((DateWritable) dateWritable).getDays();
  }

  public long getMills(Object timestampWritable) {
    return ((TimestampWritable) timestampWritable).getTimestamp().getTime();
  }
}
