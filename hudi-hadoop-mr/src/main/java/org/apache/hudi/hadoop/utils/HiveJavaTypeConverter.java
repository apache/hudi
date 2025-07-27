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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.util.DefaultJavaTypeConverter;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;

public class HiveJavaTypeConverter extends DefaultJavaTypeConverter {
  @Override
  public boolean castToBoolean(Object value) {
    if (value instanceof BooleanWritable) {
      return ((BooleanWritable) value).get();
    } else {
      throw new IllegalArgumentException("Expected BooleanWritable but got " + value.getClass());
    }
  }

  @Override
  public String castToString(Object value) {
    if (value instanceof Text) {
      return value.toString();
    } else {
      throw new IllegalArgumentException("Expected Text but got " + value.getClass());
    }
  }
}
