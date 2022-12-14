/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import org.apache.hudi.util.DataTypeUtils;

import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 */
public class TestDataTypeUtils {
  @Test
  public void testGetAsLong() {
    long t1 = DataTypeUtils.getAsLong("2012-12-12T12:12:12", DataTypes.TIMESTAMP(3).getLogicalType());
    assertEquals(1355314332000L, t1);

    long t2 = DataTypeUtils.getAsLong("2012-12-12 12:12:12", DataTypes.TIME().getLogicalType());
    assertEquals(1355314332000L, t2);

    long t3 = DataTypeUtils.getAsLong("2012-12-12", DataTypes.DATE().getLogicalType());
    assertEquals(1355270400000L, t3);

    long t4 = DataTypeUtils.getAsLong(100L, DataTypes.BIGINT().getLogicalType());
    assertEquals(100L, t4);

    long t5 = DataTypeUtils.getAsLong(100, DataTypes.INT().getLogicalType());
    assertEquals(100, t5);
  }
}