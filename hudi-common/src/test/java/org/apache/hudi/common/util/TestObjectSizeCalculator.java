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

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.ObjectSizeCalculator.getObjectSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestObjectSizeCalculator {

  @Test
  public void testGetObjectSize() {
    EmptyClass emptyClass = new EmptyClass();
    StringClass stringClass = new StringClass();
    PayloadClass payloadClass = new PayloadClass();
    String emptyString = "";
    String string = "hello";
    String[] stringArray = {emptyString, string, " world"};
    String[] anotherStringArray = new String[100];
    List<String> stringList = new ArrayList<>();
    StringBuilder stringBuilder = new StringBuilder(100);
    int maxIntPrimitive = Integer.MAX_VALUE;
    int minIntPrimitive = Integer.MIN_VALUE;
    Integer maxInteger = Integer.MAX_VALUE;
    Integer minInteger = Integer.MIN_VALUE;
    long zeroLong = 0L;
    double zeroDouble = 0.0;
    boolean booleanField = true;
    Object object = new Object();

    assertEquals(24, getObjectSize(emptyString));
    assertEquals(24, getObjectSize(string));
    assertEquals(16, getObjectSize(stringArray));
    assertEquals(416, getObjectSize(anotherStringArray));
    assertEquals(24, getObjectSize(stringList));
    assertEquals(24, getObjectSize(stringBuilder));
    assertEquals(16, getObjectSize(maxIntPrimitive));
    assertEquals(16, getObjectSize(minIntPrimitive));
    assertEquals(16, getObjectSize(maxInteger));
    assertEquals(16, getObjectSize(minInteger));
    assertEquals(24, getObjectSize(zeroLong));
    assertEquals(24, getObjectSize(zeroDouble));
    assertEquals(16, getObjectSize(booleanField));
    assertEquals(24, getObjectSize(DayOfWeek.TUESDAY));
    assertEquals(16, getObjectSize(object));
    assertEquals(32, getObjectSize(emptyClass));
    assertEquals(24, getObjectSize(stringClass));
    assertEquals(24, getObjectSize(payloadClass));
    assertEquals(32, getObjectSize(Schema.create(Schema.Type.STRING)));
  }

  class EmptyClass {
  }

  class StringClass {
    private String s;
  }

  class PayloadClass implements Serializable {
    private HoodieRecord record;
  }

  public enum DayOfWeek {
    MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY
  }
}
