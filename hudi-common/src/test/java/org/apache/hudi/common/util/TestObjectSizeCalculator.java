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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

    Assertions.assertDoesNotThrow(() -> {
      printObjectSize(emptyString);
      printObjectSize(string);
      printObjectSize(stringArray);
      printObjectSize(anotherStringArray);
      printObjectSize(stringList);
      printObjectSize(stringBuilder);
      printObjectSize(maxIntPrimitive);
      printObjectSize(minIntPrimitive);
      printObjectSize(maxInteger);
      printObjectSize(minInteger);
      printObjectSize(zeroLong);
      printObjectSize(zeroDouble);
      printObjectSize(booleanField);
      printObjectSize(DayOfWeek.TUESDAY);
      printObjectSize(object);
      printObjectSize(emptyClass);
      printObjectSize(stringClass);
      printObjectSize(payloadClass);
      printObjectSize(Schema.create(Schema.Type.STRING));
    });
  }

  public static void printObjectSize(Object object) {
    System.out.println("Object type: " + object.getClass() + ", size: " + ObjectSizeCalculator.getObjectSize(object) + " bytes");
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
