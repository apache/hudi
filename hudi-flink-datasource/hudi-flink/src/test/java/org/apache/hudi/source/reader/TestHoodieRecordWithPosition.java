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

package org.apache.hudi.source.reader;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieRecordWithPosition}.
 */
public class TestHoodieRecordWithPosition {

  @Test
  public void testConstructorWithParameters() {
    String record = "test-record";
    int fileOffset = 5;
    long recordOffset = 100L;

    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>(record, fileOffset, recordOffset);

    assertEquals(record, recordWithPosition.record());
    assertEquals(fileOffset, recordWithPosition.fileOffset());
    assertEquals(recordOffset, recordWithPosition.recordOffset());
  }

  @Test
  public void testDefaultConstructor() {
    HoodieRecordWithPosition<String> recordWithPosition = new HoodieRecordWithPosition<>();

    assertNull(recordWithPosition.record());
    assertEquals(0, recordWithPosition.fileOffset());
    assertEquals(0L, recordWithPosition.recordOffset());
  }

  @Test
  public void testSetMethod() {
    HoodieRecordWithPosition<String> recordWithPosition = new HoodieRecordWithPosition<>();

    String newRecord = "new-record";
    int newFileOffset = 10;
    long newRecordOffset = 200L;

    recordWithPosition.set(newRecord, newFileOffset, newRecordOffset);

    assertEquals(newRecord, recordWithPosition.record());
    assertEquals(newFileOffset, recordWithPosition.fileOffset());
    assertEquals(newRecordOffset, recordWithPosition.recordOffset());
  }

  @Test
  public void testSetMethodOverwritesPreviousValues() {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>("old-record", 1, 10L);

    recordWithPosition.set("new-record", 2, 20L);

    assertEquals("new-record", recordWithPosition.record());
    assertEquals(2, recordWithPosition.fileOffset());
    assertEquals(20L, recordWithPosition.recordOffset());
  }

  @Test
  public void testRecordMethod() {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>("initial", 0, 5L);

    // record() method should increment recordOffset by 1
    recordWithPosition.record("next-record");

    assertEquals("next-record", recordWithPosition.record());
    assertEquals(0, recordWithPosition.fileOffset(), "File offset should remain unchanged");
    assertEquals(6L, recordWithPosition.recordOffset(), "Record offset should increment by 1");
  }

  @Test
  public void testRecordMethodMultipleCalls() {
    HoodieRecordWithPosition<Integer> recordWithPosition =
        new HoodieRecordWithPosition<>(1, 0, 0L);

    recordWithPosition.record(2);
    assertEquals(2, recordWithPosition.record());
    assertEquals(1L, recordWithPosition.recordOffset());

    recordWithPosition.record(3);
    assertEquals(3, recordWithPosition.record());
    assertEquals(2L, recordWithPosition.recordOffset());

    recordWithPosition.record(4);
    assertEquals(4, recordWithPosition.record());
    assertEquals(3L, recordWithPosition.recordOffset());
  }

  @Test
  public void testRecordMethodWithNullRecord() {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>("initial", 5, 10L);

    recordWithPosition.record(null);

    assertNull(recordWithPosition.record());
    assertEquals(5, recordWithPosition.fileOffset());
    assertEquals(11L, recordWithPosition.recordOffset());
  }

  @Test
  public void testToString() {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>("test-data", 3, 42L);

    String result = recordWithPosition.toString();

    assertTrue(result.contains("test-data"));
    assertTrue(result.contains("3"));
    assertTrue(result.contains("42"));
  }

  @Test
  public void testToStringWithNullRecord() {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>(null, 1, 5L);

    String result = recordWithPosition.toString();

    assertTrue(result.contains("null"));
    assertTrue(result.contains("1"));
    assertTrue(result.contains("5"));
  }

  @Test
  public void testFileOffsetIndependentOfRecord() {
    HoodieRecordWithPosition<String> recordWithPosition =
        new HoodieRecordWithPosition<>("initial", 10, 0L);

    // Using record() method should not affect fileOffset
    recordWithPosition.record("record1");
    recordWithPosition.record("record2");
    recordWithPosition.record("record3");

    assertEquals(10, recordWithPosition.fileOffset(), "File offset should remain constant");
    assertEquals(3L, recordWithPosition.recordOffset());
  }

  @Test
  public void testSetResetsRecordOffset() {
    HoodieRecordWithPosition<String> recordWithPosition = new HoodieRecordWithPosition<>();

    // Increment record offset using record() method
    recordWithPosition.record("record1");
    recordWithPosition.record("record2");
    assertEquals(2L, recordWithPosition.recordOffset());

    // Using set() should overwrite the offset
    recordWithPosition.set("new-record", 5, 100L);
    assertEquals(100L, recordWithPosition.recordOffset());
  }

  @Test
  public void testDifferentRecordTypes() {
    // Test with Integer
    HoodieRecordWithPosition<Integer> intRecord = new HoodieRecordWithPosition<>(42, 0, 0L);
    assertEquals(42, intRecord.record());

    // Test with custom object
    TestObject obj = new TestObject("test", 123);
    HoodieRecordWithPosition<TestObject> objRecord = new HoodieRecordWithPosition<>(obj, 1, 1L);
    assertEquals(obj, objRecord.record());
    assertEquals("test", objRecord.record().name);
    assertEquals(123, objRecord.record().value);
  }

  /**
   * Helper class for testing with custom objects.
   */
  private static class TestObject {
    String name;
    int value;

    TestObject(String name, int value) {
      this.name = name;
      this.value = value;
    }
  }
}
