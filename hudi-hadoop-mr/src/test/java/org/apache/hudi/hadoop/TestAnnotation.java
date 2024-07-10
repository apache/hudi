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

package org.apache.hudi.hadoop;

import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAnnotation {

  @Test
  public void testHoodieParquetInputFormatAnnotation() {
    assertTrue(HoodieParquetInputFormat.class.isAnnotationPresent(UseFileSplitsFromInputFormat.class));
    assertTrue(HoodieParquetInputFormat.class.isAnnotationPresent(UseRecordReaderFromInputFormat.class));
    Annotation[] annotations = HoodieParquetInputFormat.class.getAnnotations();
    boolean foundFileSplitsAnnotation = false;
    boolean foundRecordReaderAnnotation = false;
    for (Annotation annotation : annotations) {
      if (UseFileSplitsFromInputFormat.class.getSimpleName().equals(annotation.annotationType().getSimpleName())) {
        foundFileSplitsAnnotation = true;
      } else if (UseRecordReaderFromInputFormat.class.getSimpleName().equals(annotation.annotationType().getSimpleName())) {
        foundRecordReaderAnnotation = true;
      }
    }
    assertTrue(foundFileSplitsAnnotation);
    assertTrue(foundRecordReaderAnnotation);
  }

  @Test
  public void testHoodieParquetRealtimeInputFormatAnnotations() {
    assertTrue(HoodieParquetRealtimeInputFormat.class.isAnnotationPresent(UseFileSplitsFromInputFormat.class));
    assertTrue(HoodieParquetRealtimeInputFormat.class.isAnnotationPresent(UseRecordReaderFromInputFormat.class));
    Annotation[] annotations = HoodieParquetRealtimeInputFormat.class.getAnnotations();
    boolean foundFileSplitsAnnotation = false;
    boolean foundRecordReaderAnnotation = false;
    for (Annotation annotation : annotations) {
      if (UseFileSplitsFromInputFormat.class.getSimpleName().equals(annotation.annotationType().getSimpleName())) {
        foundFileSplitsAnnotation = true;
      } else if (UseRecordReaderFromInputFormat.class.getSimpleName().equals(annotation.annotationType().getSimpleName())) {
        foundRecordReaderAnnotation = true;
      }
    }
    assertTrue(foundFileSplitsAnnotation);
    assertTrue(foundRecordReaderAnnotation);
  }
}
