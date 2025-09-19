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

package org.apache.hudi.common.util;

import org.apache.hudi.common.data.HoodieData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieDataUtils {

  @Mock
  private HoodieData<String> mockHoodieData;

  @Test
  public void testWithHoodieDataCleanUp_ComplexFunction() {
    // Given
    List<String> testData = Arrays.asList("test1", "test2", "test3");
    when(mockHoodieData.collectAsList()).thenReturn(testData);
    
    Function<HoodieData<String>, String> function = data -> {
      List<String> list = data.collectAsList();
      return String.join(",", list);
    };

    // When
    String result = HoodieDataUtils.withHoodieDataCleanUp(mockHoodieData, function);

    // Then
    assertEquals("test1,test2,test3", result);
    verify(mockHoodieData, times(1)).unpersistWithDependencies();
  }

  @Test
  public void testWithHoodieDataCleanUpOnException_ComplexFunction() {
    // Given
    List<String> testData = Arrays.asList("test1", "test2", "test3");
    when(mockHoodieData.collectAsList()).thenReturn(testData);
    
    Function<HoodieData<String>, String> function = data -> {
      List<String> list = data.collectAsList();
      return String.join(",", list);
    };

    // When
    String result = HoodieDataUtils.withHoodieDataCleanUpOnException(mockHoodieData, function);

    // Then
    assertEquals("test1,test2,test3", result);
    // Should NOT call unpersistWithDependencies on success
    verify(mockHoodieData, times(0)).unpersistWithDependencies();
  }

  @Test
  public void testWithHoodieDataCleanUp_NullFunction() {
    // Given
    Function<HoodieData<String>, Integer> function = null;

    // When & Then
    assertThrows(NullPointerException.class, () -> {
      HoodieDataUtils.withHoodieDataCleanUp(mockHoodieData, function);
    });
    
    // Verify cleanup still happens even on NPE
    verify(mockHoodieData, times(1)).unpersistWithDependencies();
  }

  @Test
  public void testWithHoodieDataCleanUpOnException_NullFunction() {
    // Given
    Function<HoodieData<String>, Integer> function = null;

    // When & Then
    assertThrows(NullPointerException.class, () -> {
      HoodieDataUtils.withHoodieDataCleanUpOnException(mockHoodieData, function);
    });
    
    // Verify cleanup happens on NPE
    verify(mockHoodieData, times(1)).unpersistWithDependencies();
  }
}
