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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.apache.hudi.config.HoodieWriteConfig.CREATE_HANDLE_CLASS_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class TestCreateHandleFactory {
  private static final String CUSTOM_CREATE_HANDLE = "io.custom.CustomCreateHandle.java";
  private static final String INVALID_CREATE_HANDLE_CLASS_NAME = "ai.onehouse.InvalidCreateHandle";
  private static final String COMMIT_TIME = "20231201120000";
  private static final String PARTITION_PATH = "partition1";
  private static final String FILE_ID_PREFIX = "file";

  @Mock
  private HoodieTable mockHoodieTable;

  @Mock
  private HoodieRecordMerger mockRecordMerger;

  @Mock
  private TaskContextSupplier mockTaskContextSupplier;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateMethodWithDefaultProperties(boolean preserveMetadata) {
    // Test successful fallback when baseFile is not Parquet

    CreateHandleFactory createHandleFactory = new CreateHandleFactory(preserveMetadata);

    Properties properties = new Properties();
    HoodieWriteConfig config = getWriterConfig(properties);
    when(config.isCreateHandleFallbackEnabled()).thenReturn(true);

    try (MockedStatic<ReflectionUtils> mockedStatic = mockStatic(ReflectionUtils.class)) {
      HoodieWriteHandle mockHandle = mock(HoodieWriteHandle.class);

      // Only mock the specific loadClass call we want to intercept
      mockedStatic.when(() -> ReflectionUtils.loadClass(
          eq(HoodieCreateHandle.class.getName()),
          any(Class[].class),
          any(HoodieWriteConfig.class), any(String.class), any(HoodieTable.class), any(String.class), any(String.class), any(Option.class), any(TaskContextSupplier.class), any(boolean.class)
      )).thenReturn(mockHandle);

      // Allow all other ReflectionUtils calls to pass through
      mockedStatic.when(() -> ReflectionUtils.getClass(any(String.class))).thenCallRealMethod();

      HoodieWriteHandle result = createHandleFactory.create(config, COMMIT_TIME, mockHoodieTable, 
          PARTITION_PATH, FILE_ID_PREFIX, mockTaskContextSupplier);
      assertEquals(mockHandle, result);
      
      // Verify ReflectionUtils.loadClass was called with correct arguments
      String expectedNextFileId = FILE_ID_PREFIX + "-0";
      mockedStatic.verify(() -> ReflectionUtils.loadClass(
          HoodieCreateHandle.class.getName(),
          new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, String.class, String.class, Option.class, TaskContextSupplier.class, boolean.class},
          config, COMMIT_TIME, mockHoodieTable, PARTITION_PATH, expectedNextFileId, Option.empty(), mockTaskContextSupplier, preserveMetadata
      ));

      // Verify custom class was never attempted (since record type is not AVRO)
      mockedStatic.verify(() -> ReflectionUtils.loadClass(
          eq(CUSTOM_CREATE_HANDLE),
          any(Class[].class),
          any(), any(), any(), any(), any(), any(), any()
      ), never());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateMethodWithInvalidClass(boolean isFallbackEnabled) {
    // Test successful fallback when createHandle class cannot be loaded
    // If fallback is enabled, should load the default create handle class
    // If fallback is disabled, an error should be thrown

    CreateHandleFactory createHandleFactory = new CreateHandleFactory();
    Properties properties = new Properties();
    properties.setProperty(CREATE_HANDLE_CLASS_NAME.key(), INVALID_CREATE_HANDLE_CLASS_NAME);

    HoodieWriteConfig config = getWriterConfig(properties);
    when(config.isCreateHandleFallbackEnabled()).thenReturn(isFallbackEnabled);

    String expectedNextFileId = FILE_ID_PREFIX + "-0";

    try (MockedStatic<ReflectionUtils> mockedStatic = mockStatic(ReflectionUtils.class)) {
      HoodieWriteHandle mockHandle = mock(HoodieWriteHandle.class);
      
      // Allow all other ReflectionUtils calls to pass through
      mockedStatic.when(() -> ReflectionUtils.getClass(any(String.class))).thenCallRealMethod();

      // First call (invalid class) throws exception
      mockedStatic.when(() -> ReflectionUtils.loadClass(
          eq(INVALID_CREATE_HANDLE_CLASS_NAME),
          any(Class[].class),
          any(), any(), any(), any(), any(), any(), any(), any()
      )).thenThrow(new HoodieException("Class not found"));

      if (isFallbackEnabled) {
        // Fallback call should succeed
        mockedStatic.when(() -> ReflectionUtils.loadClass(
            eq(HoodieCreateHandle.class.getName()),
            any(Class[].class),
            any(HoodieWriteConfig.class), any(String.class), any(HoodieTable.class), any(String.class), any(String.class), any(Option.class), any(TaskContextSupplier.class), any(boolean.class)
        )).thenReturn(mockHandle);
      }

      if (isFallbackEnabled) {
        // When fallback is enabled, create call should succeed with Reflection to default class
        HoodieWriteHandle result = createHandleFactory.create(config, COMMIT_TIME, mockHoodieTable,
            PARTITION_PATH, FILE_ID_PREFIX, mockTaskContextSupplier);
        assertEquals(mockHandle, result);

        mockedStatic.verify(() -> ReflectionUtils.loadClass(
            HoodieCreateHandle.class.getName(),
            new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, String.class, String.class, Option.class, TaskContextSupplier.class, boolean.class},
            config, COMMIT_TIME, mockHoodieTable, PARTITION_PATH, expectedNextFileId, Option.empty(), mockTaskContextSupplier, false
        ));
      } else {
        // When fallback is disabled, create call should fail without Reflection to default class
        HoodieException exception = Assertions.assertThrows(HoodieException.class, () -> {
          createHandleFactory.create(config, COMMIT_TIME, mockHoodieTable,
              PARTITION_PATH, FILE_ID_PREFIX, mockTaskContextSupplier);
        });

        assertEquals("Could not instantiate the HoodieCreateHandle implementation: " + INVALID_CREATE_HANDLE_CLASS_NAME, exception.getMessage());

        // Verify fallback was NOT attempted
        mockedStatic.verify(() -> ReflectionUtils.loadClass(
            eq(HoodieCreateHandle.class.getName()),
            any(Class[].class),
            any(), any(), any(), any(), any(), any(), any(), any()
        ), never());
      }

      // Verify call to invalid class was made for both cases
      mockedStatic.verify(() -> ReflectionUtils.loadClass(
          INVALID_CREATE_HANDLE_CLASS_NAME,
          new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, String.class, String.class, Option.class, TaskContextSupplier.class, boolean.class},
          config, COMMIT_TIME, mockHoodieTable, PARTITION_PATH, expectedNextFileId, Option.empty(), mockTaskContextSupplier, false
      ));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCreateMethodSuccess_customCreateHandle(boolean preserveMetadata) {
    // Test successful custom create handle class loading when baseFile is parquet and record is avro
    CreateHandleFactory createHandleFactory = new CreateHandleFactory(preserveMetadata);

    Properties properties = new Properties();
    properties.setProperty(CREATE_HANDLE_CLASS_NAME.key(), CUSTOM_CREATE_HANDLE);

    HoodieWriteConfig config = getWriterConfig(properties);
    when(config.isCreateHandleFallbackEnabled()).thenReturn(true);

    String expectedNextFileId = FILE_ID_PREFIX + "-0";

    try (MockedStatic<ReflectionUtils> mockedStatic = mockStatic(ReflectionUtils.class)) {
      HoodieWriteHandle mockHandle = mock(HoodieWriteHandle.class);

      // Mock successful loading of custom create handle class
      mockedStatic.when(() -> ReflectionUtils.loadClass(
          eq(CUSTOM_CREATE_HANDLE),
          any(Class[].class),
          any(HoodieWriteConfig.class), any(String.class), any(HoodieTable.class), any(String.class), any(String.class), any(Option.class), any(TaskContextSupplier.class), any(boolean.class)
      )).thenReturn(mockHandle);

      // Allow all other ReflectionUtils calls to pass through
      mockedStatic.when(() -> ReflectionUtils.getClass(any(String.class))).thenCallRealMethod();

      HoodieWriteHandle result = createHandleFactory.create(config, COMMIT_TIME, mockHoodieTable,
          PARTITION_PATH, FILE_ID_PREFIX, mockTaskContextSupplier);
      assertEquals(mockHandle, result);

      // Verify that custom create handle class was successfully loaded
      mockedStatic.verify(() -> ReflectionUtils.loadClass(
          CUSTOM_CREATE_HANDLE,
          new Class<?>[] {HoodieWriteConfig.class, String.class, HoodieTable.class, String.class, String.class, Option.class, TaskContextSupplier.class, boolean.class},
          config, COMMIT_TIME, mockHoodieTable, PARTITION_PATH, expectedNextFileId, Option.empty(), mockTaskContextSupplier, preserveMetadata
      ));

      // Verify fallback was never attempted since custom class loading succeeded
      mockedStatic.verify(() -> ReflectionUtils.loadClass(
          eq(HoodieCreateHandle.class.getName()),
          any(Class[].class),
          any(), any(), any(), any(), any(), any(), any(), any()
      ), never());
    }
  }

  private HoodieWriteConfig getWriterConfig(Properties properties) {
    return spy(HoodieWriteConfig.newBuilder()
        .withPath("/tmp/base_path")
        .withSchema("{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}")
        .withProperties(properties)
        .build());
  }
}