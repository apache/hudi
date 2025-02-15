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

package org.apache.hudi.client.transaction.lock.models;

import org.apache.hudi.exception.HoodieIOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class ConditionalWriteLockFileTest {

  private static final String JSON_DATA = "{\"expired\":false,\"validUntil\":1700000000000,\"owner\":\"testOwner\"}";
  private static final String JSON_DATA_EXTRA_FIELD = "{\"expired\":true,\"validUntil\":1600000000000,\"owner\":\"otherOwner\",\"state\":\"active\"}";
  private static final String INVALID_JSON = "{\"invalidField\":123}";
  private static final String VERSION_ID = "testVersionId";

  private InputStream validJsonStream;
  private InputStream extraFieldValidJsonStream;
  private InputStream invalidJsonStream;

  @BeforeEach
  void setup() {
    validJsonStream = new ByteArrayInputStream(JSON_DATA.getBytes());
    extraFieldValidJsonStream = new ByteArrayInputStream(JSON_DATA_EXTRA_FIELD.getBytes());
    invalidJsonStream = new ByteArrayInputStream(INVALID_JSON.getBytes());
  }

  @Test
  void testCreateValidInputStream() {
    ConditionalWriteLockFile file = ConditionalWriteLockFile.createFromStream(validJsonStream, VERSION_ID);
    assertEquals(1700000000000L, file.getValidUntil());
    assertEquals("testOwner", file.getOwner());
    assertEquals(VERSION_ID, file.getVersionId());
    assertFalse(file.isExpired());
  }

  @Test
  void testCreateValidInputStreamExtraField() {
    ConditionalWriteLockFile file = ConditionalWriteLockFile.createFromStream(extraFieldValidJsonStream, VERSION_ID);
    assertEquals(1600000000000L, file.getValidUntil());
    assertEquals("otherOwner", file.getOwner());
    assertEquals(VERSION_ID, file.getVersionId());
    assertTrue(file.isExpired());
  }

  @Test
  void testCreateInvalidInputStreamFromMock() throws IOException {
    InputStream mockInputStream = mock(InputStream.class);

    doThrow(new IOException("Simulated IOException"))
        .when(mockInputStream)
        .read();
    HoodieIOException exception = assertThrows(HoodieIOException.class, () -> ConditionalWriteLockFile.createFromStream(mockInputStream, "versionId"));
    assertTrue(exception.getMessage().contains("Failed to deserialize"));
  }

  @Test
  void testCreateInvalidInputStreamFromBadData() {
    HoodieIOException exception = assertThrows(HoodieIOException.class, () ->
        ConditionalWriteLockFile.createFromStream(invalidJsonStream, VERSION_ID)
    );
    assertTrue(exception.getMessage().contains("Failed to deserialize"));
  }

  @Test
  void testCreateNullData() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
        new ConditionalWriteLockFile(null, VERSION_ID)
    );
    assertTrue(exception.getMessage().contains("Data must not be null"));
  }

  @Test
  void testCreateNullVersionId() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(true, 1700000000000L, "testOwner");
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
        new ConditionalWriteLockFile(data, null)
    );
    assertTrue(exception.getMessage().contains("VersionId must not be null or empty."));
    exception = assertThrows(IllegalArgumentException.class, () ->
        new ConditionalWriteLockFile(data, "")
    );
    assertTrue(exception.getMessage().contains("VersionId must not be null or empty."));
  }

  @Test
  void testToJsonStreamValidData() {
    ConditionalWriteLockFile file = ConditionalWriteLockFile.createFromStream(validJsonStream, VERSION_ID);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    file.writeToStream(outputStream);
    String outputJson = new String(outputStream.toByteArray());
    assertTrue(outputJson.contains("\"expired\":false"));
    assertTrue(outputJson.contains("\"validUntil\":1700000000000"));
    assertTrue(outputJson.contains("\"owner\":\"testOwner\""));
  }

  @Test
  void testToJsonStreamErrorHandling() throws IOException {
    OutputStream mockOutputStream = mock(OutputStream.class);

    doThrow(new IOException("Simulated IOException"))
        .when(mockOutputStream)
        .write(any(byte[].class), anyInt(), anyInt());
    ConditionalWriteLockFile file = new ConditionalWriteLockFile(
        new ConditionalWriteLockData(true, System.currentTimeMillis() + 1000, "testOwner"),
        VERSION_ID);

    HoodieIOException exception = assertThrows(HoodieIOException.class, () -> file.writeToStream(mockOutputStream));
    assertTrue(exception.getMessage().contains("Error writing object to JSON"));
  }

  @Test
  void testToByteArrayValidData() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, 1700000000000L, "testOwner");
    String outputJson = new String(ConditionalWriteLockFile.toByteArray(data));
    assertTrue(outputJson.contains("\"expired\":false"));
    assertTrue(outputJson.contains("\"validUntil\":1700000000000"));
    assertTrue(outputJson.contains("\"owner\":\"testOwner\""));
  }

  @Test
  void testIsExpired() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(true, System.currentTimeMillis() - 1000, "testOwner");
    ConditionalWriteLockFile file = new ConditionalWriteLockFile(data, VERSION_ID);
    assertTrue(file.isExpired());
  }

  @Test
  void testGetVersionId() {
    ConditionalWriteLockData data = new ConditionalWriteLockData(false, 1700000000000L, "testOwner");
    ConditionalWriteLockFile file = new ConditionalWriteLockFile(data, VERSION_ID);
    assertEquals(VERSION_ID, file.getVersionId());
  }
}