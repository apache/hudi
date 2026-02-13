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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StorageLockFile {

  private final StorageLockData data;
  private final String versionId;

  // Get a custom object mapper. See StorageLockData for required properties.
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      // This allows us to add new properties down the line.
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
      // Should not let validUntil or expired be null.
      .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES);

  /**
   * Initializes a StorageLockFile using the data and unique versionId.
   *
   * @param data      The data in the lock file.
   * @param versionId The version of this lock file. Used to ensure consistency through conditional writes.
   */
  public StorageLockFile(StorageLockData data, String versionId) {
    ValidationUtils.checkArgument(data != null, "Data must not be null.");
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(versionId), "VersionId must not be null or empty.");
    this.data = data;
    this.versionId = versionId;
  }

  /**
   * Factory method to load an input stream into a StorageLockFile.
   *
   * @param inputStream The input stream of the JSON content.
   * @param versionId   The unique version identifier for the lock file.
   * @return A new instance of StorageLockFile.
   * @throws HoodieIOException If deserialization fails.
   */
  public static StorageLockFile createFromStream(InputStream inputStream, String versionId) {
    try {
      StorageLockData data = OBJECT_MAPPER.readValue(inputStream, StorageLockData.class);
      return new StorageLockFile(data, versionId);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to deserialize JSON content into StorageLockData", e);
    }
  }

  /**
   * Writes the serialized JSON representation of this object to an output stream.
   *
   * @param outputStream The output stream to write the JSON to.
   * @throws HoodieIOException If serialization fails.
   */
  public void writeToStream(OutputStream outputStream) {
    try {
      OBJECT_MAPPER.writeValue(outputStream, this.data);
    } catch (IOException e) {
      throw new HoodieIOException("Error writing object to JSON output stream", e);
    }
  }

  /**
   * Converts the data to a bytearray. Since we know the payloads will be small this is fine.
   * @return A byte array.
   * @throws HoodieIOException If serialization fails.
   */
  public static byte[] toByteArray(StorageLockData data) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(data);
    } catch (JsonProcessingException e) {
      throw new HoodieIOException("Error writing object to byte array", e);
    }
  }

  /**
   * Gets the version id.
   * @return A string for the version id.
   */
  public String getVersionId() {
    return this.versionId;
  }

  /**
   * Gets the expiration time in ms.
   * @return A long representing the expiration.
   */
  public long getValidUntilMs() {
    return this.data.getValidUntil();
  }

  /**
   * Gets whether the lock is expired.
   * @return A boolean representing expired.
   */
  public boolean isExpired() {
    return this.data.isExpired();
  }

  /**
   * Gets the owner of the lock.
   * @return A string for the owner of the lock.
   */
  public String getOwner() {
    return this.data.getOwner();
  }
}
