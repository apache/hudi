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

package org.apache.hudi.common.model;

import java.util.Objects;

/**
 * Represents all kinds of changes that a row can describe in a changelog.
 */
public enum HoodieCdcOperation {
  /**
   * Insert operation.
   */
  INSERT("I", (byte)0),
  /**
   * Update operation with previous record content,
   * should be used together with {@link #UPDATE_AFTER} for modeling an update operation.
   */
  UPDATE_BEFORE("-U", (byte)1),
  /**
   * Update operation with new record content.
   */
  UPDATE_AFTER("U", (byte)2),
  /**
   * Delete operation.
   */
  DELETE("D", (byte)3);

  private final String name;

  private final byte value;

  HoodieCdcOperation(String name, byte value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public byte getValue() {
    return value;
  }

  public static HoodieCdcOperation fromValue(byte value) {
    switch (value) {
      case 0:
        return INSERT;
      case 1:
        return UPDATE_BEFORE;
      case 2:
        return UPDATE_AFTER;
      case 3:
        return DELETE;
      default:
        throw new AssertionError();
    }
  }

  /**
   * Returns whether the operation is an INSERT.
   */
  public static boolean isInsert(String name) {
    return Objects.equals(INSERT.getName(), name);
  }

  /**
   * Returns whether the operation is an UPDATE_BEFORE.
   */
  public static boolean isUpdateBefore(String name) {
    return Objects.equals(UPDATE_BEFORE.getName(), name);
  }

  /**
   * Returns whether the operation is an UPDATE_AFTER.
   */
  public static boolean isUpdateAfter(String name) {
    return Objects.equals(UPDATE_AFTER.getName(), name);
  }

  /**
   * Returns whether the operation is an UPDATE.
   * Both "U" and "-U" are recognized as UPDATE.
   */
  public static boolean isUpdate(String name) {
    return Objects.equals(UPDATE_AFTER.getName(), name);
  }

  /**
   * Returns whether the operation is a DELETE.
   */
  public static boolean isDelete(String name) {
    return Objects.equals(DELETE.getName(), name);
  }
}
