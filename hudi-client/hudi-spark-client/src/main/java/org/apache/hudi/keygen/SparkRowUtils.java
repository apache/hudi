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

package org.apache.hudi.keygen;

import org.apache.hudi.exception.HoodieKeyException;

import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.hudi.keygen.KeyGenUtils.EMPTY_RECORDKEY_PLACEHOLDER;
import static org.apache.hudi.keygen.KeyGenUtils.NULL_RECORDKEY_PLACEHOLDER;

public class SparkRowUtils {

  protected static final UTF8String NULL_RECORD_KEY_PLACEHOLDER_UTF8 = UTF8String.fromString(NULL_RECORDKEY_PLACEHOLDER);
  protected static final UTF8String EMPTY_RECORD_KEY_PLACEHOLDER_UTF8 = UTF8String.fromString(EMPTY_RECORDKEY_PLACEHOLDER);

  protected static String requireNonNullNonEmptyKey(String key) {
    if (key != null && key.length() > 0) {
      return key;
    } else {
      throw new HoodieKeyException("Record key has to be non-empty string!");
    }
  }

  protected static UTF8String requireNonNullNonEmptyKey(UTF8String key) {
    if (key != null && key.numChars() > 0) {
      return key;
    } else {
      throw new HoodieKeyException("Record key has to be non-empty string!");
    }
  }

  protected static <S> S handleNullRecordKey(S s) {
    if (s == null || s.toString().isEmpty()) {
      throw new HoodieKeyException("Record key has to be non-null!");
    }

    return s;
  }

  static UTF8String toUTF8String(Object o) {
    if (o == null) {
      return null;
    } else if (o instanceof UTF8String) {
      return (UTF8String) o;
    } else {
      // NOTE: If object is a [[String]], [[toString]] would be a no-op
      return UTF8String.fromString(o.toString());
    }
  }

  static String toString(Object o) {
    return o == null ? null : o.toString();
  }

  static String handleNullOrEmptyCompositeKeyPart(Object keyPart) {
    if (keyPart == null) {
      return NULL_RECORDKEY_PLACEHOLDER;
    } else {
      // NOTE: [[toString]] is a no-op if key-part was already a [[String]]
      String keyPartStr = keyPart.toString();
      return !keyPartStr.isEmpty() ? keyPartStr : EMPTY_RECORDKEY_PLACEHOLDER;
    }
  }

  static UTF8String handleNullOrEmptyCompositeKeyPartUTF8(UTF8String keyPart) {
    if (keyPart == null) {
      return NULL_RECORD_KEY_PLACEHOLDER_UTF8;
    } else if (keyPart.numChars() == 0) {
      return EMPTY_RECORD_KEY_PLACEHOLDER_UTF8;
    }

    return keyPart;
  }

  @SuppressWarnings("StringEquality")
  static boolean isNullOrEmptyCompositeKeyPart(String keyPart) {
    // NOTE: Converted key-part is compared against null/empty stub using ref-equality
    //       for performance reasons (it relies on the fact that we're using internalized
    //       constants)
    return keyPart == NULL_RECORDKEY_PLACEHOLDER || keyPart == EMPTY_RECORDKEY_PLACEHOLDER;
  }

  static boolean isNullOrEmptyCompositeKeyPartUTF8(UTF8String keyPart) {
    // NOTE: Converted key-part is compared against null/empty stub using ref-equality
    //       for performance reasons (it relies on the fact that we're using internalized
    //       constants)
    return keyPart == NULL_RECORD_KEY_PLACEHOLDER_UTF8 || keyPart == EMPTY_RECORD_KEY_PLACEHOLDER_UTF8;
  }
}
