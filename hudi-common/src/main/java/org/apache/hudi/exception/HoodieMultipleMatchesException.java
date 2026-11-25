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

package org.apache.hudi.exception;

/**
 * Exception thrown when more than one incoming record of an upsert matches the same existing record.
 */
public class HoodieMultipleMatchesException extends HoodieException {

  private final String recordKey;

  public HoodieMultipleMatchesException(String recordKey, String msg) {
    super(msg);
    this.recordKey = recordKey;
  }

  public HoodieMultipleMatchesException(String recordKey, String msg, Throwable cause) {
    super(msg, cause);
    this.recordKey = recordKey;
  }

  /**
   * The record key of the existing record that more than one incoming record matches.
   */
  public String getRecordKey() {
    return recordKey;
  }
}
