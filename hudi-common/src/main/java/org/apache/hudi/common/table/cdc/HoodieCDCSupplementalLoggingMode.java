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

package org.apache.hudi.common.table.cdc;

import org.apache.hudi.exception.HoodieNotSupportedException;

/**
 * Change log capture supplemental logging mode. The supplemental log is used for
 * accelerating the generation of change log details.
 *
 * <p>Three modes are supported:</p>
 *
 * <ul>
 *   <li>OP_KEY: record keys, the reader needs to figure out the update before image and after image;</li>
 *   <li>WITH_BEFORE: before images, the reader needs to figure out the update after images;</li>
 *   <li>WITH_BEFORE_AFTER: before and after images, the reader can generate the details directly from the log.</li>
 * </ul>
 */
public enum HoodieCDCSupplementalLoggingMode {
  OP_KEY("cdc_op_key"),
  WITH_BEFORE("cdc_data_before"),
  WITH_BEFORE_AFTER("cdc_data_before_after");

  private final String value;

  HoodieCDCSupplementalLoggingMode(String value) {
    this.value = value;
  }

  public String getValue() {
    return this.value;
  }

  public static HoodieCDCSupplementalLoggingMode parse(String value) {
    switch (value) {
      case "cdc_op_key":
        return OP_KEY;
      case "cdc_data_before":
        return WITH_BEFORE;
      case "cdc_data_before_after":
        return WITH_BEFORE_AFTER;
      default:
        throw new HoodieNotSupportedException("Unsupported value: " + value);
    }
  }
}
