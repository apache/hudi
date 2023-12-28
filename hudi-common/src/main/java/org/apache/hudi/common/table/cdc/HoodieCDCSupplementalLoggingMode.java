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

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Change log capture supplemental logging mode. The supplemental log is used for
 * accelerating the generation of change log details.
 *
 * <p>Three modes are supported:</p>
 *
 * <ul>
 *   <li>op_key_only: record keys, the reader needs to figure out the update before image and after image;</li>
 *   <li>data_before: before images, the reader needs to figure out the update after images;</li>
 *   <li>data_before_after: before and after images, the reader can generate the details directly from the log.</li>
 * </ul>
 */
@EnumDescription("Change log capture supplemental logging mode. The supplemental log is used for "
    + "accelerating the generation of change log details.")
public enum HoodieCDCSupplementalLoggingMode {
  @EnumFieldDescription("Only keeping record keys in the supplemental logs, so the reader needs "
      + "to figure out the update before image and after image.")
  OP_KEY_ONLY,

  @EnumFieldDescription("Keeping the before images in the supplemental logs, so the reader needs "
      + "to figure out the update after images.")
  DATA_BEFORE,

  @EnumFieldDescription("Keeping the before and after images in the supplemental logs, so the "
      + "reader can generate the details directly from the logs.")
  DATA_BEFORE_AFTER
}
