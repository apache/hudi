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

package org.apache.hudi.common.config;

import org.apache.hudi.common.util.StringUtils;

@EnumDescription("Determines the logic of merging updates")
public enum RecordMergeMode {
  @EnumFieldDescription("Using transaction time to merge records, i.e., the record from later "
      + "transaction overwrites the earlier record with the same key.")
  COMMIT_TIME_ORDERING,

  @EnumFieldDescription("Using event time as the ordering to merge records, i.e., the record "
      + "with the larger event time overwrites the record with the smaller event time on the "
      + "same key, regardless of transaction time. The event time or preCombine field needs "
      + "to be specified by the user.")
  EVENT_TIME_ORDERING,

  @EnumFieldDescription("Using custom merging logic specified by the user.")
  CUSTOM;

  public static RecordMergeMode getValue(String mergeMode) {
    if (StringUtils.isNullOrEmpty(mergeMode)) {
      return null;
    }
    return RecordMergeMode.valueOf(mergeMode.toUpperCase());
  }
}
