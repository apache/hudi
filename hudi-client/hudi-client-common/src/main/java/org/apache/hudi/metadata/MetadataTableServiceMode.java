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

package org.apache.hudi.metadata;

import java.util.Locale;

/** Controls which phases of metadata table services are run. */
public enum MetadataTableServiceMode {
  SCHEDULE,
  EXECUTE,
  SCHEDULE_AND_EXECUTE;

  public boolean includesSchedule() {
    return this == SCHEDULE || this == SCHEDULE_AND_EXECUTE;
  }

  public boolean includesExecute() {
    return this == EXECUTE || this == SCHEDULE_AND_EXECUTE;
  }

  public static MetadataTableServiceMode fromValue(String value) {
    return valueOf(value.trim().replace('-', '_').toUpperCase(Locale.ROOT));
  }
}
