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

package org.apache.hudi.common.table;

import org.apache.hudi.common.config.EnumFieldDescription;

public enum PartialUpdateMode {
  @EnumFieldDescription(
      "No partial update logic should be employed.")
  NONE,

  @EnumFieldDescription(
      "For any column values missing in current record, pick value from previous version of the record.")
  KEEP_VALUES,

  @EnumFieldDescription(
      "For column values missing in current record, pick the default value from the schema.")
  FILL_DEFAULTS,

  @EnumFieldDescription(
      "For columns having default values set in current record, pick the value from previous version of the record."
      + "Only top level data type default is checked, which means this mode does not check leaf level data type default"
      + "value for nested data types.")
  IGNORE_DEFAULTS,

  @EnumFieldDescription(
      "For columns having marker in the current record, pick value from previous version of the record during write."
      + "Marker value can be defined using `hoodie.write.partial.update.custom.marker`, which should be added to"
      + "the value of table config `hoodie.write.partial.update.properties`. During upgrade, it could be"
      + "added automatically for some payload classes, e.g., `PostgresDebeziumAvroPayload`.")
  IGNORE_MARKERS
}
