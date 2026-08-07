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
      "For columns having default values set in current record, pick the value from previous version of the record."
      + "Only top level data type default is checked, which means this mode does not check leaf level data type default"
      + "value for nested data types.")
  IGNORE_DEFAULTS,

  @EnumFieldDescription(
      "For columns having unavailable values in the current record, pick value from previous version of the record during write. "
         + "Unavailable value can be defined using `hoodie.write.partial.update.unavailable.value` in the table property.")
  FILL_UNAVAILABLE,

  @EnumFieldDescription(
      "For change-data-capture sources that emit only the columns that actually changed in an update event (e.g. Oracle "
      + "Debezium under primary-key-only supplemental logging), pick the incoming value only for the columns listed in the "
      + "record's changed-columns field and preserve the previous version's value for every other data column. The name of "
      + "the changed-columns field is defined using `hoodie.write.partial.update.changed.fields` and the CDC metadata columns "
      + "that must always be taken from the newer record are defined using `hoodie.write.partial.update.retain.fields` in the "
      + "table property.")
  // Contract and constraints:
  //  - Records are FULL-SCHEMA, value-level partial: every data column is present, and columns not in the
  //    changed-columns list carry a placeholder (null/zero) that must NOT win. This is distinct from a
  //    schema-partial record (fewer columns), which is the IGNORE_DEFAULTS/schema-partial (IS_PARTIAL) path.
  //  - Because the merge iterates the incoming record's schema, all columns exist in the merged output only
  //    if the incoming record carries the full table schema. The Oracle Debezium transformer guarantees this
  //    (it rebuilds the row with every column + the changed-columns list); do not feed genuinely schema-partial
  //    records to a FILL_UNCHANGED table.
  //  - INCOMPATIBLE with partial-update writes (`hoodie.write.partial.update.schema`, e.g. Spark MERGE INTO with a
  //    partial UPDATE SET): a schema-partial IS_PARTIAL log block flips the reader to the KEEP_VALUES merger and
  //    silently drops this changed-columns logic for the whole file group, letting placeholders win. This
  //    combination is rejected at write time in HoodieAppendHandle. Write FILL_UNCHANGED tables full-schema only.
  FILL_UNCHANGED
}
