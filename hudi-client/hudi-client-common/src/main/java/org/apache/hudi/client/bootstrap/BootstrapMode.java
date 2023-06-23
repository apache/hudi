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

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

/**
 * Identifies different types of bootstrap.
 */
@EnumDescription("Bootstrap mode for importing an existing table into Hudi")
public enum BootstrapMode {
  /**
   * In this mode, record level metadata is generated for each source record and both original record and metadata
   * for each record copied.
   */
  @EnumFieldDescription("In this mode, the full record data is copied into hudi and metadata columns are added. A full"
    + " record bootstrap is functionally equivalent to a bulk-insert. After a full record bootstrap, Hudi will function"
    + " properly even if the original table is modified or deleted.")
  FULL_RECORD,

  /**
   * In this mode, record level metadata alone is generated for each source record and stored in new bootstrap location.
   */
  @EnumFieldDescription("In this mode, the full record data is not copied into Hudi therefore it avoids full cost of rewriting the dataset. "
      + "Instead, 'skeleton' files containing just the corresponding metadata columns are added to the Hudi table. Hudi relies on the data "
      + "in the original table and will face data-loss or corruption if files in the original table location are deleted or modified.")
  METADATA_ONLY
}
