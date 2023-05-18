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

package org.apache.hudi.client.bootstrap.selector;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped.")
public enum BootstrapModeSelectorType {

  @EnumFieldDescription("Uses `hoodie.bootstrap.mode.selector.regex.mode` and `hoodie.bootstrap.mode.selector.regex` to select bootstrap mode.")
  REGEX("org.apache.hudi.client.bootstrap.selector.BootstrapRegexModeSelector"),

  @EnumFieldDescription("All records are bootstrapped with the full bootstrap mode.")
  FULL_RECORD("org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector"),

  @EnumFieldDescription("All records are bootstrapped with the metadata only mode.")
  METADATA_ONLY("org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector"),

  @EnumFieldDescription("Use the selector class set in `hoodie.bootstrap.mode.selector`")
  CUSTOM("");

  public final String classPath;
  BootstrapModeSelectorType(String classPath) {
    this.classPath = classPath;
  }
}
