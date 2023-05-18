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

package org.apache.hudi.client;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("Used to collect information about a write.")
public enum WriteStatusType {

  @EnumFieldDescription("[INTERNAL ONLY] Used for bootstrap.")
  BOOTSTRAP("org.apache.hudi.client.bootstrap.BootstrapWriteStatus"),

  @EnumFieldDescription("Used when we want to fail fast and at the first available exception/error.")
  FAIL_FIRST_ERROR("org.apache.hudi.client.FailOnFirstErrorWriteStatus"),

  @EnumFieldDescription("Status of a write operation.")
  DEFAULT("org.apache.hudi.client.WriteStatus"),

  @EnumFieldDescription("Uses the write status class set in `hoodie.writestatus.class`")
  CUSTOM("");
  public final String classPath;

  WriteStatusType(String classPath) {
    this.classPath = classPath;
  }
}
