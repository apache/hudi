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

package org.apache.hudi.tableformat;

import org.apache.hudi.common.NativeTableFormat;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;

/**
 * A non-native table format that keeps the native timeline and metadata table under another name, so that a test
 * of the behavior of non-native tables can use a record level index the metadata table serves.
 */
public class NativeBackedTestTableFormat extends NativeTableFormat {

  public static final String NAME = "native-backed-test-format";

  public NativeBackedTestTableFormat() {
    super(TimelineLayoutVersion.CURR_LAYOUT_VERSION);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
