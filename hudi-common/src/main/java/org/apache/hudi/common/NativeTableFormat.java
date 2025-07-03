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

package org.apache.hudi.common;

import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.metadata.NativeTableMetadataFactory;
import org.apache.hudi.metadata.TableMetadataFactory;

public class NativeTableFormat implements HoodieTableFormat {
  public static final String TABLE_FORMAT = "native";
  private final TimelineLayoutVersion timelineLayoutVersion;

  public NativeTableFormat(TimelineLayoutVersion timelineLayoutVersion) {
    this.timelineLayoutVersion = timelineLayoutVersion;
  }

  @Override
  public String getName() {
    return NativeTableFormat.TABLE_FORMAT;
  }

  public TimelineFactory getTimelineFactory() {
    return TimelineLayout.fromVersion(timelineLayoutVersion).getTimelineFactory();
  }

  @Override
  public TableMetadataFactory getMetadataFactory() {
    return NativeTableMetadataFactory.getInstance();
  }
}
