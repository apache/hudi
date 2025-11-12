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

package org.apache.hudi.common.table.timeline.versioning.v2;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantReader;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.TimelineLayout;

import java.util.stream.Stream;

public class TimelineV2Factory extends TimelineFactory {

  private final TimelineLayout layout;

  public TimelineV2Factory(TimelineLayout layout) {
    this.layout = layout;
  }

  @Override
  public HoodieTimeline createDefaultTimeline(Stream<HoodieInstant> instants, HoodieInstantReader instantReader) {
    return new BaseTimelineV2(instants, instantReader);
  }

  @Override
  public HoodieActiveTimeline createActiveTimeline() {
    return new ActiveTimelineV2();
  }

  @Override
  public HoodieArchivedTimeline createArchivedTimeline(HoodieTableMetaClient metaClient) {
    return new ArchivedTimelineV2(metaClient);
  }

  @Override
  public HoodieArchivedTimeline createArchivedTimeline(HoodieTableMetaClient metaClient, String startTs) {
    return new ArchivedTimelineV2(metaClient, startTs);
  }

  @Override
  public ArchivedTimelineLoader createArchivedTimelineLoader() {
    return new ArchivedTimelineLoaderV2();
  }

  @Override
  public HoodieActiveTimeline createActiveTimeline(HoodieTableMetaClient metaClient) {
    return new ActiveTimelineV2(metaClient);
  }

  @Override
  public HoodieActiveTimeline createActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilter) {
    return new ActiveTimelineV2(metaClient, applyLayoutFilter);
  }

  @Override
  public CompletionTimeQueryView createCompletionTimeQueryView(HoodieTableMetaClient metaClient) {
    return new CompletionTimeQueryViewV2(metaClient);
  }
}
