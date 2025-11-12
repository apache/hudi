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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.io.Serializable;
import java.util.stream.Stream;

public abstract class TimelineFactory implements Serializable {

  public abstract HoodieTimeline createDefaultTimeline(Stream<HoodieInstant> instants, HoodieInstantReader instantReader);

  public abstract HoodieActiveTimeline createActiveTimeline();

  public abstract HoodieArchivedTimeline createArchivedTimeline(HoodieTableMetaClient metaClient);

  public abstract HoodieArchivedTimeline createArchivedTimeline(HoodieTableMetaClient metaClient, String startTs);

  public abstract ArchivedTimelineLoader createArchivedTimelineLoader();

  public abstract HoodieActiveTimeline createActiveTimeline(HoodieTableMetaClient metaClient);

  public abstract HoodieActiveTimeline createActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilter);

  public abstract CompletionTimeQueryView createCompletionTimeQueryView(HoodieTableMetaClient metaClient);
}
