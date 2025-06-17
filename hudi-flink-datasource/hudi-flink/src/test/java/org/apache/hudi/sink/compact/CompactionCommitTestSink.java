/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.timeline.HoodieTimelineArchiver;
import org.apache.hudi.client.timeline.versioning.v2.TimelineArchiverV2;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 * CompactionCommitTestSink, throw for first attempt to simulate failure
 */
public class CompactionCommitTestSink extends CompactionCommitSink {
  public CompactionCommitTestSink(Configuration conf) {
    super(conf);
  }

  @Override
  public void invoke(CompactionCommitEvent event, Context context) throws Exception {
    super.invoke(event, context);
    List<HoodieInstant> instants = writeClient.getHoodieTable().getMetaClient().getActiveTimeline().getInstants();
    boolean compactCommitted = instants.stream().anyMatch(i -> i.requestedTime().equals(event.getInstant()) && i.isCompleted());
    if (compactCommitted && RuntimeContextUtils.getAttemptNumber(getRuntimeContext()) == 0) {
      // archive compact instant
      this.writeClient.getConfig().setValue(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP, "1");
      this.writeClient.getConfig().setValue(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP, "1");
      HoodieTimelineArchiver archiver = new TimelineArchiverV2(this.writeClient.getConfig(), this.writeClient.getHoodieTable());
      this.writeClient.getHoodieTable().getMetaClient().reloadActiveTimeline();
      archiver.archiveIfRequired(HoodieFlinkEngineContext.DEFAULT);
      throw new HoodieException("Fail first attempt to simulate failover in test.");
    }
  }
}
