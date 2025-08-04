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

package org.apache.hudi.sink.cluster;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.clustering.ClusteringCommitEvent;
import org.apache.hudi.sink.clustering.ClusteringCommitSink;
import org.apache.hudi.utils.RuntimeContextUtils;

import org.apache.flink.configuration.Configuration;

import java.util.List;

/**
 *
 */
public class ClusteringCommitTestSink extends ClusteringCommitSink {
  public ClusteringCommitTestSink(Configuration conf) {
    super(conf);
  }

  @Override
  public void invoke(ClusteringCommitEvent event, Context context) throws Exception {
    super.invoke(event, context);
    List<HoodieInstant> instants = writeClient.getHoodieTable().getMetaClient().getActiveTimeline().getInstants();
    boolean committed = instants.stream().anyMatch(i -> i.requestedTime().equals(event.getInstant()) && i.isCompleted());
    if (committed && RuntimeContextUtils.getAttemptNumber(getRuntimeContext()) == 0) {
      throw new HoodieException("Fail first attempt to simulate failover in test.");
    }
  }
}
