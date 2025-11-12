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

package org.apache.hudi.client.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A combination of instants covering action states: requested, inflight, completed.
 *
 * <p>It holds all the instant details besides the instants.
 */
public class ActiveActionWithDetails extends ActiveAction {
  private final Option<byte[]> requestedDetails;
  private final Option<byte[]> inflightDetails;
  private final Option<byte[]> completedDetails;

  /**
   * The constructor.
   */
  protected ActiveActionWithDetails(
      @Nullable HoodieInstant requested,
      Option<byte[]> requestedDetails,
      @Nullable HoodieInstant inflight,
      Option<byte[]> inflightDetails,
      HoodieInstant completed,
      Option<byte[]> completedDetails) {
    super(requested, inflight, Collections.singletonList(completed));
    this.requestedDetails = requestedDetails;
    this.inflightDetails = inflightDetails;
    this.completedDetails = completedDetails;
  }

  public static ActiveActionWithDetails fromInstantAndDetails(List<Pair<HoodieInstant, Option<byte[]>>> instantAndDetails) {
    ValidationUtils.checkArgument(instantAndDetails.size() <= 3);

    HoodieInstant requested = null;
    HoodieInstant inflight = null;
    HoodieInstant completed = null;

    Option<byte[]> requestedDetails = Option.empty();
    Option<byte[]> inflightDetails = Option.empty();
    Option<byte[]> completedDetails = Option.empty();

    for (Pair<HoodieInstant, Option<byte[]>> instantAndDetail : instantAndDetails) {
      HoodieInstant instant = instantAndDetail.getKey();
      Option<byte[]> details = instantAndDetail.getRight();
      if (instant.isRequested()) {
        requested = instant;
        requestedDetails = details;
      } else if (instant.isInflight()) {
        inflight = instant;
        inflightDetails = details;
      } else {
        completed = instant;
        completedDetails = details;
      }
    }
    return new ActiveActionWithDetails(requested, requestedDetails, inflight, inflightDetails, Objects.requireNonNull(completed), completedDetails);
  }

  public Option<byte[]> getCommitMetadata(HoodieTableMetaClient metaClient) {
    return this.completedDetails;
  }

  public Option<byte[]> getRequestedCommitMetadata(HoodieTableMetaClient metaClient) {
    return this.requestedDetails;
  }

  public Option<byte[]> getInflightCommitMetadata(HoodieTableMetaClient metaClient) {
    return this.inflightDetails;
  }

  public Option<byte[]> getCleanPlan(HoodieTableMetaClient metaClient) {
    return this.requestedDetails;
  }

  public Option<byte[]> getCompactionPlan(HoodieTableMetaClient metaClient) {
    return this.requestedDetails;
  }

  public Option<byte[]> getLogCompactionPlan(HoodieTableMetaClient metaClient) {
    return this.requestedDetails;
  }
}
