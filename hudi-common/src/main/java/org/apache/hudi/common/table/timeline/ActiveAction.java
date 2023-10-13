/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A combination of instants covering action states: requested, inflight, completed.
 */
public class ActiveAction implements Serializable, Comparable<ActiveAction> {
  private final HoodieInstant requested;
  private final HoodieInstant inflight;
  private final HoodieInstant completed;

  /**
   * The constructor.
   */
  protected ActiveAction(@Nullable HoodieInstant requested, @Nullable HoodieInstant inflight, HoodieInstant completed) {
    this.requested = requested;
    this.inflight = inflight;
    this.completed = completed;
  }

  public static ActiveAction fromInstants(List<HoodieInstant> instants) {
    ValidationUtils.checkArgument(instants.size() <= 3);
    HoodieInstant requested = null;
    HoodieInstant inflight = null;
    HoodieInstant completed = null;
    for (HoodieInstant instant : instants) {
      if (instant.isRequested()) {
        requested = instant;
      } else if (instant.isInflight()) {
        inflight = instant;
      } else {
        completed = instant;
      }
    }
    return new ActiveAction(requested, inflight, Objects.requireNonNull(completed));
  }

  public List<HoodieInstant> getPendingInstants() {
    List<HoodieInstant> instants = new ArrayList<>(2);
    if (this.requested != null) {
      instants.add(this.requested);
    }
    if (this.inflight != null) {
      instants.add(this.inflight);
    }
    return instants;
  }

  public HoodieInstant getCompleted() {
    return completed;
  }

  public String getAction() {
    return this.completed.getAction();
  }

  /**
   * A COMPACTION action eventually becomes COMMIT when completed.
   */
  public String getPendingAction() {
    return getPendingInstant().getAction();
  }

  public String getInstantTime() {
    return this.completed.getTimestamp();
  }

  public String getCompletionTime() {
    return this.completed.getCompletionTime();
  }

  public Option<byte[]> getCommitMetadata(HoodieTableMetaClient metaClient) {
    Option<byte[]> content = metaClient.getActiveTimeline().getInstantDetails(this.completed);
    if (content.isPresent() && content.get().length == 0) {
      return Option.empty();
    }
    return content;
  }

  public Option<byte[]> getRequestedCommitMetadata(HoodieTableMetaClient metaClient) {
    if (this.requested != null) {
      Option<byte[]> requestedContent = metaClient.getActiveTimeline().getInstantDetails(this.requested);
      if (!requestedContent.isPresent() || requestedContent.get().length == 0) {
        return Option.empty();
      } else {
        return requestedContent;
      }
    } else {
      return Option.empty();
    }
  }

  public Option<byte[]> getInflightCommitMetadata(HoodieTableMetaClient metaClient) {
    if (this.inflight != null) {
      Option<byte[]> inflightContent = metaClient.getActiveTimeline().getInstantDetails(this.inflight);
      if (!inflightContent.isPresent() || inflightContent.get().length == 0) {
        return Option.empty();
      } else {
        return inflightContent;
      }
    } else {
      return Option.empty();
    }
  }

  public byte[] getCleanPlan(HoodieTableMetaClient metaClient) {
    return metaClient.getActiveTimeline().readCleanerInfoAsBytes(getPendingInstant()).get();
  }

  public byte[] getCompactionPlan(HoodieTableMetaClient metaClient) {
    return metaClient.getActiveTimeline().readCompactionPlanAsBytes(HoodieTimeline.getCompactionRequestedInstant(getInstantTime())).get();
  }

  public byte[] getLogCompactionPlan(HoodieTableMetaClient metaClient) {
    return metaClient.getActiveTimeline().readCompactionPlanAsBytes(HoodieTimeline.getLogCompactionRequestedInstant(getInstantTime())).get();
  }

  protected HoodieInstant getPendingInstant() {
    if (requested != null) {
      return requested;
    } else if (inflight != null) {
      return inflight;
    } else {
      throw new AssertionError("Pending instant does not exist.");
    }
  }

  @Override
  public int compareTo(ActiveAction other) {
    return this.completed.getTimestamp().compareTo(other.completed.getTimestamp());
  }

  @Override
  public String toString() {
    return getCompleted().getTimestamp() + "__" + getCompleted().getAction();
  }
}
