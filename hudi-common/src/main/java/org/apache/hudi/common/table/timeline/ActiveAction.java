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
import java.util.Comparator;
import java.util.List;

/**
 * A combination of instants covering action states: requested, inflight, completed.
 */
public class ActiveAction implements Serializable, Comparable<ActiveAction> {
  private final HoodieInstant requested;
  private final HoodieInstant inflight;
  private final List<HoodieInstant> completed;

  /**
   * The constructor.
   */
  protected ActiveAction(@Nullable HoodieInstant requested, @Nullable HoodieInstant inflight, List<HoodieInstant> completed) {
    this.requested = requested;
    this.inflight = inflight;
    this.completed = completed;
  }

  public static ActiveAction fromInstants(List<HoodieInstant> instants) {
    ValidationUtils.checkArgument(instants != null, "Instants should not be null");
    HoodieInstant requested = null;
    HoodieInstant inflight = null;
    // there could be multiple completed cleaning instants for one instant time,
    // currently we do not force explicit lock guard for cleaning.
    List<HoodieInstant> completed = new ArrayList<>();
    for (HoodieInstant instant : instants) {
      if (instant.isRequested()) {
        requested = instant;
      } else if (instant.isInflight()) {
        inflight = instant;
      } else {
        completed.add(instant);
      }
    }
    ValidationUtils.checkState(!completed.isEmpty(), "The instants to archive must be completed: " + instants);
    completed.sort(Comparator.comparing(HoodieInstant::getCompletionTime).reversed());
    return new ActiveAction(requested, inflight, completed);
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

  public List<HoodieInstant> getCompletedInstants() {
    return completed;
  }

  private HoodieInstant getCompleted() {
    return completed.get(0);
  }

  public String getAction() {
    return getCompleted().getAction();
  }

  /**
   * A COMPACTION action eventually becomes COMMIT when completed.
   */
  public String getPendingAction() {
    return getPendingInstant().map(HoodieInstant::getAction).orElse("null");
  }

  public String getInstantTime() {
    return getCompleted().requestedTime();
  }

  public String getCompletionTime() {
    return getCompleted().getCompletionTime();
  }

  public Option<byte[]> getCommitMetadata(HoodieTableMetaClient metaClient) {
    Option<byte[]> content = metaClient.getActiveTimeline().getInstantDetails(getCompleted());
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

  public Option<byte[]> getCleanPlan(HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> pendingInstant = getPendingInstant();
    if (!pendingInstant.isPresent()) {
      return Option.empty();
    }
    return metaClient.getActiveTimeline().readCleanerInfoAsBytes(pendingInstant.get());
  }

  public Option<byte[]> getCompactionPlan(HoodieTableMetaClient metaClient) {
    if (this.requested != null) {
      return metaClient.getActiveTimeline().readCompactionPlanAsBytes(this.requested);
    }
    return Option.empty();
  }

  public Option<byte[]> getLogCompactionPlan(HoodieTableMetaClient metaClient) {
    if (this.requested != null) {
      return metaClient.getActiveTimeline().readCompactionPlanAsBytes(this.requested);
    }
    return Option.empty();
  }

  protected Option<HoodieInstant> getPendingInstant() {
    if (requested != null) {
      return Option.of(requested);
    } else if (inflight != null) {
      return Option.of(inflight);
    } else {
      return Option.empty();
    }
  }

  @Override
  public int compareTo(ActiveAction other) {
    return this.getCompleted().requestedTime().compareTo(other.getCompleted().requestedTime());
  }

  @Override
  public String toString() {
    return getInstantTime() + "__" + getAction();
  }
}
