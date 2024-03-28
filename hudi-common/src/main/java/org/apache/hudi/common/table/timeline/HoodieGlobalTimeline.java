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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A global timeline view with both active and archived timeline involved.
 */
public class HoodieGlobalTimeline extends HoodieDefaultTimeline {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HoodieGlobalTimeline.class);
  private final HoodieTableMetaClient metaClient;
  private final HoodieActiveTimeline activeTimeline;
  private final HoodieArchivedTimeline archivedTimeline;

  protected HoodieGlobalTimeline(HoodieTableMetaClient metaClient, Option<String> startInstant) {
    this.metaClient = metaClient;
    this.activeTimeline = new HoodieActiveTimeline(metaClient);
    if (startInstant.isPresent()) {
      String firstActiveInstant = this.activeTimeline.firstInstant().map(HoodieInstant::getTimestamp).orElse(HoodieTimeline.INIT_INSTANT_TS);
      this.archivedTimeline = HoodieTimeline.compareTimestamps(startInstant.get(), LESSER_THAN, firstActiveInstant)
          ? new HoodieArchivedTimeline(metaClient, startInstant.get())
          : new HoodieArchivedTimeline(metaClient, firstActiveInstant);
    } else {
      // load the whole archived timeline
      this.archivedTimeline = new HoodieArchivedTimeline(metaClient);
    }
    this.details = FederatedDetails.create(this.activeTimeline, archivedTimeline);
    setInstants(mergeInstants(archivedTimeline.getInstants(), activeTimeline.getInstants()));
  }

  protected HoodieGlobalTimeline(HoodieActiveTimeline activeTimeline, HoodieArchivedTimeline archivedTimeline) {
    this.metaClient = activeTimeline.metaClient;
    this.activeTimeline = activeTimeline;
    this.archivedTimeline = archivedTimeline;
    this.details = FederatedDetails.create(this.activeTimeline, archivedTimeline);
    setInstants(mergeInstants(archivedTimeline.getInstants(), activeTimeline.getInstants()));
  }

  /**
   * For serialization and de-serialization only.
   */
  public HoodieGlobalTimeline() {
    this.activeTimeline = null;
    this.archivedTimeline = null;
    this.metaClient = null;
  }

  @Override
  public HoodieTimeline filterPendingCompactionTimeline() {
    // override for efficiency
    return this.activeTimeline.filterPendingCompactionTimeline();
  }

  @Override
  public HoodieTimeline filterPendingLogCompactionTimeline() {
    // override for efficiency
    return this.activeTimeline.filterPendingLogCompactionTimeline();
  }

  @Override
  public HoodieTimeline filterPendingMajorOrMinorCompactionTimeline() {
    // override for efficiency
    return this.activeTimeline.filterPendingMajorOrMinorCompactionTimeline();
  }

  @Override
  public HoodieTimeline filterPendingReplaceTimeline() {
    // override for efficiency
    return this.activeTimeline.filterPendingReplaceTimeline();
  }

  @Override
  public HoodieTimeline filterPendingRollbackTimeline() {
    // override for efficiency
    return this.activeTimeline.filterPendingRollbackTimeline();
  }

  @Override
  public HoodieTimeline filterRequestedRollbackTimeline() {
    // override for efficiency
    return this.activeTimeline.filterRequestedRollbackTimeline();
  }

  @Override
  public HoodieTimeline filterPendingIndexTimeline() {
    // override for efficiency
    return this.activeTimeline.filterPendingIndexTimeline();
  }

  @Override
  public boolean empty() {
    return this.activeTimeline.empty();
  }

  /**
   * Returns whether the active timeline contains the given instant or the instant is archived.
   */
  @Override
  public boolean isValidInstant(String ts) {
    return this.activeTimeline.isValidInstant(ts);
  }

  @Override
  public boolean isArchived(String ts) {
    return this.activeTimeline.isArchived(ts);
  }

  @Override
  public Option<HoodieInstant> getFirstNonSavepointActiveCommit() {
    return this.activeTimeline.getFirstNonSavepointActiveCommit();
  }

  @Override
  public Option<HoodieInstant> getLastPendingClusterInstant() {
    // override for efficiency
    return this.activeTimeline.getLastPendingClusterInstant();
  }

  /**
   * Needs to fix this method to only check on active timeline.
   */
  @Override
  protected Option<HoodieInstant> findFirstNonSavepointCommit(List<HoodieInstant> instants) {
    return this.activeTimeline.findFirstNonSavepointCommit(instants);
  }

  /**
   * Reloads all the instants.
   */
  public HoodieGlobalTimeline reload() {
    return new HoodieGlobalTimeline(this.metaClient, Option.empty());
  }

  /**
   * The active timeline is always reloaded,
   * the archived timeline is reloaded based on the given start timestamp {@code startTs}.
   */
  public HoodieGlobalTimeline reload(String startTs) {
    HoodieActiveTimeline reloadedActiveTimeline = this.metaClient.reloadActiveTimeline();
    HoodieInstant oldFirstActiveCommit = this.activeTimeline.firstInstant().orElse(null);
    HoodieInstant newFirstActiveCommit = reloadedActiveTimeline.firstInstant().orElse(null);
    // reload the archived timeline incrementally if the archiving snapshot does not change.
    HoodieArchivedTimeline reloadedArchivedTimeline = Objects.equals(oldFirstActiveCommit, newFirstActiveCommit)
        ? this.archivedTimeline.reload(startTs)
        : new HoodieArchivedTimeline(metaClient, startTs);
    return new HoodieGlobalTimeline(reloadedActiveTimeline, reloadedArchivedTimeline);
  }

  /**
   * A details function federated with both active and archived timeline details.
   */
  private static class FederatedDetails implements Function<HoodieInstant, Option<byte[]>> {
    private final Function<HoodieInstant, Option<byte[]>> archivedDetails;
    private final Function<HoodieInstant, Option<byte[]>> activeDetails;
    private final Function<HoodieInstant, Boolean> archivingChecker;

    private FederatedDetails(
        @Nullable Function<HoodieInstant, Boolean> archivingChecker,
        @Nullable Function<HoodieInstant, Option<byte[]>> archivedDetails,
        Function<HoodieInstant, Option<byte[]>> activeDetails) {
      ValidationUtils.checkArgument(archivingChecker == null && archivedDetails == null || archivingChecker != null && archivedDetails != null,
          "Archiving check and details must be both either null or non-null");
      this.archivedDetails = archivedDetails;
      this.activeDetails = activeDetails;
      this.archivingChecker = archivingChecker;
    }

    /**
     * Creates a details function.
     */
    public static Function<HoodieInstant, Option<byte[]>> create(HoodieActiveTimeline activeTimeline, HoodieArchivedTimeline archivedTimeline) {
      return new FederatedDetails(instant -> activeTimeline.isArchived(instant.getTimestamp()), archivedTimeline.details, activeTimeline.details);
    }

    @Override
    public Option<byte[]> apply(HoodieInstant instant) {
      return archivingChecker != null && archivingChecker.apply(instant)
          ? this.archivedDetails.apply(instant) : this.activeDetails.apply(instant);
    }
  }
}
