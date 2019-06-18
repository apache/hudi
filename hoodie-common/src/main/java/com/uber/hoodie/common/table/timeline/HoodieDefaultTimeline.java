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

package com.uber.hoodie.common.table.timeline;

import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.StringUtils;
import com.uber.hoodie.exception.HoodieException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * HoodieDefaultTimeline is a default implementation of the HoodieTimeline. It provides methods to
 * inspect a List[HoodieInstant]. Function to get the details of the instant is passed in as a
 * lamdba.
 *
 * @see HoodieTimeline
 */
public class HoodieDefaultTimeline implements HoodieTimeline {

  private static final transient Logger log = LogManager.getLogger(HoodieDefaultTimeline.class);

  private static final String HASHING_ALGORITHM = "SHA-256";

  protected transient Function<HoodieInstant, Optional<byte[]>> details;
  private List<HoodieInstant> instants;
  private String timelineHash;

  public HoodieDefaultTimeline(Stream<HoodieInstant> instants,
      Function<HoodieInstant, Optional<byte[]>> details) {
    this.details = details;
    setInstants(instants.collect(Collectors.toList()));
  }

  public void setInstants(List<HoodieInstant> instants) {
    this.instants = instants;
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance(HASHING_ALGORITHM);
      this.instants.stream().forEach(i -> md.update(
          StringUtils.joinUsingDelim("_", i.getTimestamp(), i.getAction(), i.getState().name()).getBytes()));
    } catch (NoSuchAlgorithmException nse) {
      throw new HoodieException(nse);
    }
    this.timelineHash = new String(Hex.encodeHex(md.digest()));
  }

  /**
   * For serailizing and de-serializing
   *
   * @deprecated
   */
  public HoodieDefaultTimeline() {
  }

  @Override
  public HoodieTimeline filterInflights() {
    return new HoodieDefaultTimeline(instants.stream().filter(HoodieInstant::isInflight),
        details);
  }

  @Override
  public HoodieTimeline filterInflightsExcludingCompaction() {
    return new HoodieDefaultTimeline(instants.stream().filter(instant -> {
      return instant.isInflight() && (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION));
    }), details);
  }

  @Override
  public HoodieTimeline filterCompletedInstants() {
    return new HoodieDefaultTimeline(instants.stream().filter(s -> !s.isInflight()), details);
  }

  @Override
  public HoodieTimeline filterCompletedAndCompactionInstants() {
    return new HoodieDefaultTimeline(instants.stream().filter(s -> {
      return !s.isInflight() || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION);
    }), details);
  }

  @Override
  public HoodieTimeline filterPendingCompactionTimeline() {
    return new HoodieDefaultTimeline(
        instants.stream().filter(s -> s.getAction().equals(HoodieTimeline.COMPACTION_ACTION)),
        details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsInRange(String startTs, String endTs) {
    return new HoodieDefaultTimeline(instants.stream().filter(
        s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), startTs, GREATER)
            && HoodieTimeline.compareTimestamps(
            s.getTimestamp(), endTs, LESSER_OR_EQUAL)), details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsAfter(String commitTime, int numCommits) {
    return new HoodieDefaultTimeline(
        instants.stream()
            .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), commitTime, GREATER))
            .limit(numCommits), details);
  }

  @Override
  public HoodieTimeline filter(Predicate<HoodieInstant> filter) {
    return new HoodieDefaultTimeline(instants.stream().filter(filter), details);
  }

  @Override
  public boolean empty() {
    return !instants.stream().findFirst().isPresent();
  }

  @Override
  public int countInstants() {
    return new Long(instants.stream().count()).intValue();
  }

  @Override
  public Optional<HoodieInstant> firstInstant() {
    return instants.stream().findFirst();
  }

  @Override
  public Optional<HoodieInstant> nthInstant(int n) {
    if (empty() || n >= countInstants()) {
      return Optional.empty();
    }
    return Optional.of(instants.get(n));
  }

  @Override
  public Optional<HoodieInstant> lastInstant() {
    return empty() ? Optional.empty() : nthInstant(countInstants() - 1);
  }

  @Override
  public Optional<HoodieInstant> nthFromLastInstant(int n) {
    if (countInstants() < n + 1) {
      return Optional.empty();
    }
    return nthInstant(countInstants() - 1 - n);
  }

  @Override
  public boolean containsInstant(HoodieInstant instant) {
    return instants.stream().anyMatch(s -> s.equals(instant));
  }

  @Override
  public boolean containsOrBeforeTimelineStarts(String instant) {
    return instants.stream().anyMatch(s -> s.getTimestamp().equals(instant))
        || isBeforeTimelineStarts(instant);
  }

  @Override
  public String getTimelineHash() {
    return timelineHash;
  }

  @Override
  public Stream<HoodieInstant> getInstants() {
    return instants.stream();
  }

  @Override
  public boolean isBeforeTimelineStarts(String instant) {
    Optional<HoodieInstant> firstCommit = firstInstant();
    return firstCommit.isPresent()
        && HoodieTimeline.compareTimestamps(instant, firstCommit.get().getTimestamp(), LESSER);
  }

  @Override
  public Optional<byte[]> getInstantDetails(HoodieInstant instant) {
    return details.apply(instant);
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ": " + instants.stream().map(Object::toString)
        .collect(Collectors.joining(","));
  }

}
