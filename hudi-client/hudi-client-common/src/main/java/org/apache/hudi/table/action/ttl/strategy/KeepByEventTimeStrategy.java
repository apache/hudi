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

package org.apache.hudi.table.action.ttl.strategy;

import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.table.HoodieTable;

import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Event-time based partition TTL strategy: lifetime is read from the partition path, not from
 * commit metadata. Late-arriving writes into an old partition do not extend its lifetime, and
 * backfilled historic partitions are still considered old. Compare with {@link KeepByTimeStrategy}
 * (last commit time) and {@link KeepByCreationTimeStrategy} (creation commit time).
 *
 * <h3>Supported path shapes</h3>
 * The first-class, tested shapes are day and hour granularity. Each example shows the partition
 * path on the left and the required {@code timeSegStartIndex} on the right (see
 * <i>Locating the time block</i> below); non-time segments may appear before and/or after the
 * time block.
 * <ul>
 *   <li>Day, {@code format=yyyy-MM-dd}
 *     <ul>
 *       <li>time only: {@code 2026-06-27}, {@code dt=2026-06-27} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time: {@code region=us/2026-06-27}, {@code region=us/dt=2026-06-27} — timeSegStartIndex {@code 1}</li>
 *       <li>time + suffix: {@code 2026-06-27/source=app}, {@code dt=2026-06-27/source=app} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time + suffix: {@code region=us/dt=2026-06-27/source=app} — timeSegStartIndex {@code 1}</li>
 *     </ul>
 *   </li>
 *   <li>Day, {@code format=yyyyMMdd}
 *     <ul>
 *       <li>time only: {@code 20260627}, {@code dt=20260627} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time: {@code region=us/20260627}, {@code region=us/dt=20260627} — timeSegStartIndex {@code 1}</li>
 *       <li>time + suffix: {@code 20260627/source=app}, {@code dt=20260627/source=app} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time + suffix: {@code region=us/dt=20260627/source=app} — timeSegStartIndex {@code 1}</li>
 *     </ul>
 *   </li>
 *   <li>Hour, {@code format=yyyy-MM-dd/HH}
 *     <ul>
 *       <li>time only: {@code 2026-06-27/12}, {@code dt=2026-06-27/hh=12} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time: {@code region=us/2026-06-27/12}, {@code region=us/dt=2026-06-27/hh=12} — timeSegStartIndex {@code 1}</li>
 *       <li>time + suffix: {@code 2026-06-27/12/source=app}, {@code dt=2026-06-27/hh=12/source=app} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time + suffix: {@code region=us/dt=2026-06-27/hh=12/source=app} — timeSegStartIndex {@code 1}</li>
 *     </ul>
 *   </li>
 *   <li>Hour, {@code format=yyyyMMdd/HH}
 *     <ul>
 *       <li>time only: {@code 20260627/12}, {@code dt=20260627/hh=12} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time: {@code region=us/20260627/12}, {@code region=us/dt=20260627/hh=12} — timeSegStartIndex {@code 1}</li>
 *       <li>time + suffix: {@code 20260627/12/source=app}, {@code dt=20260627/hh=12/source=app} — timeSegStartIndex {@code 0}</li>
 *       <li>prefix + time + suffix: {@code region=us/dt=20260627/hh=12/source=app} — timeSegStartIndex {@code 1}</li>
 *     </ul>
 *   </li>
 * </ul>
 * Hive-style key names are not constrained: {@code dt=}, {@code day=}, {@code event_date=},
 * {@code hh=}, {@code hour=} all work; only the value after {@code =} is parsed.
 * <p>
 * Any {@link java.time.format.DateTimeFormatter} pattern works as long as the resulting
 * {@link java.time.temporal.TemporalAccessor} can be resolved to either an {@link java.time.Instant}
 * or a {@link java.time.LocalDate} (day-only patterns are anchored at UTC start-of-day). A
 * {@code /} in the pattern means the time value spans that many consecutive path segments.
 * Patterns missing day-of-month, e.g. month-only {@code yyyy-MM}, cannot be resolved and will
 * raise the standard parse-failure error at runtime.
 *
 * <h3>Locating the time block</h3>
 * The time block must occupy a <i>contiguous</i> segment range of the partition path. Only the
 * segments before it count toward {@code timeSegStartIndex}; segments after are ignored. Interleaved
 * layouts such as {@code dt=20260627/source=app/hh=12} are not supported -- the time block must
 * be in one piece.
 *
 * <h3>Time zone</h3>
 * Both the partition's event time and the cutoff derived from {@code instantTime} are interpreted
 * in UTC. Set {@code hoodie.table.timeline.timezone=UTC} so the timeline writes instants under the
 * same convention; otherwise the cutoff drifts by the JVM's UTC offset -- a boundary effect at
 * day granularity, a full-offset shift at hour granularity.
 *
 * <h3>Configuration</h3>
 * All three knobs come with defaults, so a table whose partition path is purely a date in
 * {@code yyyy-MM-dd} form works out of the box.
 * <ul>
 *   <li>{@link org.apache.hudi.config.HoodieTTLConfig#EVENT_TIME_FORMAT} — date-time pattern of
 *       the time block in the partition path. Default {@code yyyy-MM-dd}. A {@code /} in the
 *       pattern means the time block spans that many consecutive segments.</li>
 *   <li>{@link org.apache.hudi.config.HoodieTTLConfig#EVENT_TIME_SEGMENT_START_INDEX} —
 *       0-based index of the first segment that carries the time block. Default {@code 0}.
 *       Raise it when non-time segments come before the time block (see examples above).</li>
 *   <li>{@link org.apache.hudi.config.HoodieTTLConfig#EVENT_TIME_DELETE_HIVE_DEFAULT_PARTITION} —
 *       whether to treat partitions whose time block contains {@code __HIVE_DEFAULT_PARTITION__}
 *       in <i>any</i> segment as expired. Such a partition has an undefined event time (one of
 *       its event-time columns was {@code NULL}, so the row cannot be placed on the configured
 *       time axis) and falls outside the normal cutoff comparison. Default {@code false}, i.e.
 *       such partitions are skipped with a WARN and the user keeps explicit control over them.
 *       Applies to both single-segment formats (e.g. {@code dt=__HIVE_DEFAULT_PARTITION__}) and
 *       multi-segment ones (e.g. {@code dt=2026-06-28/hh=__HIVE_DEFAULT_PARTITION__}).</li>
 * </ul>
 */
@Slf4j
public class KeepByEventTimeStrategy extends KeepByTimeStrategy {

  private final String eventTimeFormat;
  private final int timeSegStartIndex;
  private final boolean deleteHiveDefaultPartition;
  private final boolean hiveStylePartitioning;

  public KeepByEventTimeStrategy(HoodieTable hoodieTable, String instantTime) {
    super(hoodieTable, instantTime);
    // Defaults: format='yyyy-MM-dd', timeSegStartIndex=0, deleteHiveDefaultPartition=false. The
    // two guards below catch users who set the values explicitly to obviously-broken inputs.
    this.eventTimeFormat = writeConfig.getPartitionTTLEventTimeFormat();
    if (eventTimeFormat == null || eventTimeFormat.isEmpty()) {
      throw new IllegalArgumentException(
          "hoodie.partition.ttl.strategy.event.time.format must not be empty.");
    }
    this.timeSegStartIndex = writeConfig.getPartitionTTLEventTimeSegmentStartIndex();
    if (timeSegStartIndex < 0) {
      throw new IllegalArgumentException(
          "hoodie.partition.ttl.strategy.event.time.segment.start.index must be >= 0, got " + timeSegStartIndex);
    }
    this.deleteHiveDefaultPartition = writeConfig.shouldDeleteHiveDefaultPartitionForEventTimeTTL();
    // Hive-style partitioning is a table-level property recorded at table creation; trust it
    // rather than guessing per-segment from a stray '=' character in a value.
    this.hiveStylePartitioning = Boolean.parseBoolean(
        hoodieTable.getMetaClient().getTableConfig().getHiveStylePartitioningEnable());
  }

  @Override
  protected List<String> getExpiredPartitionsForTimeStrategy(List<String> partitionPathsForTTL) {
    long cutoffMillis = resolveCutoffMillis(instantTime, ttlInMilis);
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(eventTimeFormat).withZone(ZoneOffset.UTC);
    int segCount = segmentCount(eventTimeFormat);
    return partitionPathsForTTL.stream().parallel()
        .filter(path -> isPartitionExpiredByEventTime(
            path, formatter, timeSegStartIndex, segCount, cutoffMillis, deleteHiveDefaultPartition, hiveStylePartitioning))
        .collect(Collectors.toList());
  }

  /**
   * Number of '/'-separated path segments the configured format occupies.
   * Example: {@code yyyy-MM-dd/HH} -> 2.
   */
  static int segmentCount(String format) {
    return format.split("/").length;
  }

  /**
   * Resolve the "now" reference timestamp from {@code instantTime}, in UTC.
   * <p>
   * Anchoring on {@code instantTime} keeps the strategy idempotent across retries of the same
   * replace commit. We parse it in UTC so the cutoff and the partition's event time (also parsed
   * in UTC above) sit on the same axis -- otherwise expiry drifts by the JVM's UTC offset, which
   * is negligible at day granularity but a full-offset shift at hour granularity.
   */
  static long resolveCutoffMillis(String instantTime, long ttlInMillis) {
    try {
      return HoodieInstantTimeGenerator.parseDateFromInstantTime(instantTime, ZoneOffset.UTC).getTime() - ttlInMillis;
    } catch (ParseException e) {
      throw new IllegalStateException("Failed to parse instant time " + instantTime, e);
    }
  }

  /**
   * Decide whether a partition path is expired.
   * <p>
   * The strategy treats any partition that cannot be parsed under the configured format /
   * start-index / hive-style as a hard error. Reasoning: this class derives lifetime from the
   * path itself, so a partition we cannot parse has no defined lifetime, and silently skipping
   * it would leave it in the table forever while the rest of TTL appears to succeed. Switch to
   * {@code KEEP_BY_TIME} or {@code KEEP_BY_CREATION_TIME} (which key off commit metadata, not
   * the path) if the table contains partitions that don't conform to a single event-time shape.
   * <p>
   * Package-private so unit tests can exercise it directly without spinning up a HoodieTable.
   */
  static boolean isPartitionExpiredByEventTime(String partitionPath,
                                               DateTimeFormatter formatter,
                                               int timeSegStartIndex,
                                               int segCount,
                                               long cutoffMillis,
                                               boolean deleteHiveDefaultPartition,
                                               boolean hiveStylePartitioning) {
    String[] segments = partitionPath.split("/");
    if (segments.length < timeSegStartIndex + segCount) {
      throw new IllegalArgumentException(String.format(
          "Partition '%s' has %d segment(s) but the configured event time spans %d segment(s) starting at index %d. "
              + "Check hoodie.partition.ttl.strategy.event.time.format and event.time.segment.start.index, "
              + "or switch to KEEP_BY_TIME / KEEP_BY_CREATION_TIME if not all partitions of this table follow an event-time shape.",
          partitionPath, segments.length, segCount, timeSegStartIndex));
    }

    String[] timeSegs = new String[segCount];
    boolean hasDefaultSegment = false;
    for (int i = 0; i < segCount; i++) {
      String seg = segments[timeSegStartIndex + i];
      if (hiveStylePartitioning) {
        int eq = seg.indexOf('=');
        if (eq < 0) {
          throw new IllegalArgumentException(String.format(
              "Partition '%s' segment '%s' has no hive-style 'field=value' prefix but "
                  + "hoodie.datasource.write.hive_style_partitioning=true on the table. "
                  + "Switch to KEEP_BY_TIME / KEEP_BY_CREATION_TIME if such legacy partitions must coexist.",
              partitionPath, seg));
        }
        timeSegs[i] = seg.substring(eq + 1);
      } else {
        timeSegs[i] = seg;
      }
      if (PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH.equals(timeSegs[i])) {
        hasDefaultSegment = true;
      }
    }

    // Any default-marker segment means the event time itself is undefined: a date-recorded /
    // hour-NULL row cannot be placed on the hour axis any more than a date-NULL row can. We
    // therefore treat the whole time block as "the default partition" and let the explicit
    // user switch decide. Without this branch a multi-segment shape like
    // dt=2026-06-28/hh=__HIVE_DEFAULT_PARTITION__ would either silently expire (old code) or
    // stall TTL forever on a hard error.
    if (hasDefaultSegment) {
      if (deleteHiveDefaultPartition) {
        log.info("Partition '{}' time block contains {} (event time undefined) and delete switch "
            + "is on; marking expired", partitionPath, PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH);
        return true;
      }
      log.warn("Skipping partition '{}': time block contains {} (event time undefined; set "
              + "hoodie.partition.ttl.strategy.event.time.delete.hive.default.partition=true to delete)",
          partitionPath, PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH);
      return false;
    }

    String timeStr = String.join("/", timeSegs);
    Long eventMillis = parseEventMillis(timeStr, formatter);
    if (eventMillis == null) {
      throw new IllegalArgumentException(String.format(
          "Partition '%s': cannot parse '%s' with pattern '%s'. "
              + "Fix hoodie.partition.ttl.strategy.event.time.format, or switch to "
              + "KEEP_BY_TIME / KEEP_BY_CREATION_TIME if such partitions must remain in the table.",
          partitionPath, timeStr, formatter));
    }
    return eventMillis < cutoffMillis;
  }

  /**
   * Parse {@code timeStr} into epoch millis. Tries full date-time first; falls back to a
   * date-only parse anchored at UTC start-of-day so day-level patterns (e.g. {@code yyyy-MM-dd})
   * also work.
   */
  static Long parseEventMillis(String timeStr, DateTimeFormatter formatter) {
    TemporalAccessor parsed;
    try {
      parsed = formatter.parse(timeStr);
    } catch (DateTimeParseException e) {
      return null;
    }
    try {
      return Instant.from(parsed).toEpochMilli();
    } catch (DateTimeException ignore) {
      // Day-level pattern with no time-of-day: anchor at UTC start of day.
      try {
        return LocalDate.from(parsed).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      } catch (DateTimeException e) {
        return null;
      }
    }
  }
}
