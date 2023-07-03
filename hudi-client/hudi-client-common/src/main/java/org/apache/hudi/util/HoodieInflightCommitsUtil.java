package org.apache.hudi.util;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HoodieInflightCommitsUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieInflightCommitsUtil.class);

  public static List<HoodieInstant> listAllInflightCommits(HoodieTableMetaClient metaClient) {
    return metaClient.reloadActiveTimeline().filterInflightsAndRequested().getInstants();
  }

  public static List<HoodieInstant> inflightWriteCommitsOlderThan(HoodieTableMetaClient metaClient,
                                                                  long mins,
                                                                  boolean includeIngestionCommits) {
    long goBackMs = Duration.ofMinutes(mins).getSeconds() * 1000;
    String oldestAllowedTimestamp = HoodieInstantTimeGenerator.formatDate(new Date(System.currentTimeMillis() - goBackMs));

    Stream<HoodieInstant> inflightInstants =  metaClient
        .reloadActiveTimeline()
        .getWriteTimeline()
        .filterInflightsAndRequested()
        .findInstantsBefore(oldestAllowedTimestamp)
        .getInstants().stream();

    // Filter ingestion commits
    if (!includeIngestionCommits) {
      Predicate<HoodieInstant> ingestionCommitsFilter =
          (x) -> x.getAction().equals(HoodieTimeline.COMMIT_ACTION) || x.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION);
      inflightInstants = inflightInstants.filter(ingestionCommitsFilter.negate());
    }
    List<HoodieInstant> inflightInstantsList = inflightInstants.collect(Collectors.toList());
    LOG.info("Inflight commits " + inflightInstantsList);
    return inflightInstantsList;
  }
}