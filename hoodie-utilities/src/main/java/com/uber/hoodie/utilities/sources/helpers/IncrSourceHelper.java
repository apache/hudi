package com.uber.hoodie.utilities.sources.helpers;

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.collection.Pair;
import java.util.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;


/**
 * Helper for Hudi Incremental Source. Has APIs to
 *   (a) calculate begin and end instant time for incrementally pulling from Hudi source
 *   (b) Find max seen instant to be set as checkpoint for next fetch.
 */
public class IncrSourceHelper {

  /**
   * Get a timestamp which is the next value in a descending sequence
   *
   * @param timestamp Timestamp
   */
  private static String getStrictlyLowerTimestamp(String timestamp) {
    long ts = Long.parseLong(timestamp);
    Preconditions.checkArgument(ts > 0, "Timestamp must be positive");
    Long lower = ts - 1;
    return "" + lower;
  }

  /**
   * Find begin and end instants to be set for the next fetch
   *
   * @param jssc Java Spark Context
   * @param srcBasePath Base path of Hudi source table
   * @param numInstantsPerFetch Max Instants per fetch
   * @param beginInstant Last Checkpoint String
   * @param readLatestOnMissingBeginInstant when begin instant is missing, allow reading from latest committed instant
   * @return begin and end instants
   */
  public static Pair<String, String> calculateBeginAndEndInstants(
      JavaSparkContext jssc, String srcBasePath, int numInstantsPerFetch, Optional<String> beginInstant,
      boolean readLatestOnMissingBeginInstant) {
    Preconditions.checkArgument(numInstantsPerFetch > 0, "Make sure the config"
        + " hoodie.deltastreamer.source.hoodieincr.num_instants is set to a positive value");
    HoodieTableMetaClient srcMetaClient = new HoodieTableMetaClient(jssc.hadoopConfiguration(),
        srcBasePath, true);

    final HoodieTimeline activeCommitTimeline =
        srcMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

    String beginInstantTime = beginInstant.orElseGet(() -> {
      if (readLatestOnMissingBeginInstant) {
        Optional<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
        return lastInstant.map(hoodieInstant -> getStrictlyLowerTimestamp(hoodieInstant.getTimestamp())).orElse("000");
      } else {
        throw new IllegalArgumentException("Missing begin instant for incremental pull. For reading from latest "
            + "committed instant set hoodie.deltastreamer.source.hoodie.read_latest_on_midding_ckpt to true");
      }
    });

    Optional<HoodieInstant> nthInstant =
        activeCommitTimeline.findInstantsAfter(beginInstantTime, numInstantsPerFetch).getInstants().reduce((x, y) -> y);
    return Pair.of(beginInstantTime, nthInstant.map(instant -> instant.getTimestamp()).orElse(beginInstantTime));
  }

  /**
   * Validate instant time seen in the incoming row
   *
   * @param row Input Row
   * @param instantTime Hoodie Instant time of the row
   * @param sinceInstant begin instant of the batch
   * @param endInstant end instant of the batch
   */
  public static void validateInstantTime(Row row, String instantTime, String sinceInstant, String endInstant) {
    Preconditions.checkNotNull(instantTime);
    Preconditions.checkArgument(HoodieTimeline.compareTimestamps(instantTime,
        sinceInstant, HoodieTimeline.GREATER),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime
            + "but expected to be between " + sinceInstant + "(excl) - "
            + endInstant + "(incl)");
    Preconditions.checkArgument(HoodieTimeline.compareTimestamps(instantTime,
        endInstant, HoodieTimeline.LESSER_OR_EQUAL),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime
            + "but expected to be between " + sinceInstant + "(excl) - " + endInstant + "(incl)");
  }
}
