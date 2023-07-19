package org.apache.hudi.client.utils;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

public class SparkCommitUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SparkCommitUtils.class);

  /**
   * If last completed commit is older than a certain duration.
   * New empty commit will be created by using th extrametadata from the previous completed commit.
   * By doing so, checkpoint information stored in the commit files can be transferred.
   */
  public static void checkAndCreateEmptyCommit(SparkRDDWriteClient writeClient, HoodieTableMetaClient metaClient,
                                               Option<HoodieInstant> lastInstant) throws IOException, ParseException {
    long currentCommitTimeInMillis = System.currentTimeMillis();

    if (!lastInstant.isPresent()) {
      boolean isCOWTableType = metaClient.getTableType().equals(HoodieTableType.COPY_ON_WRITE);
      // Consider only ingestion commits i.e. .commit, .deltacommit or .replacecommit excluding clustering operations.
      // For MOR table type exclude .commit action as it is not an ingestion commit.
      HoodieTimeline activeTimeline = metaClient.getActiveTimeline();
      lastInstant = metaClient.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants()
          .filter(instant -> !ClusteringUtils.isClusteringInstant(activeTimeline, instant))
          .filter(instant -> isCOWTableType || !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION))
          .lastInstant();
      ValidationUtils.checkState(lastInstant.isPresent(), "No completed commit present in the timeline.");
    } else {
      ValidationUtils.checkState(lastInstant.get().isCompleted(), "Provided last instant is not completed.");
    }

    // Check if the last commit is created before the allowed duration.
    long lastCommitTimeInMillis = HoodieInstantTimeGenerator
        .parseDateFromInstantTime(lastInstant.get().getTimestamp())
        .getTime();
    long duration = writeClient.getConfig().maxDurationToTransferExtraMetadata();
    if (duration < 0 || currentCommitTimeInMillis - lastCommitTimeInMillis <= duration) {
      return;
    }

    // Fetch the commit metadata from last commit and create a dummy commit.
    HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
        metaClient.getActiveTimeline().getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
    createEmptyCommitWithMetadata(writeClient, metaClient, commitMetadata.getExtraMetadata());
  }

  /**
   * Create commit with an empty RDD by copying the metadata info to new commit metadata
   */
  public static void createEmptyCommitWithMetadata(SparkRDDWriteClient writeClient, HoodieTableMetaClient metaClient,
                                                   Map<String, String> extraMetadata) {
    LOG.info("Creating an empty commit with extraMetadata " + extraMetadata);
    ValidationUtils.checkState(!writeClient.getConfig().shouldAutoCommit(),
        "Auto commit should not be true, since we are creating a dummy commit to transfer the offsets.");
    String instantTime = writeClient.startCommit();
    metaClient.reloadActiveTimeline();

    // Create empty commit with metadata
    HoodieSparkEngineContext engineContext = (HoodieSparkEngineContext) writeClient.getEngineContext();
    JavaRDD<WriteStatus> emptyHoodieData1 = engineContext.getJavaSparkContext().emptyRDD();
    JavaRDD<WriteStatus> writeStatusRDD = writeClient.bulkInsert(emptyHoodieData1, instantTime);

    writeClient.commit(instantTime, writeStatusRDD, Option.of(extraMetadata));
  }
}
