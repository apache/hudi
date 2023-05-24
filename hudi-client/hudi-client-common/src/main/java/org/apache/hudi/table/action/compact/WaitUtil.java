package org.apache.hudi.table.action.compact;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class WaitUtil {

  private static final Logger LOG = LogManager.getLogger(WaitUtil.class);

  public static HashMap<HoodieFileGroupId, HashSet<String>> waitCompleteGroup(final HoodieTable table, List<String> missingInstants) {
    HashMap<HoodieFileGroupId, HashSet<String>> fileGroupTofilePaths = new HashMap<>();
    if (missingInstants == null) {
      return fileGroupTofilePaths;
    }
    missingInstants.forEach(instantTime -> {
      // check if the instant has been rollback
      HoodieInstant hoodieInstantRequest =
            new HoodieInstant(
                  HoodieInstant.State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime);
      HoodieInstant hoodieInstantInflight =
            new HoodieInstant(
                  HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime);
      HoodieInstant hoodieInstant =
            new HoodieInstant(
                  HoodieInstant.State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime);

      Integer i = 0;
      while (!table.getMetaClient().getCommitsTimeline().containsInstant(hoodieInstant)) {
        if (i == 12) {
          LOG.info("Compactor has already waited 120s for instant " + instantTime + " to complete, now exit");
          throw new HoodieException("Compactor has already waited 120s for instant " + instantTime + " to complete, now exit");
        }

        try {
          LOG.info("CommitsTimeline instantssss: " + table.getMetaClient().getCommitsTimeline().getInstants().collect(toList()));
          if (!table.getMetaClient().getCommitsTimeline().containsInstant(hoodieInstantRequest)
                && !table.getMetaClient().getCommitsTimeline().containsInstant(hoodieInstantInflight)) {
            LOG.info("instant: " + instantTime + " has been rollback");
            return;
          }
          LOG.info("instant: " + instantTime + " has not completed, wait 10s...");
          Thread.sleep(10000);
          table.getMetaClient().reloadActiveTimeline();
          i++;
        } catch (InterruptedException e) {
          // ignore InterruptedException here.
        }
      }
      LOG.info("instant: " + instantTime + " has completed");
      try {
        HoodieCommitMetadata commitMetadata =
              HoodieCommitMetadata.fromBytes(
                    table
                          .getMetaClient()
                          .getCommitsTimeline()
                          .getInstantDetails(hoodieInstant)
                          .get(),
                    HoodieCommitMetadata.class);
        commitMetadata
              .getFileGroupIdAndRelativePaths()
              .forEach((k, v) -> {
                HashSet<String> filePaths = fileGroupTofilePaths.computeIfAbsent(k, key -> new HashSet<>());
                filePaths.add(v);
              });
      } catch (IOException e) {
        throw new HoodieIOException(
              String.format(
                    "Failed to fetch HoodieCommitMetadata for instant (%s)", hoodieInstant),
              e);
      }
    });

    return fileGroupTofilePaths;
  }

  public static List<String> getMissingInstants(HoodieTableMetaClient metaClient) {
    return metaClient
          .getActiveTimeline()
          .filterPendingExcludingCompaction()
          .getInstants()
          .filter(HoodieInstant -> HoodieInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION))
          .map(HoodieInstant::getTimestamp)
          .collect(toList());
  }
}
