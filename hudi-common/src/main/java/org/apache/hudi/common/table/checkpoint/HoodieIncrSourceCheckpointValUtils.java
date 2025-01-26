package org.apache.hudi.common.table.checkpoint;

/**
 * Utility class providing methods to check if a string starts with specific resume-related prefixes.
 */
public class HoodieIncrSourceCheckpointValUtils {
  private static final String RESET_CHECKPOINT_V2_SEPARATOR = ":";
  private static final String REQUEST_TIME_PREFIX = "resumeFromInstantRequestTime:";
  private static final String COMPLETION_TIME_PREFIX = "resumeFromInstantCompletionTime:";

  /**
   * For hoodie incremental source ingestion, if the target table is version 8 or higher, the checkpoint
   * key set by streamer config can be in either of the following format:
   * - resumeFromInstantRequestTime:[checkpoint value based on request time]
   * - resumeFromInstantCompletionTime:[checkpoint value based on completion time]
   *
   * StreamerCheckpointV2FromCfgCkp class itself captured the fact that this is version 8 and higher, plus
   * the checkpoint source is from streamer config override.
   *
   * When the checkpoint is consumed by individual data sources, we need to convert them to either vanilla
   * checkpoint v1 (request time based) or checkpoint v2 (completion time based).
   */
  public static Checkpoint resolveToV1V2Checkpoint(StreamerCheckpointFromCfgCkp checkpoint) {
    String[] parts = extractKeyValues(checkpoint);
    switch (parts[0]) {
      case REQUEST_TIME_PREFIX: {
        return new StreamerCheckpointV1(checkpoint).setCheckpointKey(parts[1]);
      }
      case COMPLETION_TIME_PREFIX: {
        return new StreamerCheckpointV2(checkpoint).setCheckpointKey(parts[1]);
      }
      default:
        throw new IllegalArgumentException("Unknown event ordering mode " + parts[0]);
    }
  }

  private static String [] extractKeyValues(StreamerCheckpointFromCfgCkp checkpoint) {
    String checkpointKey = checkpoint.getCheckpointKey();
    String[] parts = checkpointKey.split(RESET_CHECKPOINT_V2_SEPARATOR);
    if (parts.length != 2
        || (
          !parts[0].trim().equals(REQUEST_TIME_PREFIX)
          && !parts[0].trim().equals(COMPLETION_TIME_PREFIX)
        )) {
      throw new IllegalArgumentException(
          "Illegal checkpoint key override `" + checkpointKey + "`. Valid format is either `resumeFromInstantRequestTime:<checkpoint value>` or "
          + "`resumeFromInstantCompletionTime:<checkpoint value>`.");
    }
    return parts;
  }

  /**
   * Immutable class to hold the parts of a resume string.
   */
  public static class CheckpointWithMode {
    private final String eventOrderingMode;
    private final String checkpointKey;

    public CheckpointWithMode(String eventOrderingMode, String checkpointKey) {
      this.eventOrderingMode = eventOrderingMode;
      this.checkpointKey = checkpointKey;
    }

    public String getEventOrderingMode() {
      return eventOrderingMode;
    }

    public String getCheckpointKey() {
      return checkpointKey;
    }

    @Override
    public String toString() {
      return "ResetCheckpointKeyParsed {"
          + "eventOrderingMode='" + eventOrderingMode + "', "
          + "checkpointKey='" + checkpointKey + "'}";
    }
  }
}