package org.apache.hudi.common.table.read;

public class ReaderParameters {
  private final boolean shouldUseRecordPosition;
  private final boolean emitDelete;
  private final boolean sortOutput;
  // Allows to consider inflight instants while merging log records using HoodieMergedLogRecordReader
  // The inflight instants need to be considered while updating RLI records. RLI needs to fetch the revived
  // and deleted keys from the log files written as part of active data commit. During the RLI update,
  // the allowInflightInstants flag would need to be set to true. This would ensure the HoodieMergedLogRecordReader
  // considers the log records which are inflight.
  private final boolean allowInflightInstants;
  private final boolean enableOptimizedLogBlockScan;

  ReaderParameters(boolean shouldUseRecordPosition, boolean emitDelete, boolean sortOutput, boolean allowInflightInstants, boolean enableOptimizedLogBlockScan) {
    this.shouldUseRecordPosition = shouldUseRecordPosition;
    this.emitDelete = emitDelete;
    this.sortOutput = sortOutput;
    this.allowInflightInstants = allowInflightInstants;
    this.enableOptimizedLogBlockScan = enableOptimizedLogBlockScan;
  }

  public boolean isShouldUseRecordPosition() {
    return shouldUseRecordPosition;
  }

  public boolean isEmitDelete() {
    return emitDelete;
  }

  public boolean isSortOutput() {
    return sortOutput;
  }

  public boolean isAllowInflightInstants() {
    return allowInflightInstants;
  }

  public boolean isEnableOptimizedLogBlockScan() {
    return enableOptimizedLogBlockScan;
  }
}
