package org.apache.hudi.source.flip27.split;

import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 *
 * @param <SplitT>
 */
public class MergeOnReadInputSplitState<SplitT extends MergeOnReadInputSplit> {
  private final SplitT split;

  public MergeOnReadInputSplitState(SplitT split) {
    this.split = checkNotNull(split);
  }

  public SplitT toFileSourceSplit() {
    return this.split;
  }
}
