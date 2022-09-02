package org.apache.hudi.source.flip27.split;

import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import java.util.Map;

/**
 * hoodie source reader.
 */
public class HoodieSourceReader extends SingleThreadMultiplexSourceReaderBase<
    RowData, RowData, MergeOnReadInputSplit, MergeOnReadInputSplitState> {

  public HoodieSourceReader(FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue,
                            SingleThreadFetcherManager<RowData, MergeOnReadInputSplit> splitFetcherManager,
                            RecordEmitter<RowData, RowData, MergeOnReadInputSplitState> recordEmitter,
                            Configuration config, SourceReaderContext context) {
    super(elementsQueue, splitFetcherManager, recordEmitter, config, context);
  }

  @Override
  protected void onSplitFinished(Map<String, MergeOnReadInputSplitState> map) {
    context.sendSplitRequest();
  }

  @Override
  protected MergeOnReadInputSplitState initializedState(MergeOnReadInputSplit split) {
    return new MergeOnReadInputSplitState(split);
  }

  @Override
  protected MergeOnReadInputSplit toSplitType(String slitId, MergeOnReadInputSplitState mergeOnReadInputSplitState) {
    return mergeOnReadInputSplitState.toFileSourceSplit();
  }

  @Override
  public void start() {
    // we request a split only if we did not get splits during the checkpoint restore
    if (getNumberOfCurrentlyAssignedSplits() == 0) {
      context.sendSplitRequest();
    }
  }
}
