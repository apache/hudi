package org.apache.hudi.source.flip27.split;

import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * hoodie file source spilt reader.
 */
public class HoodieFileSourceSplitReader implements SplitReader<RowData, MergeOnReadInputSplit> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFileSourceSplitReader.class);

  private final Configuration config;

  private final Queue<MergeOnReadInputSplit> splits = new LinkedBlockingDeque<>();

  private MergeOnReadInputFormat currentFormat;
  private MergeOnReadInputFormat format;

  private final SourceReaderContext context;

  @Nullable
  private String currentSplitId;
  private SourceReaderMetricGroup sourceReaderMetricGroup;
  private int subTaskId;

  public HoodieFileSourceSplitReader(Configuration config, SourceReaderContext context, MergeOnReadInputFormat format) {
    this.context = context;
    this.format = format;
    this.config = config;
    this.subTaskId = context.getIndexOfSubtask();
    this.sourceReaderMetricGroup = context.metricGroup();
  }

  @Override
  public RecordsWithSplitIds<RowData> fetch() throws IOException {
    moveToNextSplit();
    Iterator<RowData> nextBatch = currentFormat.readBatch();
    if (nextBatch == null) {
      currentFormat = null;
    }
    return nextBatch != null ? HoodieFileRecords.forRecords(this.currentSplitId,this.sourceReaderMetricGroup, nextBatch) : finishSplit();
  }

  @Override
  public void handleSplitsChanges(SplitsChange<MergeOnReadInputSplit> splitsChange) {
    if (!(splitsChange instanceof SplitsAddition)) {
      throw new UnsupportedOperationException(
        String.format(
          "The SplitChange type of %s is not supported.",
          splitsChange.getClass()));
    }

    LOG.info("Handling split change {}", splitsChange);
    splits.addAll(splitsChange.splits());
  }

  @Override
  public void wakeUp() {

  }

  public void moveToNextSplit() throws IOException {
    if (currentFormat != null || splits.isEmpty()) {
      return;
    }
    this.currentFormat = format.copy();
    MergeOnReadInputSplit split = splits.poll();
    if (split != null) {
      this.currentSplitId = split.splitId();
      currentFormat.open(split);
    }
  }

  @Override
  public void close() throws Exception {
    if (currentFormat != null) {
      currentFormat.close();
      currentFormat = null;
    }
  }

  private HoodieFileRecords finishSplit() throws IOException {
    if (currentFormat != null) {
      currentFormat.close();
      currentFormat = null;
    }

    final HoodieFileRecords finishRecords = HoodieFileRecords.finishedSplit(currentSplitId);
    currentSplitId = null;
    return finishRecords;
  }
}
