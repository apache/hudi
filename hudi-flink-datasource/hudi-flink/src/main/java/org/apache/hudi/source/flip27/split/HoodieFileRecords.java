package org.apache.hudi.source.flip27.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 *
 */
public class HoodieFileRecords implements RecordsWithSplitIds<RowData> {

  @Nullable
  private String splitId;
  private Iterator<RowData> iterator;
  private final Set<String> finishedSplits;

  private SourceReaderMetricGroup sourceReaderMetricGroup;

  public HoodieFileRecords(@Nullable String splitId,SourceReaderMetricGroup sourceReaderMetricGroup, Iterator<RowData> iterator, Set<String> finishedSplits) {
    this.splitId = splitId;
    this.iterator = iterator;
    this.sourceReaderMetricGroup = sourceReaderMetricGroup;
    this.finishedSplits = finishedSplits;
  }

  public static HoodieFileRecords empty(Set<String> finishedSplits) {
    return new HoodieFileRecords(null, null,null, finishedSplits);
  }

  @Nullable
  @Override
  public String nextSplit() {
    final String nextSplit = this.splitId;
    this.splitId = null;
    return nextSplit;
  }

  @Nullable
  @Override
  public RowData nextRecordFromSplit() {
    try {
      if (iterator != null && iterator.hasNext()) {
        return iterator.next();
      }
    } catch (Exception e) {
      throw new IllegalStateException();
    }
    return null;
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }

  @Override
  public void recycle() {
    if (iterator != null) {
      iterator = null;
    }
  }

  public static HoodieFileRecords forRecords(
      final String splitId, final SourceReaderMetricGroup sourceReaderMetricGroup, final Iterator recordsForSplit) {
    return new HoodieFileRecords(splitId, sourceReaderMetricGroup, recordsForSplit, Collections.emptySet());
  }

  public static HoodieFileRecords finishedSplit(String splitId) {
    return new HoodieFileRecords(null, null,null, Collections.singleton(splitId));
  }
}
