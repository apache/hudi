package org.apache.hudi.source.flip27;

import org.apache.hudi.source.flip27.split.MergeOnReadInputSplitState;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.table.data.RowData;

/**
 *
 */
public class HoodieRecordEmitter implements RecordEmitter<RowData, RowData, MergeOnReadInputSplitState> {

  public HoodieRecordEmitter() {
  }

  @Override
  public void emitRecord(RowData rowData, SourceOutput<RowData> sourceOutput, MergeOnReadInputSplitState mergeOnReadInputSplitState) throws Exception {
    sourceOutput.collect(rowData);
  }
}
