package org.apache.hudi.table;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.filesystem.PartitionReader;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;

/**
 * hudi look up
 */
public class HoodieLookupPartitionReader<M, R> implements PartitionReader {

  private final InputFormat inputFormat;
  private final Configuration confuration;

  public HoodieLookupPartitionReader(InputFormat inputFormat, Configuration confuration) {
    this.inputFormat = inputFormat;
    this.confuration = confuration;

  }

  @Override
  public void open(List partitions) throws IOException {
    inputFormat.configure(confuration);
    InputSplit[] inputSplits = inputFormat.createInputSplits(1);
    ((RichInputFormat) inputFormat).openInputFormat();
    inputFormat.open(inputSplits[0]);
  }

  @Nullable
  @Override
  public Object read(Object reuse) throws IOException {
    if (!inputFormat.reachedEnd()) {
      return inputFormat.nextRecord(reuse);
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    inputFormat.close();
    if (inputFormat instanceof RichInputFormat) {
      ((RichInputFormat) inputFormat).closeInputFormat();
    }
  }
}
