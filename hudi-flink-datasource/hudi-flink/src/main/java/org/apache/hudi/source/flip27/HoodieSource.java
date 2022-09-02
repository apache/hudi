package org.apache.hudi.source.flip27;

import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.source.flip27.split.EnumeratorCheckpointStateSerializer;
import org.apache.hudi.source.flip27.split.HoodieFileSourceSplitReader;
import org.apache.hudi.source.flip27.split.HoodieSourceReader;
import org.apache.hudi.source.flip27.split.MergeOnReadInputSplitSerializer;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;

import java.util.Set;
import java.util.function.Supplier;

/**
 *
 */
public class HoodieSource implements Source<RowData, MergeOnReadInputSplit, HoodieSourceEnumState> {

  private final Boundedness boundedness;
  private Path path;
  private Configuration conf;
  private Set<String> requiredPartitionPaths;
  private int maxParallism;

  private RowType rowType;

  //private BulkFormat readerFormat;

  private MergeOnReadInputFormat format;

  public HoodieSource(Boundedness boundedness, Path path, Configuration conf, Set<String> requiredPartitionPaths, int maxParallism, RowType rowType, MergeOnReadInputFormat format) {
    this.boundedness = boundedness;
    this.path = path;
    this.conf = conf;
    this.requiredPartitionPaths = requiredPartitionPaths;
    this.maxParallism = maxParallism;
    this.rowType = rowType;
    this.format = format;
    this.conf.setBoolean("classloader.check-leaked-classloader", false);
    //this.readerFormat = new HoodieBulkFormat(conf, mergeOnReadInputFormat);
  }

  public HoodieSource(Boundedness boundedness, Configuration conf, MergeOnReadInputFormat format) {
    this.boundedness = boundedness;
    this.conf = conf;
    this.format = format;
  }

  @Override
  public Boundedness getBoundedness() {
    return boundedness;
  }

  @Override
  public SourceReader<RowData, MergeOnReadInputSplit> createReader(SourceReaderContext readerContext) {
    Supplier<SplitReader<RowData, MergeOnReadInputSplit>> splitReaderSupplier = () -> new HoodieFileSourceSplitReader(this.conf, readerContext, format);
    HoodieRecordEmitter hoodieRecordEmitter = new HoodieRecordEmitter();
    FutureCompletingBlockingQueue<RecordsWithSplitIds<RowData>> elementsQueue = new FutureCompletingBlockingQueue<>();
    return new HoodieSourceReader(elementsQueue, new SingleThreadFetcherManager(elementsQueue, splitReaderSupplier), hoodieRecordEmitter, conf, readerContext);
  }

  @Override
  public SplitEnumerator<MergeOnReadInputSplit, HoodieSourceEnumState> createEnumerator(SplitEnumeratorContext<MergeOnReadInputSplit> enumContext) throws Exception {
    return new HoodieSourceEnumerator(path, conf, enumContext, boundedness, requiredPartitionPaths, maxParallism, rowType);
  }

  @Override
  public SplitEnumerator<MergeOnReadInputSplit, HoodieSourceEnumState> restoreEnumerator(SplitEnumeratorContext<MergeOnReadInputSplit> enumContext, HoodieSourceEnumState checkpoint) throws Exception {
    return new HoodieSourceEnumerator(path, conf, enumContext, checkpoint.getIssuedInstants(), boundedness,checkpoint.getUnassigned(), requiredPartitionPaths, maxParallism, rowType);
  }

  @Override
  public SimpleVersionedSerializer<MergeOnReadInputSplit> getSplitSerializer() {
    return new MergeOnReadInputSplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<HoodieSourceEnumState> getEnumeratorCheckpointSerializer() {
    return new EnumeratorCheckpointStateSerializer(getSplitSerializer());
  }
}
