package org.apache.hudi.source.flip27;

import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 */
public class HoodieSourceEnumerator implements SplitEnumerator<MergeOnReadInputSplit, HoodieSourceEnumState> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieSourceEnumerator.class);
  private final Path path;

  private final long interval;
  private final Configuration conf;
  private final IncrementalInputSplits incrementalInputSplits;
  private final SplitEnumeratorContext<MergeOnReadInputSplit> context;
  private final List<MergeOnReadInputSplit> unassigned;
  private final Boundedness boundedness;

  private org.apache.hadoop.conf.Configuration hadoopConf;
  private String issuedInstant;
  private HoodieTableMetaClient metaClient;
  private int maxParallism;

  private RowType rowType;

  private boolean noMoreNewPartitionSplits = false;

  public HoodieSourceEnumerator(Path path,
                                Configuration conf,
                                SplitEnumeratorContext<MergeOnReadInputSplit> context,
                                List<String> retrievedInstants,
                                Boundedness boundedness,
                                List<MergeOnReadInputSplit> unassigned,
                                Set<String> requiredPartitionPaths,
                                int maxParallism,
                                RowType rowType) {
    this.context = context;
    this.boundedness = boundedness;
    this.conf = conf;
    this.path = path;
    this.interval = conf.getInteger(FlinkOptions.READ_STREAMING_CHECK_INTERVAL);
    this.maxParallism = maxParallism;
    this.rowType = rowType;
    this.incrementalInputSplits = IncrementalInputSplits.builder()
      .conf(conf)
      .path(path)
      .rowType(this.rowType)
      .maxCompactionMemoryInBytes(StreamerUtil.getMaxCompactionMemoryInBytes(conf))
      .requiredPartitions(requiredPartitionPaths)
      .skipCompaction(conf.getBoolean(FlinkOptions.READ_STREAMING_SKIP_COMPACT))
      .build();
    this.hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    this.unassigned = new ArrayList<>(unassigned);
    restoreIssuedInstant(retrievedInstants);
  }

  public HoodieSourceEnumerator(Path path,
                                Configuration conf,
                                SplitEnumeratorContext<MergeOnReadInputSplit> context,
                                Boundedness boundedness,
                                Set<String> requiredPartitionPaths,
                                int maxParallism,
                                RowType rowType) {
    this(path, conf,context, new ArrayList<>(), boundedness, new ArrayList<>(), requiredPartitionPaths, maxParallism, rowType);
  }

  /**
   *
   */
  @Override
  public void start() {
    metaClient = getOrCreateMetaClient();
    if (interval > 0) {
      context.callAsync(this::discoverSplits,
           this::handleIncrementSplits, 0, 10000L);
    } else {
      context.callAsync(this::discoverSplits, this::handleIncrementSplits);
    }
  }

  public void restoreIssuedInstant(List<String> retrievedInstants) {

    ValidationUtils.checkArgument(retrievedInstants.size() <= 1,
        getClass().getSimpleName() + " retrieved invalid state.");

    if (retrievedInstants.size() == 1 && issuedInstant != null) {
      // this is the case where we have both legacy and new state.
      // the two should be mutually exclusive for the operator, thus we throw the exception.

      throw new IllegalArgumentException(
        "The " + getClass().getSimpleName() + " has already restored from a previous Flink version.");

    } else if (retrievedInstants.size() == 1) {
      this.issuedInstant = retrievedInstants.get(0);
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} retrieved a issued instant of time {} for table {} with path {}.",
            getClass().getSimpleName(), issuedInstant, conf.get(FlinkOptions.TABLE_NAME), path);
      }
    }
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (!unassigned.isEmpty()) {
      splitsAssignment(Collections.singleton(subtaskId));
    }
  }

  @Override
  public void addSplitsBack(List<MergeOnReadInputSplit> splits, int subtaskId) {
    addSplitsToUnassigned(splits);
    if (context.registeredReaders().containsKey(subtaskId)) {
      splitsAssignment(Collections.singleton(subtaskId));
    }
  }

  public void addSplitsToUnassigned(List<MergeOnReadInputSplit> splits) {
    if (!splits.isEmpty()) {
      this.unassigned.addAll(splits);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    LOG.info("------ {} ", subtaskId);
    splitsAssignment(Collections.singleton(subtaskId));
  }

  @Override
  public HoodieSourceEnumState snapshotState(long checkpointId) throws Exception {
    List<String> instantsState = new ArrayList<>();
    if (issuedInstant != null) {
      instantsState.add(issuedInstant);
    }
    return new HoodieSourceEnumState(instantsState, unassigned);
  }

  @Override
  public void close() throws IOException {
    if (metaClient != null) {
      metaClient = null;
    }
  }

  public List<MergeOnReadInputSplit> discoverSplits() {
    if (boundedness == Boundedness.BOUNDED) {
      return this.unassigned;
    }
    if (metaClient == null) {
      return Collections.emptyList();
    }
    IncrementalInputSplits.Result result =
        incrementalInputSplits.inputSplits(metaClient, this.hadoopConf, this.issuedInstant);
    if (result.isEmpty()) {
      return Collections.emptyList();
    }
    // update the issues instant time
    this.issuedInstant = result.getEndInstant();
    return result.getInputSplits();
  }

  public void handleIncrementSplits(List<MergeOnReadInputSplit> newSplits, Throwable t) {
    if (t != null) {
      throw new FlinkRuntimeException(
        "Failed to list hoodie file split due to ", t);
    }
    addSplitsToUnassigned(newSplits);
    Set<Integer> readers = context.registeredReaders().keySet();
    splitsAssignment(readers);
  }

  private void splitsAssignment(Set<Integer> readers) {
    if (readers.isEmpty() || unassigned.isEmpty()) {
      return;
    }
    Set<Integer> currentReaders = context.registeredReaders().keySet();
    Map<Integer, List<MergeOnReadInputSplit>> incrementalAssignment = new HashMap<>();
    readers.forEach(reader -> {
      if (!currentReaders.contains(reader)) {
        throw new IllegalStateException(
          String.format("Reader %d is not registered to source coordinator", reader));
      }
      List<MergeOnReadInputSplit> splits = splitsToReader(reader);
      incrementalAssignment.computeIfAbsent(reader, k -> new ArrayList<>())
        .addAll(splits);
    });
    if (!incrementalAssignment.isEmpty()) {
      LOG.info("Assigning splits to readers {}", incrementalAssignment);
      context.assignSplits(new SplitsAssignment<>(incrementalAssignment));
      incrementalAssignment.forEach((k, v) -> unassigned.removeAll(v));
    }
    if (noMoreNewPartitionSplits && boundedness == Boundedness.BOUNDED) {
      LOG.info(
          "No more HoodieFileSplits to assign. Sending NoMoreSplitsEvent to reader {}", readers);
      readers.forEach(context::signalNoMoreSplits);
    }
  }

  public List<MergeOnReadInputSplit> splitsToReader(Integer reader) {
    int readerNum = context.currentParallelism();
    return this.unassigned.stream().filter(split ->
      reader == assignSplit(split, readerNum)).distinct()
      .collect(Collectors.toList());
  }

  private int assignSplit(MergeOnReadInputSplit split, int readerNum) {
    //assign subTask by split id (file group id)
    String fileGroupId = split.splitId();
    if (maxParallism == -1) {
      maxParallism = KeyGroupRangeAssignment.computeDefaultMaxParallelism(readerNum);
    }
    return KeyGroupRangeAssignment
      .assignKeyToParallelOperator(fileGroupId, maxParallism, readerNum);
  }

  private HoodieTableMetaClient getOrCreateMetaClient() {
    if (this.metaClient != null) {
      return this.metaClient;
    }

    if (StreamerUtil.tableExists(this.path.toString(), hadoopConf)) {
      this.metaClient = StreamerUtil.createMetaClient(this.path.toString(), hadoopConf);
      return this.metaClient;
    }
    // fallback
    return null;
  }
}
