/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source;

import org.apache.hudi.adapter.AbstractStreamOperatorAdapter;
import org.apache.hudi.adapter.AbstractStreamOperatorFactoryAdapter;
import org.apache.hudi.adapter.MailboxExecutorAdapter;
import org.apache.hudi.adapter.Utils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * The operator that reads the {@link MergeOnReadInputSplit splits} received from the preceding {@link
 * StreamReadMonitoringFunction}. Contrary to the {@link StreamReadMonitoringFunction} which has a parallelism of 1,
 * this operator can have multiple parallelism.
 *
 * <p>As soon as an input split {@link MergeOnReadInputSplit} is received, it is put into a queue,
 * the {@code MailboxExecutor} read the actual data of the split.
 * This architecture allows the separation of split reading from processing the checkpoint barriers,
 * thus removing any potential back-pressure.
 */
public class StreamReadOperator extends AbstractStreamOperatorAdapter<RowData>
    implements OneInputStreamOperator<MergeOnReadInputSplit, RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(StreamReadOperator.class);

  private static final int MINI_BATCH_SIZE = 2048;

  // It's the same thread that runs this operator and checkpoint actions. Use this executor to schedule only
  // splits for subsequent reading, so that a new checkpoint could be triggered without blocking a long time
  // for exhausting all scheduled split reading tasks.
  private final MailboxExecutorAdapter executor;

  private MergeOnReadInputFormat format;

  private transient SourceFunction.SourceContext<RowData> sourceContext;

  private transient ListState<MergeOnReadInputSplit> inputSplitsState;

  private transient Queue<MergeOnReadInputSplit> splits;

  // Splits are read by the same thread that calls #processElement. Each read task is submitted to that thread by adding
  // them to the executor. This state is used to ensure that only one read task is in that splits queue at a time, so that
  // read tasks do not accumulate ahead of checkpoint tasks. When there is a read task in the queue, this is set to RUNNING.
  // When there are no more files to read, this will be set to IDLE.
  private transient volatile SplitState currentSplitState;

  private StreamReadOperator(MergeOnReadInputFormat format, ProcessingTimeService timeService,
                             MailboxExecutorAdapter mailboxExecutor) {
    this.format = Preconditions.checkNotNull(format, "The InputFormat should not be null.");
    this.processingTimeService = timeService;
    this.executor = Preconditions.checkNotNull(mailboxExecutor, "The mailboxExecutor should not be null.");
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);

    // TODO Replace Java serialization with Avro approach to keep state compatibility.
    inputSplitsState = context.getOperatorStateStore().getListState(
        new ListStateDescriptor<>("splits", new JavaSerializer<>()));

    // Initialize the current split state to IDLE.
    currentSplitState = SplitState.IDLE;

    // Recover splits state from flink state backend if possible.
    splits = new LinkedBlockingDeque<>();
    if (context.isRestored()) {
      int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
      LOG.info("Restoring state for operator {} (task ID: {}).", getClass().getSimpleName(), subtaskIdx);

      for (MergeOnReadInputSplit split : inputSplitsState.get()) {
        splits.add(split);
      }
    }

    this.sourceContext = Utils.getSourceContext(
        getOperatorConfig().getTimeCharacteristic(),
        getProcessingTimeService(),
        getContainingTask(),
        output,
        getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval());

    // Enqueue to process the recovered input splits.
    enqueueProcessSplits();
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);

    inputSplitsState.clear();
    inputSplitsState.addAll(new ArrayList<>(splits));
  }

  @Override
  public void processElement(StreamRecord<MergeOnReadInputSplit> element) {
    splits.add(element.getValue());
    enqueueProcessSplits();
  }

  private void enqueueProcessSplits() {
    if (currentSplitState == SplitState.IDLE && !splits.isEmpty()) {
      currentSplitState = SplitState.RUNNING;
      executor.execute(this::processSplits, "process input split");
    }
  }

  private void processSplits() throws IOException, InterruptedException {
    MergeOnReadInputSplit split = splits.peek();
    if (split == null) {
      currentSplitState = SplitState.IDLE;
      return;
    }

    // 1. open a fresh new input split and start reading as mini-batch
    // 2. if the input split has remaining records to read, switches to another runnable to handle
    // 3. if the input split reads to the end, close the format and remove the split from the queue #splits
    // 4. for each runnable, reads at most #MINI_BATCH_SIZE number of records
    if (format.isClosed()) {
      try {
        LOG.info("Processing input split : {}", split);
        format.setOpening();
        new Thread(() -> {
          try {
            // This log is important to indicate the consuming process,
            // there is only one log message for one data bucket.
            format.open(split);
          } catch (IOException e) {
            throw new HoodieIOException("Failed to open the split " + split, e);
          }
        }).start();
      } finally {
        currentSplitState = SplitState.IDLE;
      }
      TimeUnit.MICROSECONDS.sleep(100);
      // Re-schedule to process the next split.
      enqueueProcessSplits();
      return;
    } else if (!format.isOpened()) {
      TimeUnit.MICROSECONDS.sleep(100);
      currentSplitState = SplitState.IDLE;
      // Re-schedule to process the next split.
      enqueueProcessSplits();
      return;
    }

    try {
      consumeAsMiniBatch(split);
    } finally {
      currentSplitState = SplitState.IDLE;
    }

    // Re-schedule to process the next split.
    enqueueProcessSplits();
  }

  /**
   * Consumes at most {@link #MINI_BATCH_SIZE} number of records
   * for the given input split {@code split}.
   *
   * <p>Note: close the input format and remove the input split for the queue {@link #splits}
   * if the split reads to the end.
   *
   * @param split The input split
   */
  private void consumeAsMiniBatch(MergeOnReadInputSplit split) throws IOException {
    for (int i = 0; i < MINI_BATCH_SIZE; i++) {
      if (!format.reachedEnd()) {
        sourceContext.collect(format.nextRecord(null));
        split.consume();
      } else {
        // close the input format
        format.close();
        // remove the split
        splits.poll();
        break;
      }
    }
  }

  @Override
  public void processWatermark(Watermark mark) {
    // we do nothing because we emit our own watermarks if needed.
  }

  @Override
  public void close() throws Exception {
    super.close();

    if (format != null) {
      format.close();
      format.closeInputFormat();
      format = null;
    }

    sourceContext = null;
  }

  @Override
  public void finish() throws Exception {
    super.finish();
    output.close();
    if (sourceContext != null) {
      sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
      sourceContext.close();
      sourceContext = null;
    }
  }

  public static OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory(MergeOnReadInputFormat format) {
    return new OperatorFactory(format);
  }

  private enum SplitState {
    IDLE, RUNNING
  }

  private static class OperatorFactory extends AbstractStreamOperatorFactoryAdapter<RowData>
      implements OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> {

    private final MergeOnReadInputFormat format;

    private OperatorFactory(MergeOnReadInputFormat format) {
      this.format = format;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <O extends StreamOperator<RowData>> O createStreamOperator(StreamOperatorParameters<RowData> parameters) {
      StreamReadOperator operator = new StreamReadOperator(format, processingTimeService, getMailboxExecutorAdapter());
      operator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
      return (O) operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
      return StreamReadOperator.class;
    }
  }
}
