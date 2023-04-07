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
import org.apache.hudi.adapter.AbstractStreamOperatorAdapter;
import org.apache.hudi.adapter.AbstractStreamOperatorFactoryAdapter;
import org.apache.hudi.adapter.MailboxExecutorAdapter;
import org.apache.hudi.adapter.Utils;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

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

  // It's the same thread that runs this operator and checkpoint actions. Use this executor to schedule only
  // splits for subsequent reading, so that a new checkpoint could be triggered without blocking a long time
  // for exhausting all scheduled split reading tasks.
  private final MailboxExecutorAdapter executor;

  private MergeOnReadInputFormat format;

  private transient SourceFunction.SourceContext<RowData> sourceContext;

  private transient ListState<MergeOnReadInputSplit> inputSplitsState;

  private transient Queue<MergeOnReadInputSplit> splits;

  private transient volatile SplitProcessThread splitProcessThread;

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

    splitProcessThread = new SplitProcessThread(splits,
        executor,
        format,
        sourceContext,
        this::splitProcessException,
        getRuntimeContext().getIndexOfThisSubtask(),
        getRuntimeContext().getNumberOfParallelSubtasks());
    splitProcessThread.start();
  }

  public void splitProcessException() throws Exception {
    throw new Exception("Split process thread occur exception.");
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
    if (splitProcessThread != null) {
      splitProcessThread.shutdown();
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
    if (splitProcessThread != null) {
      splitProcessThread.shutdown();
    }
  }

  public static OneInputStreamOperatorFactory<MergeOnReadInputSplit, RowData> factory(MergeOnReadInputFormat format) {
    return new OperatorFactory(format);
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
