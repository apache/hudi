/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bulk.sort;

import org.apache.hudi.adapter.Utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Operator for batch sort.
 *
 * <p>Copied from org.apache.flink.table.runtime.operators.sort.SortOperator to change the annotation.
 */
@Slf4j
public class SortOperator extends TableStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {

  private GeneratedNormalizedKeyComputer gComputer;
  private GeneratedRecordComparator gComparator;

  private final Configuration conf;

  private transient BinaryExternalSorter sorter;
  private transient StreamRecordCollector<RowData> collector;
  private transient BinaryRowDataSerializer binarySerializer;

  public SortOperator(
      GeneratedNormalizedKeyComputer gComputer, GeneratedRecordComparator gComparator,
      Configuration conf) {
    this.gComputer = gComputer;
    this.gComparator = gComparator;
    this.conf = conf;
  }

  @Override
  public void open() throws Exception {
    super.open();
    log.info("Opening SortOperator");

    ClassLoader cl = getContainingTask().getUserCodeClassLoader();

    AbstractRowDataSerializer inputSerializer =
        (AbstractRowDataSerializer)
            getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
    this.binarySerializer = new BinaryRowDataSerializer(inputSerializer.getArity());

    NormalizedKeyComputer computer = gComputer.newInstance(cl);
    RecordComparator comparator = gComparator.newInstance(cl);
    gComputer = null;
    gComparator = null;

    MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
    this.sorter =
        Utils.getBinaryExternalSorter(
            this.getContainingTask(),
            memManager,
            computeMemorySize(),
            this.getContainingTask().getEnvironment().getIOManager(),
            inputSerializer,
            binarySerializer,
            computer,
            comparator,
            conf);
    this.sorter.startThreads();

    collector = new StreamRecordCollector<>(output);

    // register the metrics.
    getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
    getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
    getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);
  }

  /**
   * The modifier of this method is updated to `protected` sink Flink 2.0, here we overwrite the method
   * with `public` modifier to make it compatible considering usage in hudi-flink module.
   */
  @Override
  public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<RowData>> output) {
    super.setup(containingTask, config, output);
  }

  /**
   * The modifier of this method is updated to `protected` sink Flink 2.0, here we overwrite the method
   * with `public` modifier to make it compatible considering usage in hudi-flink module.
   */
  @Override
  public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
    super.setProcessingTimeService(processingTimeService);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) throws Exception {
    this.sorter.write(element.getValue());
  }

  @Override
  public void endInput() throws Exception {
    BinaryRowData row = binarySerializer.createInstance();
    MutableObjectIterator<BinaryRowData> iterator = sorter.getIterator();
    while ((row = iterator.next(row)) != null) {
      collector.collect(row);
    }
  }

  @Override
  public void close() throws Exception {
    log.info("Closing SortOperator");
    super.close();
    if (sorter != null) {
      sorter.close();
    }
  }
}
