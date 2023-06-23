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

package org.apache.hudi.adapter;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;

import java.util.Collections;

/**
 * Adapter utils.
 */
public class Utils {
  public static <O> SourceFunction.SourceContext<O> getSourceContext(
      TimeCharacteristic timeCharacteristic,
      ProcessingTimeService processingTimeService,
      StreamTask<?, ?> streamTask,
      Output<StreamRecord<O>> output,
      long watermarkInterval) {
    return StreamSourceContexts.getSourceContext(
        timeCharacteristic,
        processingTimeService,
        new Object(), // no actual locking needed
        output,
        watermarkInterval,
        -1,
        true);
  }

  public static FactoryUtil.DefaultDynamicTableContext getTableContext(
      ObjectIdentifier tablePath,
      ResolvedCatalogTable catalogTable,
      ReadableConfig conf) {
    return new FactoryUtil.DefaultDynamicTableContext(tablePath, catalogTable,
        Collections.emptyMap(), conf, Thread.currentThread().getContextClassLoader(), false);
  }

  public static BinaryExternalSorter getBinaryExternalSorter(
      final Object owner,
      MemoryManager memoryManager,
      long reservedMemorySize,
      IOManager ioManager,
      AbstractRowDataSerializer<RowData> inputSerializer,
      BinaryRowDataSerializer serializer,
      NormalizedKeyComputer normalizedKeyComputer,
      RecordComparator comparator,
      Configuration conf) {
    return new BinaryExternalSorter(owner, memoryManager, reservedMemorySize,
        ioManager, inputSerializer, serializer, normalizedKeyComputer, comparator, conf);
  }
}
