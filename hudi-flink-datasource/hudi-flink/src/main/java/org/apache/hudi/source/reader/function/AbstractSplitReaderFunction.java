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

package org.apache.hudi.source.reader.function;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.FlinkWriteClients;

import java.util.List;

/**
 * Abstract implementation of SplitReaderFunction with shared limit push-down and
 * Hadoop configuration support.
 *
 * <p>Subclasses call {@link #limitIterator(ClosableIterator, long)} to cap the number of
 * records returned per split. Use {@link #NO_LIMIT} as the sentinel when no limit is set.
 */
public abstract class AbstractSplitReaderFunction implements SplitReaderFunction<RowData> {

  /** Sentinel value indicating that no row limit has been pushed down. */
  public static final long NO_LIMIT = -1L;

  protected final long limit;
  protected final Configuration conf;
  protected final InternalSchemaManager internalSchemaManager;
  protected final List<ExpressionPredicates.Predicate> predicates;
  protected final boolean emitDelete;
  private transient HoodieWriteConfig writeConfig;
  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  public AbstractSplitReaderFunction(
      Configuration conf,
      List<ExpressionPredicates.Predicate> predicates,
      InternalSchemaManager internalSchemaManager,
      long limit,
      boolean emitDelete) {
    this.conf = conf;
    this.predicates = predicates;
    this.internalSchemaManager = internalSchemaManager;
    this.limit = limit;
    this.emitDelete = emitDelete;
  }

  /**
   * Wraps a {@link ClosableIterator} to stop after {@code limit} records.
   */
  protected ClosableIterator<RowData> limitIterator(ClosableIterator<RowData> iterator, long limit) {
    return new ClosableIterator<RowData>() {
      private long currentReadCount = 0;

      @Override
      public boolean hasNext() {
        return currentReadCount < limit && iterator.hasNext();
      }

      @Override
      public RowData next() {
        currentReadCount++;
        return iterator.next();
      }

      @Override
      public void close() {
        iterator.close();
      }
    };
  }

  protected HoodieWriteConfig getWriteConfig() {
    if (writeConfig == null) {
      writeConfig = FlinkWriteClients.getHoodieClientConfig(conf);
    }
    return writeConfig;
  }

  protected org.apache.hadoop.conf.Configuration getHadoopConf() {
    if (hadoopConf == null) {
      hadoopConf = HadoopConfigurations.getHadoopConf(conf);
    }
    return hadoopConf;
  }
}
