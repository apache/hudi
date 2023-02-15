/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.execution;

import org.apache.avro.Schema;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.ExecutorFactory;

import java.util.Iterator;
import java.util.List;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class JavaLazyInsertIterable<T> extends HoodieLazyInsertIterable<T> {
  public JavaLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr,
                                boolean areRecordsSorted,
                                HoodieWriteConfig config,
                                String instantTime,
                                HoodieTable hoodieTable,
                                String idPrefix,
                                TaskContextSupplier taskContextSupplier) {
    super(recordItr, areRecordsSorted, config, instantTime, hoodieTable, idPrefix, taskContextSupplier);
  }

  public JavaLazyInsertIterable(Iterator<HoodieRecord<T>> recordItr,
                                boolean areRecordsSorted,
                                HoodieWriteConfig config,
                                String instantTime,
                                HoodieTable hoodieTable,
                                String idPrefix,
                                TaskContextSupplier taskContextSupplier,
                                WriteHandleFactory writeHandleFactory) {
    super(recordItr, areRecordsSorted, config, instantTime, hoodieTable, idPrefix, taskContextSupplier, writeHandleFactory);
  }

  @Override
  protected List<WriteStatus> computeNext() {
    // Executor service used for launching writer thread.
    HoodieExecutor<List<WriteStatus>> bufferedIteratorExecutor =
        null;
    try {
      final Schema schema = new Schema.Parser().parse(hoodieConfig.getSchema());
      bufferedIteratorExecutor =
          ExecutorFactory.create(hoodieConfig, inputItr, getInsertHandler(), getTransformer(schema, hoodieConfig));
      final List<WriteStatus> result = bufferedIteratorExecutor.execute();
      checkState(result != null && !result.isEmpty());
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      if (null != bufferedIteratorExecutor) {
        bufferedIteratorExecutor.shutdownNow();
        bufferedIteratorExecutor.awaitTermination();
      }
    }
  }
}
