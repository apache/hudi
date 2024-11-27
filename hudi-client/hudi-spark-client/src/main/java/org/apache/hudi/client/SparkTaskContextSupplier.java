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

package org.apache.hudi.client;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.SparkFileFormatInternalRowReaderContext;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.io.storage.HoodieSparkFileWriterFactory;

import com.google.flatbuffers.FlexBuffers;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.Utils;

import java.io.Serializable;
import java.util.Collections;
import java.util.function.Supplier;

import scala.collection.immutable.HashMap;
import scala.collection.immutable.Seq$;
import scala.collection.mutable.ArrayBuffer;

/**
 * Spark task context supplier.
 */
public class SparkTaskContextSupplier extends TaskContextSupplier implements Serializable {

  @Override
  public Supplier<Integer> getPartitionIdSupplier() {
    return TaskContext::getPartitionId;
  }

  @Override
  public Supplier<Integer> getStageIdSupplier() {
    return () -> TaskContext.get().stageId();
  }

  @Override
  public Supplier<Long> getAttemptIdSupplier() {
    return () -> TaskContext.get().taskAttemptId();
  }

  @Override
  public Option<String> getProperty(EngineProperty prop) {
    if (prop == EngineProperty.TOTAL_MEMORY_AVAILABLE) {
      // This is hard-coded in spark code {@link
      // https://github.com/apache/spark/blob/576c43fb4226e4efa12189b41c3bc862019862c6/core/src/main/scala/org/apache/
      // spark/SparkContext.scala#L471} so have to re-define this here
      final String DEFAULT_SPARK_EXECUTOR_MEMORY_MB = "1024"; // in MB
      final String SPARK_EXECUTOR_MEMORY_PROP = "spark.executor.memory";
      if (SparkEnv.get() != null) {
        // 1 GB is the default conf used by Spark, look at SparkContext.scala
        return Option.ofNullable(String.valueOf(Utils.memoryStringToMb(SparkEnv.get().conf()
            .get(SPARK_EXECUTOR_MEMORY_PROP, DEFAULT_SPARK_EXECUTOR_MEMORY_MB)) * 1024 * 1024L));
      }
      return Option.empty();
    } else if (prop == EngineProperty.MEMORY_FRACTION_IN_USE) {
      // This is hard-coded in spark code {@link
      // https://github.com/apache/spark/blob/576c43fb4226e4efa12189b41c3bc862019862c6/core/src/main/scala/org/apache/
      // spark/memory/UnifiedMemoryManager.scala#L231} so have to re-define this here
      final String DEFAULT_SPARK_EXECUTOR_MEMORY_FRACTION = "0.6";
      final String SPARK_EXECUTOR_MEMORY_FRACTION_PROP = "spark.memory.fraction";
      if (SparkEnv.get() != null) {
        // 0.6 is the default value used by Spark,
        // look at {@link
        // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/SparkConf.scala#L507}
        return Option.ofNullable(SparkEnv.get().conf()
            .get(SPARK_EXECUTOR_MEMORY_FRACTION_PROP, DEFAULT_SPARK_EXECUTOR_MEMORY_FRACTION));
      }
      return Option.empty();
    } else if (prop == EngineProperty.TOTAL_CORES_PER_EXECUTOR) {
      final String DEFAULT_SPARK_EXECUTOR_CORES = "1";
      final String SPARK_EXECUTOR_EXECUTOR_CORES_PROP = "spark.executor.cores";
      if (SparkEnv.get() != null) {
        return Option.ofNullable(SparkEnv.get().conf()
            .get(SPARK_EXECUTOR_EXECUTOR_CORES_PROP, DEFAULT_SPARK_EXECUTOR_CORES));
      }
      return Option.empty();
    }
    throw new HoodieException("Unknown engine property :" + prop);
  }

  // This reader context is used to read records before write, like compaction, clustering.
  @Override
  public Option<HoodieReaderContext> getReaderContext(HoodieTableMetaClient metaClient) {
    // 1. Create parquet reader.
    // 2. Get partition filer.
    // 3. Get data filter.
    SparkParquetReader reader = SparkAdapterSupport$.MODULE$.sparkAdapter().createParquetFileReader(
        false, SQLConf.get(), new HashMap<>(), (Configuration) metaClient.getStorageConf().unwrap());
    return Option.of(new SparkFileFormatInternalRowReaderContext(
        reader,
        metaClient.getTableConfig().getRecordKeyFields().get()[0],
        new ArrayBuffer<>(),
        new ArrayBuffer<>()));
  }
}
