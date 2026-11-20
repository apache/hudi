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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.nio.file.Paths;

/**
 * One memory cell's embedding rounds, in a child JVM whose heap is the cell's reference heap: reads the job's properties from
 * a file, runs rounds of the embedding job until one commits nothing (at most {@code maxRounds}) and prints one line per round
 * ({@code ROUND <n> commit=<instant> rows=<rows> checkpoint=<checkpoint>}), the inputs each compute task embedded
 * ({@code COMPUTE_PARTITIONS <partition>=<inputs>,...}), the sum of the heap pools' peaks ({@code HEAP_POOL_PEAKS_SUM_BYTES <n>},
 * an upper bound of the peak heap), the JVM's {@code Runtime.maxMemory()} ({@code MAX_HEAP_BYTES <n>}), the heap held after its
 * garbage collections ({@link PostGcHeap}: {@code POST_GC_HEAP_PEAK_BYTES <n>} and the lines after it) and {@code RESULT ok}. A
 * failure prints its stack and exits 1; an exhausted heap ends the JVM ({@code -XX:+ExitOnOutOfMemoryError}).
 *
 * <p>Arguments: the properties file, the most rounds to run.
 */
public final class EmbeddingRoundMemoryChild {

  private EmbeddingRoundMemoryChild() {
  }

  public static void main(String[] args) throws Exception {
    TypedProperties props = new TypedProperties();
    try (InputStream in = new FileInputStream(args[0])) {
      props.load(in);
    }
    int maxRounds = Integer.parseInt(args[1]);
    PostGcHeap postGcHeap = PostGcHeap.install();
    SparkConf conf = new SparkConf()
        // memory.cell.master: the Spark master (default local[4]); the partition counts stay 4, so local[1] runs them one at a time
        .setMaster(System.getProperty("memory.cell.master", "local[4]"))
        .setAppName("embedding-memory-cell")
        .set("spark.ui.enabled", "false")
        .set("spark.default.parallelism", "4")
        .set("spark.sql.shuffle.partitions", "4")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
    SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
    BucketFolderFileSystem.install(spark.sparkContext().hadoopConfiguration(), Paths.get(props.getString(BucketFolderFileSystem.ROOT)));
    int exit = 0;
    try {
      for (int n = 1; n <= maxRounds; n++) {
        Round round = EmbeddingStreamers.round(spark, props);
        if (!round.getCommit().isPresent()) {
          break;
        }
        HoodieInstant commit = round.getCommit().get();
        String table = props.getString("hoodie.base.path");
        System.out.println("ROUND " + n + " commit=" + commit.requestedTime() + " rows=" + round.getRowsWritten() + " checkpoint="
            + EmbeddingStreamers.checkpoint(table, EmbeddingStreamers.storageConf(spark), commit));
      }
      StringBuilder counts = new StringBuilder();
      PartitionCountingEmbeddingClient.counts().forEach((partition, inputs) ->
          counts.append(counts.length() == 0 ? "" : ",").append(partition).append('=').append(inputs));
      System.out.println("COMPUTE_PARTITIONS " + counts);
      long peak = 0;
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        if (pool.getType() == MemoryType.HEAP) {
          peak += pool.getPeakUsage().getUsed();
        }
      }
      System.out.println("HEAP_POOL_PEAKS_SUM_BYTES " + peak);
      System.out.println("MAX_HEAP_BYTES " + Runtime.getRuntime().maxMemory());
      postGcHeap.print(System.out);
      System.out.println("RESULT ok");
    } catch (Throwable t) {
      t.printStackTrace(System.out);
      System.out.println("RESULT failed");
      exit = 1;
    } finally {
      spark.stop();
    }
    System.exit(exit);
  }
}
