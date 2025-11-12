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

package org.apache.hudi.integ.testsuite;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.writer.DeltaWriteStats;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Test suite Writer that assists in testing async table operations with Deltastreamer continuous mode.
 *
 * TODO: [HUDI-8294]
 * Sample command
 * ./bin/spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 \
 *  --conf spark.task.cpus=1 --conf spark.executor.cores=1 \
 * --conf spark.task.maxFailures=100 \
 * --conf spark.memory.fraction=0.4 \
 * --conf spark.rdd.compress=true \
 * --conf spark.kryoserializer.buffer.max=2000m \
 * --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 * --conf spark.memory.storageFraction=0.1 \
 * --conf spark.shuffle.service.enabled=true \
 * --conf spark.sql.hive.convertMetastoreParquet=false \
 * --conf spark.driver.maxResultSize=12g \
 * --conf spark.executor.heartbeatInterval=120s \
 * --conf spark.network.timeout=600s \
 * --conf spark.yarn.max.executor.failures=10 \
 * --conf spark.sql.catalogImplementation=hive \
 * --class org.apache.hudi.integ.testsuite.HoodieTestSuiteJob <PATH_TO_BUNDLE>/hudi-integ-test-bundle-0.12.0-SNAPSHOT.jar \
 * --source-ordering-field test_suite_source_ordering_field \
 * --use-deltastreamer \
 * --target-base-path /tmp/hudi/output \
 * --input-base-path /tmp/hudi/input \
 * --target-table table1 \
 * -props file:/tmp/test.properties \
 * --schemaprovider-class org.apache.hudi.integ.testsuite.schema.TestSuiteFileBasedSchemaProvider \
 * --source-class org.apache.hudi.utilities.sources.AvroDFSSource \
 * --input-file-size 125829120 \
 * --workload-yaml-path file:/tmp/simple-deltastreamer.yaml \
 * --workload-generator-classname org.apache.hudi.integ.testsuite.dag.WorkflowDagGenerator \
 * --table-type COPY_ON_WRITE \
 * --compact-scheduling-minshare 1 \
 * --clean-input \
 * --clean-output \
 * --continuous \
 * --test-continuous-mode \
 * --min-sync-interval-seconds 20
 */
public class HoodieContinuousTestSuiteWriter extends HoodieTestSuiteWriter {

  private static Logger log = LoggerFactory.getLogger(HoodieContinuousTestSuiteWriter.class);

  public HoodieContinuousTestSuiteWriter(JavaSparkContext jsc, Properties props, HoodieTestSuiteJob.HoodieTestSuiteConfig cfg, String schema) throws Exception {
    super(jsc, props, cfg, schema);
  }

  @Override
  public void shutdownResources() {
    if (this.deltaStreamerWrapper != null) {
      log.info("Shutting down DS wrapper gracefully ");
      this.deltaStreamerWrapper.shutdownGracefully();
    }
    if (this.writeClient != null) {
      log.info("Closing local write client");
      this.writeClient.close();
    }
  }

  @Override
  public RDD<GenericRecord> getNextBatch() throws Exception {
    return null;
  }

  @Override
  public Pair<SchemaProvider, Pair<Checkpoint, JavaRDD<HoodieRecord>>> fetchSource() throws Exception {
    return null;
  }

  @Override
  public Option<String> startCommit() {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> upsert(Option<String> instantTime) throws Exception {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> insert(Option<String> instantTime) throws Exception {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> insertOverwrite(Option<String> instantTime) throws Exception {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> insertOverwriteTable(Option<String> instantTime) throws Exception {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(Option<String> instantTime) throws Exception {
    return null;
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> compact(Option<String> instantTime) throws Exception {
    return null;
  }

  @Override
  public void inlineClustering() {
  }

  @Override
  public Option<String> scheduleCompaction(Option<Map<String, String>> previousCommitExtraMetadata) throws
      Exception {
    return Option.empty();
  }

  @Override
  public void commit(JavaRDD<WriteStatus> records, JavaRDD<DeltaWriteStats> generatedDataStats,
                     Option<String> instantTime) {
  }

  @Override
  public void commitCompaction(Option<String> instantTime, HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetadata) throws IOException {
  }
}
