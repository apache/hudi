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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.model.debezium.OracleDebeziumAvroPayload;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.transform.debezium.OracleDebeziumTransformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * Subprocess entry point for {@link TestOracleCdcKafkaReplayCrashHarness}: a real HoodieStreamer
 * ingesting Oracle-Debezium JSON from Kafka into a v9 FILL_UNCHANGED MOR table. Launched via
 * {@code java -cp} so the parent test can SIGKILL it mid-commit.
 *
 * <p>args: (0) tablePath (1) bootstrap (2) topic (3) groupId (4) mode ["once" | "continuous"].
 */
public class OracleCdcKafkaStreamer {

  // Avro schema of the incoming Oracle-Debezium JSON envelope (before/after are distinct records).
  private static final String ENVELOPE_AVSC =
      "{\"type\":\"record\",\"name\":\"Envelope\",\"namespace\":\"oracle.cdc\",\"fields\":["
      + "{\"name\":\"op\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"before\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BeforeRow\",\"fields\":["
      + "{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"notes\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},"
      + "{\"name\":\"after\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"AfterRow\",\"fields\":["
      + "{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"notes\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null},"
      + "{\"name\":\"source\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Source\",\"fields\":["
      + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"ts_ms\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"txId\",\"type\":[\"null\",\"long\"],\"default\":null},"
      + "{\"name\":\"scn\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"commit_scn\",\"type\":[\"null\",\"string\"],\"default\":null}]}],\"default\":null}]}";

  public static void main(String[] args) throws Exception {
    String tablePath = args[0];
    String bootstrap = args[1];
    String topic = args[2];
    String group = args[3];
    boolean continuous = args.length > 4 && "continuous".equals(args[4]);

    SparkSession spark = SparkSession.builder()
        .master("local[2]")
        .appName("oracle-cdc-kafka-streamer")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    try {
      Path schemaFile = Paths.get(tablePath + "_source.avsc");
      Files.createDirectories(schemaFile.toAbsolutePath().getParent());
      Files.write(schemaFile, ENVELOPE_AVSC.getBytes(StandardCharsets.UTF_8));

      HoodieStreamer.Config cfg = new HoodieStreamer.Config();
      cfg.targetBasePath = tablePath;
      cfg.targetTableName = "oracle_kafka_crash";
      cfg.tableType = "MERGE_ON_READ";
      cfg.sourceClassName = JsonKafkaSource.class.getName();
      cfg.schemaProviderClassName = TestOracleCdcKafkaReplayCrashHarness.SourceOnlySchemaProvider.class.getName();
      cfg.transformerClassNames = Collections.singletonList(OracleDebeziumTransformer.class.getName());
      cfg.sourceOrderingFields = "_event_ordering";
      cfg.payloadClassName = OracleDebeziumAvroPayload.class.getName();
      cfg.operation = WriteOperationType.UPSERT;
      cfg.continuousMode = continuous;
      cfg.minSyncIntervalSeconds = 1;
      cfg.configs = new ArrayList<>(Arrays.asList(
          "hoodie.datasource.write.recordkey.field=id",
          "hoodie.write.table.version=9",
          // MOR streamer compaction: once-mode requires inline=true, continuous-mode requires inline=false
          // (async compaction runs instead). Keep the threshold high so compaction does not fire during the
          // short probe/crash windows and confound the crash timing.
          "hoodie.compact.inline=" + (continuous ? "false" : "true"),
          "hoodie.compact.inline.max.delta.commits=50",
          "hoodie.embed.timeline.server=false",
          "hoodie.streamer.source.kafka.topic=" + topic,
          "bootstrap.servers=" + bootstrap,
          "auto.offset.reset=earliest",
          "enable.auto.commit=false",
          "group.id=" + group,
          "hoodie.streamer.schemaprovider.source.schema.file=file:" + schemaFile.toAbsolutePath(),
          // continuous: one event per commit -> many crash boundaries; once: drain the backlog in one pass.
          "hoodie.streamer.kafka.source.maxEvents=" + (continuous ? "1" : "1000")));
      new HoodieStreamer(cfg, jsc).sync();
    } finally {
      spark.stop();
    }
  }
}
