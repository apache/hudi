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

package org.apache.hudi.examples.k8s.quickstart;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import org.apache.hudi.adapter.SourceFunctionAdapter;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.examples.k8s.quickstart.utils.DataGenerator;
import org.apache.hudi.util.HoodiePipeline;
import org.apache.hudi.common.config.HoodieCommonConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This Flink program serves as a demonstration of inserting, updating, and deleting records in a Hudi table using the DataStream API.
 * The program inserts four messages for ten batches. Two of the messages generate a random UUID, acting as new insert records, while
 * two records reuse the same record keys, resulting in an update for those two records in each batch.
 * In the first batch, four new records are inserted into a newly created Hudi table.
 * Subsequently, after each batch, two new records are inserted, leading to an increment in the count by two with each batch.
 * In the 11th batch, we illustrate the delete operation by publishing a record with the delete row kind. As a result,
 * we observe the deletion of the third ID after this batch.
 *
 * After this code finishes you should see total 21 records in hudi table.
 */
public class HudiDataStreamWriter {

  public static DataType ROW_DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
      DataTypes.FIELD("uuid", DataTypes.VARCHAR(40)),// record key
      DataTypes.FIELD("rider", DataTypes.VARCHAR(20)),
      DataTypes.FIELD("driver", DataTypes.VARCHAR(20)),
      DataTypes.FIELD("fare", DataTypes.DOUBLE()),
      DataTypes.FIELD("city", DataTypes.VARCHAR(20))).notNull();

  /**
   * Main Entry point takes two parameters.
   * It can be run with Flink cli.
   * Sample Command - bin/flink run -c com.hudi.flink.quickstart.HudiDataStreamWriter ${HUDI_FLINK_QUICKSTART_REPO}/target/hudi-examples-0.1.jar hudi_table "file:///tmp/hudi_table"
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Enable checkpointing
    configureCheckpointing(env);

    DataStreamSource<RowData> dataStream = env.addSource(new SampleDataSource());

    final String targetS3Path = System.getenv("TARGET_S3_PATH");
    HoodiePipeline.Builder builder = createHudiPipeline("hudi_table", createHudiOptions(targetS3Path));
    builder.sink(dataStream, false);

    env.execute("Api_Sink");
  }

  /**
   * Configure Flink checkpointing settings.
   *
   * @param env The Flink StreamExecutionEnvironment.
   */
  private static void configureCheckpointing(StreamExecutionEnvironment env) {
    env.enableCheckpointing(5000); // Checkpoint every 5 seconds
    CheckpointConfig checkpointConfig = env.getCheckpointConfig();
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    checkpointConfig.setMinPauseBetweenCheckpoints(10000); // Minimum time between checkpoints
    checkpointConfig.setCheckpointTimeout(60000); // Checkpoint timeout in milliseconds
  }

  /**
   * Create Hudi options for the data sink.
   *
   * @param basePath The base path for Hudi data.
   * @return A Map containing Hudi options.
   */
  private static Map<String, String> createHudiOptions(String basePath) {
    Map<String, String> options = new HashMap<>();
    options.put(FlinkOptions.PATH.key(), basePath);
    options.put(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key(), "s3a");
    options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    options.put(FlinkOptions.ORDERING_FIELDS.key(), "ts");
    options.put(FlinkOptions.RECORD_KEY_FIELD.key(), "uuid");
    options.put(FlinkOptions.IGNORE_FAILED.key(), "true");
    return options;
  }

  /**
   * Create a HudiPipeline.Builder with the specified target table and options.
   *
   * @param targetTable The name of the Hudi table.
   * @param options     The Hudi options for the data sink.
   * @return A HudiPipeline.Builder.
   */
  private static HoodiePipeline.Builder createHudiPipeline(String targetTable, Map<String, String> options) {
    return HoodiePipeline.builder(targetTable)
        .column("ts TIMESTAMP(3)")
        .column("uuid VARCHAR(40)")
        .column("rider VARCHAR(20)")
        .column("driver VARCHAR(20)")
        .column("fare DOUBLE")
        .column("city VARCHAR(20)")
        .pk("uuid")
        .partition("city")
        .options(options);
  }

  /**
   * Sample data source for generating RowData objects.
   */
  static class SampleDataSource implements SourceFunctionAdapter<RowData> {
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
      int batchNum = 0;
      while (isRunning) {
        batchNum++;
        List<RowData> dataSetInsert = DataGenerator.generateRandomRowData(ROW_DATA_TYPE);
        if (batchNum < 11) {
          // For first 10 batches, inserting 4 records. 2 with random id (INSERTS) and 2 with hardcoded UUID(UPDATE)
          for (RowData row : dataSetInsert) {
            ctx.collect(row);
          }
        } else {
          // For 11th Batch, inserting only one record with row kind delete.
          RowData rowToBeDeleted = dataSetInsert.get(2);
          rowToBeDeleted.setRowKind(RowKind.DELETE);
          ctx.collect(rowToBeDeleted);
          TimeUnit.MILLISECONDS.sleep(10000);
          // Stop the stream once deleted
          isRunning = false;
        }
        TimeUnit.MILLISECONDS.sleep(10000); // Simulate a delay
      }
    }

    @Override
    public void cancel() {
      isRunning = false;
    }
  }
}
