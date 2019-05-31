/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * An implementation of {@link Source}, that emits test upserts.
 */
public class TestDataSource extends AvroSource {

  private static volatile Logger log = LogManager.getLogger(TestDataSource.class);

  // Static instance, helps with reuse across a test.
  private static HoodieTestDataGenerator dataGenerator;

  public static void initDataGen() {
    dataGenerator = new HoodieTestDataGenerator();
  }

  public static void resetDataGen() {
    dataGenerator = null;
  }

  public TestDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  private GenericRecord toGenericRecord(HoodieRecord hoodieRecord) {
    try {
      Optional<IndexedRecord> recordOpt = hoodieRecord.getData().getInsertValue(dataGenerator.avroSchema);
      return (GenericRecord) recordOpt.get();
    } catch (IOException e) {
      return null;
    }
  }

  @Override
  protected InputBatch<JavaRDD<GenericRecord>> fetchNewData(Optional<String> lastCheckpointStr,
      long sourceLimit) {

    int nextCommitNum = lastCheckpointStr.map(s -> Integer.parseInt(s) + 1).orElse(0);
    String commitTime = String.format("%05d", nextCommitNum);
    // No new data.
    if (sourceLimit <= 0) {
      return new InputBatch<>(Optional.empty(), commitTime);
    }

    // generate `sourceLimit` number of upserts each time.
    int numExistingKeys = dataGenerator.getExistingKeysList().size();
    int numUpdates = Math.min(numExistingKeys, (int) sourceLimit / 2);
    int numInserts = (int) sourceLimit - numUpdates;

    List<GenericRecord> records = new ArrayList<>();
    try {
      records.addAll(dataGenerator.generateUniqueUpdates(commitTime, numUpdates).stream()
          .map(this::toGenericRecord).collect(Collectors.toList()));
      records.addAll(dataGenerator.generateInserts(commitTime, numInserts).stream()
          .map(this::toGenericRecord).collect(Collectors.toList()));
    } catch (IOException e) {
      log.error("Error generating test data.", e);
    }
    
    JavaRDD<GenericRecord> avroRDD = sparkContext.<GenericRecord>parallelize(records, 4);
    return new InputBatch<>(Optional.of(avroRDD), commitTime);
  }
}
