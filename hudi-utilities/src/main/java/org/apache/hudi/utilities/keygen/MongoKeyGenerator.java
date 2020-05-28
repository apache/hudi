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

package org.apache.hudi.utilities.keygen;

import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.utilities.mongo.SchemaUtils;

/* Key generator which takes the name of the key field to be used for recordKey,  computes
 murmur hash of the key value, modulo number-of-partitions as partition value.
 */
public class MongoKeyGenerator extends KeyGenerator {

  private static final String RECORD_KEY = SchemaUtils.ID_FIELD;
  private static final String PARTITION_KEY = "shard";
  private final boolean hiveStylePartitioning;
  private final int numberOfPartitions;

  /**
   * Supported configs.
   */
  static class Config {

    // Number of hash partitions for Mongo tables
    private static final String NUMBER_OF_PARTITION_PROP =
        "hoodie.deltastreamer.keygen.mongo.number_partitions";
  }

  public MongoKeyGenerator(TypedProperties props) {
    super(props);
    DataSourceUtils.checkRequiredProperties(config,
        Collections.singletonList(Config.NUMBER_OF_PARTITION_PROP));
    this.hiveStylePartitioning = props.getBoolean(
        DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
    this.numberOfPartitions = props.getInteger(Config.NUMBER_OF_PARTITION_PROP);
    if (this.numberOfPartitions <= 0) {
      throw new HoodieKeyException(
          "number_partitions value " + this.numberOfPartitions + " must be positive");
    }
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    String recordKey = DataSourceUtils.getNestedFieldValAsString(record, RECORD_KEY, true);
    if (recordKey == null) {
      throw new HoodieKeyException(SchemaUtils.ID_FIELD + " field value cannot be null");
    }
    int hashValue = Hash.getInstance(Hash.MURMUR_HASH).hash(recordKey.getBytes());
    String partitionPath = Integer.toString(hashValue % numberOfPartitions);
    if (hiveStylePartitioning) {
      partitionPath = PARTITION_KEY + "=" + partitionPath;
    }
    return new HoodieKey(recordKey, partitionPath);
  }
}
